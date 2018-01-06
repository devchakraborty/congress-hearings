// Fetches data about hearings in the US Congress from gpo.gov (GovInfo).
// The hearing data is dumped into an Elasticsearch instance.

// Modify these constants as needed.

const FIRST_YEAR = 1993;
const LAST_YEAR = 2017;
const REQUESTS_PER_SECOND = 5;

const { promisify } = require('util');
const _ = require('lodash');
const parseXmlString = promisify(require('xml2js').parseString);
const request = promisify(require('request'));
const textVersion = require('textversionjs');
const PromiseThrottle = require('promise-throttle');
const elasticsearch = require('elasticsearch');
const ProgressBar = require('progress');

const GPO_ROOT = 'https://www.gpo.gov';
const gpoThrottle = new PromiseThrottle({
    requestsPerSecond: REQUESTS_PER_SECOND,
    promiseImplementation: Promise
});

const esClient = new elasticsearch.Client({
    host: process.env.ELASTIC_HOST,
    httpAuth: `${process.env.ELASTIC_USER}:${process.env.ELASTIC_PASSWORD}`
});

/**
 * Fetches a hearing minutes page and returns a plaintext version of it. The
 * request is subject to the global request throttle.
 */ 
async function fetchHearingText(contentUrl) {
    const response = await gpoThrottle.add(() => request({ url: contentUrl }));
    return textVersion(response.body);
}

/**
 * Fetches an XML document and returns a JSON representation of it. The request
 * is subject to the global request throttle.
 */ 
async function fetchXml(url) {
    const response = await gpoThrottle.add(() => request({ url }));
    try {
        return await parseXmlString(response.body);
    } catch (err) {
        console.log(url, err);
        throw err;
    }
}

/**
 * Determines whether a hearing id already exists in Elasticsearch.
 */
async function hearingExists(elasticId) {
    return await esClient.exists({
        index: 'hearings',
        type: 'all',
        id: elasticId
    });
}

/**
 * Fetches the details for a hearing and adds it to Elasticsearch (if necessary).
 */
async function fetchHearing(detailUrl, checkedExists=false) {
    // Get the data from the MODS file
    const modsUrl = detailUrl.replace('content-detail.html', 'mods.xml');
    const data = await fetchXml(modsUrl);
    const allExtensions = _.get(data, 'mods.extension');
    if (!allExtensions) {
        console.warn(`No extensions in mods file: ${detailUrl}`);
        return;
    }
    const extensions = Object.assign({}, ...allExtensions);

    // Extract desired attributes
    var committeeThomasId = _.get(extensions, 'congCommittee[0].$.authorityId');
    if (committeeThomasId) {
        committeeThomasId = committeeThomasId.substr(0, 4);
    }
    const subcommitteeName = _.get(extensions, 'congCommittee[0].subCommittee[0].name[0]._');
    var congressNumber = _.get(extensions, 'congress[0]');
    if (congressNumber) {
        congressNumber = parseInt(congressNumber);
    }
    var congressSession = _.get(extensions, 'session[0]');
    if (congressSession) {
        congressSession = parseInt(congressSession);
    }
    var congressChamber = _.get(extensions, 'chamber[0]');
    if (congressChamber) {
        congressChamber = congressChamber.toLowerCase();
    }
    const title = _.get(extensions, 'searchTitle[0]');
    const jacketId = _.get(extensions, 'jacketId[0]');
    const heldDate = _.get(extensions, 'heldDate[0]');
    const isAppropriation = _.get(extensions, 'isAppropriation[0]') === 'true';
    const isNomination = _.get(extensions, 'isNomination[0]') === 'true';
    const isErrata = _.get(extensions, 'isErrata[0]') === 'true';

    // Abort if there is insufficient identification information for this hearing
    if (!_.every([congressChamber, congressNumber, jacketId])) {
        return;
    }

    // Abort if the hearing already exists in Elasticsearch
    const elasticId =
        (congressChamber === 'house' ? 'H' : 'S')
        + '-' + congressNumber + '-' + jacketId;
    if (!checkedExists && await hearingExists(elasticId)) {
        return;
    }
    
    // Fetch the minutes
    const pathComponents = detailUrl.split('/');
    const pageId = pathComponents[pathComponents.length - 2];
    const contentUrl = `${GPO_ROOT}/fdsys/pkg/${pageId}/html/${pageId}.htm`;
    const content = await fetchHearingText(contentUrl);

    // Build the hearing object
    const hearing = {
        committeeThomasId,
        subcommitteeName,
        congressNumber,
        congressSession,
        congressChamber,
        title,
        jacketId,
        heldDate,
        isAppropriation,
        isNomination,
        isErrata,
        content
    };

    // Add the hearing to Elasticsearch
    try {
        await esClient.create({
            index: 'hearings',
            type: 'all',
            id: elasticId,
            body: hearing
        });
    } catch (err) {
        console.log(err);
        throw err;
    }
}

/**
 * Fetches the details for a hearing and adds it to Elasticsearch (if necessary).
 * Also does a preliminary check to see if the hearing already exists in
 * Elasticsearch.
 */
async function fetchHearingWithCache(detailUrl) {
    const pathComponents = detailUrl.split('/');
    const pageId = pathComponents[pathComponents.length - 2];
    const pageIdComponents = pageId.match(/CHRG\-(\d{3})([hs])hrg(\d{2})(\d{3})/);
    if (pageIdComponents) {
        [_, number, chamber, jacket1, jacket2] = pageIdComponents;
        if (await hearingExists(`${chamber.toUpperCase()}-${number}-${jacket1}-${jacket2}`)) {
            return;
        }
        fetchHearing(detailUrl, true);
        return;
    }
    fetchHearing(detailUrl);
    return;
}

/**
 * Returns a function that fetches a hearing, adds it to Elasticsearch, and
 * updates a progress bar when it is done.
 */
function fetchHearingWithProgress(bar) {
    return async (detailUrl) => {
        await fetchHearing(detailUrl);
        bar.tick(1);
    };
}

/**
 * Fetches all the hearings in the given year and adds them to Elasticsearch
 * while displaying a progress bar.
 */
async function fetchHearingsInYear(year) {
    const data = await fetchXml(
        `${GPO_ROOT}/smap/fdsys/sitemap_${year}/${year}_CHRG_sitemap.xml`
    );
    const urls = _.compact(data.urlset.url.map(url => _.get(url, 'loc[0]')));
    const bar = new ProgressBar(
        `${year} [:bar] :rate/s :current/:total :etas`,
        {
            complete: '=',
            incomplete: ' ',
            width: 40,
            total: urls.length
        }
    );
    await Promise.all(urls.map(fetchHearingWithProgress(bar)));
}

/**
 * Fetches all the hearings in the year range and adds them to Elasticsearch.
 */
async function fetchHearings() {
    const years = _.range(FIRST_YEAR, LAST_YEAR + 1);
    for (const year of years) {
        await fetchHearingsInYear(year);
    }
}

// Entry point
fetchHearings();
