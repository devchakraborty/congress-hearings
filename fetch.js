const { promisify } = require('util');
const _ = require('lodash');
const parseXmlString = promisify(require('xml2js').parseString);
const request = promisify(require('request'));
const textVersion = require('textversionjs');
const PromiseThrottle = require('promise-throttle');
const elasticsearch = require('elasticsearch');

const FIRST_YEAR = 1993;
const LAST_YEAR = 2017;

const GPO_ROOT = 'https://www.gpo.gov';
const gpoThrottle = new PromiseThrottle({
    requestsPerSecond: 30,
    promiseImplementation: Promise
});

const esClient = new elasticsearch.Client({
    host: process.env.ELASTIC_HOST,
    // log: 'trace'
});

async function fetchHearingText(contentUrl) {
    // console.log('A', contentUrl);
    const response = await gpoThrottle.add(() => request({ url: contentUrl }));
    // console.log('B', response.body.length, contentUrl);
    return textVersion(response.body);
}

async function fetchXml(url) {
    const response = await gpoThrottle.add(() => request({ url }));
    try {
        return await parseXmlString(response.body);
    } catch (err) {
        console.log(url, err);
        throw err;
    }
}

async function fetchHearing(detailUrl) {
    const modsUrl = detailUrl.replace('content-detail.html', 'mods.xml');
    const data = await fetchXml(modsUrl);
    const allExtensions = _.get(data, 'mods.extension');
    if (!allExtensions) {
        console.warn(`No extensions in mods file: ${detailUrl}`);
        return;
    }
    const extensions = Object.assign({}, ...allExtensions);

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

    const elasticId =
        (congressChamber === 'house' ? 'H' : 'S')
        + '-' + congressNumber + '-' + jacketId;
    if (
        await esClient.exists({
            index: 'hearings',
            type: 'all',
            id: elasticId
        })
    ) {
        return;
    }
    
    const pathComponents = detailUrl.split('/');
    const pageId = pathComponents[pathComponents.length - 2];
    const contentUrl = `${GPO_ROOT}/fdsys/pkg/${pageId}/html/${pageId}.htm`;
    const content = await fetchHearingText(contentUrl);

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

    // console.log('pre', elasticId);
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
    // console.log('post', elasticId);
}

async function fetchHearingsInYear(year) {
    const data = await fetchXml(
        `${GPO_ROOT}/smap/fdsys/sitemap_${year}/${year}_CHRG_sitemap.xml`
    );
    const urls = _.compact(data.urlset.url.map(url => _.get(url, 'loc[0]')));
    await Promise.all(urls.map(fetchHearing));
}

async function fetchHearings() {
    const years = _.range(FIRST_YEAR, LAST_YEAR + 1);
    for (const year of years) {
        await fetchHearingsInYear(year);
    }
}

fetchHearings();