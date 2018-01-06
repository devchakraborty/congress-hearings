# Congressional Hearings Fetcher

This script fetches details about hearings in the US Congress and saves them to an Elasticsearch instance.

## Usage

```
node fetch.js
```

Set the `ELASTIC_HOST`, `ELASTIC_USER`, and `ELASTIC_PASS` environment variables to connect to an external host; otherwise `localhost:9200` will be assumed.

The years to pull and the rate at which requests are made to Govinfo can be controlled by updating constants in `fetch.js`.
