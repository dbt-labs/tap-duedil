import singer
from singer import metrics, transform
import datetime
import time

LOGGER = singer.get_logger()


class Stream(object):
    def __init__(self, tap_stream_id, pk_fields, path,
                 returns_collection=True,
                 collection_key=None,
                 custom_formatter=None):

        self.tap_stream_id = tap_stream_id
        self.pk_fields = pk_fields
        self.path = path
        self.returns_collection = returns_collection
        self.collection_key = collection_key
        self.custom_formatter = custom_formatter or (lambda x: x)

    def metrics(self, records):
        with metrics.record_counter(self.tap_stream_id) as counter:
            counter.increment(len(records))

    def write_records(self, records):
        singer.write_records(self.tap_stream_id, records)
        self.metrics(records)

    def format_response(self, response):
        if self.returns_collection:
            if self.collection_key:
                records = (response or {}).get(self.collection_key, [])
            else:
                records = response or []
        else:
            records = [] if not response else [response]
        return self.custom_formatter(records)


class CompanyQuery(Stream):
    def get_params(self, ctx, offset, query):
        return {
            "query": {
                "offset": offset,
                "limit": 50
            },

            "body": query
        }

    def _company_fetch(self, ctx, offset, query, attempts=0):
        if attempts > 2:
            LOGGER.info("Query for stream={} with path={}, query={} failed after retrying - exiting".format(
                        self.tap_stream_id,
                        self.path,
                        query))
            raise RuntimeError("Failed after retry for company query")

        params = self.get_params(ctx, offset, query)
        data = {"path": self.path, "data": params}
        resp = ctx.client.POST(data, self.tap_stream_id)

        if resp is None:
            LOGGER.info("Unable to get results for stream={}, data={}".format(self.tap_stream_id, data))
            LOGGER.info("Sleeping for 30 seconds, then trying again")
            time.sleep(30)
            return self._company_fetch(ctx, offset, query, attempts + 1)
        else:
            return resp

    def _sync(self, ctx, query):
        schema = ctx.catalog.get_stream(self.tap_stream_id).schema.to_dict()

        all_companies = []
        offset = 0
        while True:
            resp = self._company_fetch(ctx, offset, query)
            resp_companies = resp.pop('companies')

            companies = [transform(company, schema) for company in resp_companies]
            self.write_records(companies)
            all_companies.extend(companies)

            if len(companies) == 0:
                break

            offset += resp['pagination']['limit']
            total = resp['pagination']['total']
            LOGGER.info("Queried offset {} of {} -- got {} companies.".format(offset, total, len(companies)))

        return all_companies

    def sync(self, ctx, query):
        ctx.update_company_query_page_bookmark([self.tap_stream_id, 'company_offset'])

        companies = self._sync(ctx, query)
        ctx.cache["companies"] = companies


class CompanyInfo(Stream):
    def get_path(self, company):
        company_id = company['companyId']
        country_code = company['countryCode'].lower()

        return self.path \
                .replace(':company_id', company_id) \
                .replace(':country_code', country_code)

    def get_params(self, offset=0):
        return {
            "offset": int(offset),
            "limit": 50
        }


    def _sync(self, ctx, company):
        schema = ctx.catalog.get_stream(self.tap_stream_id).schema.to_dict()
        path = self.get_path(company)

        params = self.get_params()
        while True:
            resp = ctx.client.GET({"path": path, "params": params}, self.tap_stream_id)
            if not resp:
                # no data available for this company!
                break
            
            raw_record = self.format_response(resp)
            record = transform(raw_record, schema)
            self.write_records([record])

            if 'pagination' not in record:
                break

            offset = record['pagination']['offset']
            limit = record['pagination']['limit']
            total = record['pagination']['total']

            if offset > total:
                params = self.get_params(offset=offset + limit)
            else:
                break

    def sync(self, ctx, companies, chunk_index, num_chunks):
        total_companies = len(companies)
        for i, company in enumerate(companies):
            LOGGER.info("Running for {} on {} ({} of {}) [chunk {}/{}]".format(
                company['companyId'],
                self.tap_stream_id,
                i + 1,
                total_companies,
                chunk_index + 1,
                num_chunks))

            self._sync(ctx, company)


class CompanyOfficers(CompanyInfo):
    def get_params(self, offset=0):
        return {
            "offset": int(offset),
            "limit": 50,
            "appointmentStatuses": "open,closed,retired"
        }


PK = ["companyId", "countryCode"]
company_query = CompanyQuery("company_query", PK, "/search/companies.json")
all_streams = [
    company_query,
    CompanyInfo('company_vitals', PK, "/company/:country_code/:company_id.json"),
    CompanyInfo('company_industries', PK, "/company/:country_code/:company_id/industries.json"),
    CompanyInfo('company_addresses', PK, "/company/:country_code/:company_id/addresses.json"),
    CompanyInfo('company_descriptions', PK, "/company/:country_code/:company_id/descriptions.json"),
    CompanyInfo('company_keywords', PK, "/company/:country_code/:company_id/keywords.json"),
    CompanyInfo('company_telephone_numbers', PK, "/company/:country_code/:company_id/telephone-numbers.json"),
    CompanyInfo('company_websites', PK, "/company/:country_code/:company_id/websites.json"),
    CompanyInfo('company_related_names', PK, "/company/:country_code/:company_id/related-names.json"),
    CompanyOfficers('company_officers', PK, "/company/:country_code/:company_id/officers.json"),
    CompanyInfo('company_social_media_profiles', PK, "/company/:country_code/:company_id/social-media-profiles.json"),
    CompanyInfo('company_shareholders', PK, "/company/:country_code/:company_id/shareholders.json"),
    CompanyInfo('company_group_parents', PK, "/company/:country_code/:company_id/group-parents.json"),
    CompanyInfo('company_group_subsidiaries', PK, "/company/:country_code/:company_id/group-subsidiaries.json"),
    CompanyInfo('company_portfolio_companies', PK, "/company/:country_code/:company_id/portfolio-companies.json"),
    CompanyInfo('company_gazette_notices', PK, "/company/:country_code/:company_id/gazette-notices.json"),
    CompanyInfo('company_related_companies', PK, "/company/:country_code/:company_id/related-companies.json"),
    CompanyInfo('company_fca_authorisations', PK, "/company/:country_code/:company_id/fca-authorisations.json"),
    CompanyInfo('company_filings', PK, "/company/:country_code/:company_id/filings.json"),
    CompanyInfo('company_charges', PK, "/company/:country_code/:company_id/charges.json"),
    CompanyInfo('company_persons_of_significant_control', PK, "/company/:country_code/:company_id/persons-significant-control.json"),
    CompanyInfo('company_financials', PK, "/company/:country_code/:company_id/financials.json"),
]
all_stream_ids = [s.tap_stream_id for s in all_streams]
