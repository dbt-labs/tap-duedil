import singer
from singer import metrics, transform
import pendulum  # TODO

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
        self.start_date = None

    def get_start_date(self, ctx, key):
        if not self.start_date:
            self.start_date = ctx.get_bookmark([self.tap_stream_id, key])
        return self.start_date

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
    def get_params(self, ctx, page):
        return {
            "query": {
                "offset": page,
                "limit": 5, # TODO
            },
            "body": {
                "criteria": {
                    "countryCodes": {
                        "values": ["GB"]
                    }
                }
            }
        }

    def _sync(self, ctx):
        schema = ctx.catalog.get_stream(self.tap_stream_id).schema.to_dict()

        page = 0
        all_companies = []
        while True:
            params = self.get_params(ctx, page)
            data = {"path": self.path, "data": params}
            resp = ctx.client.POST(data, self.tap_stream_id)

            resp_filters = resp.pop('filters')
            resp_companies = resp.pop('companies')

            companies = [transform({"filters": resp_filters, "companies": [company]}, schema)
                         for company in resp_companies]
            all_companies.extend(companies)

            if len(companies) == 0 or page >= 0:  # TODO
                break

            page += 1

        return all_companies

    def sync(self, ctx):
        self.write_records(ctx.cache['companies'])

    def fetch_into_cache(self, ctx):
        companies = self._sync(ctx)
        ctx.cache["companies"] = companies


class CompanyInfo(Stream):
    def get_path(self, company):
        company_id = company['companyId']
        country_code = company['countryCode'].lower()

        return self.path \
                .replace(':company_id', company_id) \
                .replace(':country_code', country_code)

    def get_params(self):
        return {
            "offset": 0,
            "limit": 50
        }


    def _sync(self, ctx, company):
        schema = ctx.catalog.get_stream(self.tap_stream_id).schema.to_dict()
        path = self.get_path(company)
        params = self.get_params()
        resp = ctx.client.GET({"path": path, "params": params}, self.tap_stream_id)
        raw_record = self.format_response(resp)
        record = transform(raw_record, schema)
        self.write_records([record])

        if 'pagination' in record:
            # TODO
            if record['pagination']['total'] > record['pagination']['limit']:
                import ipdb; ipdb.set_trace()
                raise RuntimeError("PAGINATED!")

    def sync(self, ctx):
        for company_obj in ctx.cache['companies']:
            LOGGER.info("Running for {} on {}".format(company_obj['companies'][0]['companyId'], self.tap_stream_id))
            company = company_obj['companies'][0]
            self._sync(ctx, company)


class CompanyOfficers(CompanyInfo):
    def get_params(self):
        return {
            "offset": 0,
            "limit": 50,
            "appointmentStatuses": "open,closed,retired"
        }


company_query = CompanyQuery("company_query", ["companyId"], "/search/companies.json")
all_streams = [
    company_query,
    CompanyInfo('company_financials', ["companyId"], "/company/:country_code/:company_id/financials.json"),
    # CompanyInfo('company_persons_of_significant_control', ["companyId"], "/company/:country_code/:company_id/persons-significant-control.json"),
    # CompanyInfo('company_charges', ["companyId"], "/company/:country_code/:company_id/charges.json"),
    # CompanyInfo('company_filings', ["companyId"], "/company/:country_code/:company_id/filings.json"),
    # CompanyInfo('company_fca_authorisations', ["companyId"], "/company/:country_code/:company_id/fca-authorisations.json"),
    # CompanyInfo('company_related_companies', ["companyId"], "/company/:country_code/:company_id/related-companies.json"),
    # CompanyInfo('company_gazette_notices', ["companyId"], "/company/:country_code/:company_id/gazette-notices.json"),
    # CompanyInfo('company_portfolio_companies', ["companyId"], "/company/:country_code/:company_id/portfolio-companies.json"),
    # CompanyInfo('company_group_subsidiaries', ["companyId"], "/company/:country_code/:company_id/group-subsidiaries.json"),
    # CompanyInfo('company_group_parents', ["companyId"], "/company/:country_code/:company_id/group-parents.json"),
    # CompanyInfo('company_shareholders', ["companyId"], "/company/:country_code/:company_id/shareholders.json"),
    # CompanyInfo('company_social_media_profiles', ["companyId"], "/company/:country_code/:company_id/social-media-profiles.json"),
    # CompanyOfficers('company_officers', ["companyId"], "/company/:country_code/:company_id/officers.json"),
    # CompanyInfo('company_related_names', ["companyId"], "/company/:country_code/:company_id/related-names.json"),
    # CompanyInfo('company_websites', ["companyId"], "/company/:country_code/:company_id/websites.json"),
    # CompanyInfo('company_telephone_numbers', ["companyId"], "/company/:country_code/:company_id/telephone-numbers.json"),
    # CompanyInfo('company_keywords', ["companyId"], "/company/:country_code/:company_id/keywords.json"),
    # CompanyInfo('company_descriptions', ["companyId"], "/company/:country_code/:company_id/descriptions.json"),
    # CompanyInfo('company_addresses', ["companyId"], "/company/:country_code/:company_id/addresses.json"),
    # CompanyInfo('company_industries', ["companyId"], "/company/:country_code/:company_id/industries.json"),
    # CompanyInfo('company_vitals', ["companyId"], "/company/:country_code/:company_id.json"),
]
all_stream_ids = [s.tap_stream_id for s in all_streams]
