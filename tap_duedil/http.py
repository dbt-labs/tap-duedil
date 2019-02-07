import requests
import singer
from singer import metrics
import backoff
import json
import time

LOGGER = singer.get_logger()
BASE_URL = "https://duedil.io/v4/"

MAX_ERROR = 10

class RateLimitException(Exception):
    pass


class UnhandledAPIErrorException(Exception):
    pass


def _join(a, b):
    return a.rstrip("/") + "/" + b.lstrip("/")


class Client(object):
    def __init__(self, config):
        self.user_agent = config.get("user_agent")
        self.api_key = config.get("api_key")
        self.session = requests.Session()
        self.base_url = BASE_URL
        self._token = None

        self.unhandled_500s = 0

    @property
    def token(self):
        if self._token is None:
            raise RuntimeError("Client is not yet authenticated")
        return self._token

    def prepare_and_send(self, request):
        if self.user_agent:
            request.headers["User-Agent"] = self.user_agent

        request.headers["X-AUTH-TOKEN"] = self.api_key
        request.headers["Accept"] = 'application/json'
        request.headers["Content-Type"] = 'application/json'

        return self.session.send(request.prepare())

    def url(self, path):
        return _join(BASE_URL, path)

    def create_get_request(self, path, **kwargs):
        return requests.Request(method="GET", url=self.url(path), **kwargs)

    def create_post_request(self, path, **kwargs):
        data = kwargs.pop('data', {})
        query = data.pop('query', {})
        body = json.dumps(data.pop('body', {}))
        return requests.Request(method="POST", url=self.url(path), data=body, params=query, **kwargs)

    @backoff.on_exception(backoff.expo,
                          UnhandledAPIErrorException,
                          max_tries=8)
    @backoff.on_exception(backoff.expo,
                          RateLimitException,
                          max_tries=8)
    def request_with_handling(self, request, tap_stream_id):
        with metrics.http_request_timer(tap_stream_id) as timer:
            response = self.prepare_and_send(request)
            timer.tags[metrics.Tag.http_status_code] = response.status_code
        if response.status_code in [429]:
            LOGGER.info("Received rate limit error (code={})".format(response.status_code))
            raise RateLimitException()
        if response.status_code in [500, 503]:
            LOGGER.info("Received unexpected error (code={})".format(response.status_code))
            raise UnhandledAPIErrorException()
        if response.status_code == 404:
            return None
        if response.status_code == 400:
            LOGGER.fatal(response.json())
        if tap_stream_id == 'company_query' and response.status_code == 500:
            LOGGER.info('POSSIBLE CACHE MISS - RECEIVED 500 ERROR. Retrying!')
            raise RateLimitException()

        response.raise_for_status()
        return response.json()


    def GET(self, request_kwargs, *args, **kwargs):
        try:
            req = self.create_get_request(**request_kwargs)
            return self.request_with_handling(req, *args, **kwargs)
        except RateLimitException as e:
            LOGGER.info("Encountered rate limit exception. Returning None")
            return None
        except UnhandledAPIErrorException as e:
            LOGGER.info("Encountered unhandled API error. Returning None")
            return None
        except Exception as e:
            LOGGER.error(e)
            LOGGER.info("Unhandled exception")
            return None


    def POST(self, request_kwargs, *args, **kwargs):
        try:
            req = self.create_post_request(**request_kwargs)
            return self.request_with_handling(req, *args, **kwargs)
        except RateLimitException as e:
            return None
        except UnhandledAPIErrorException as e:
            return None
        except Exception as e:
            LOGGER.error(e)
            LOGGER.info("Unhandled exception")
            return None
