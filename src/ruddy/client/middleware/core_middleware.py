import logging
from typing import Any

import pyarrow.flight as flight
import pydash as _

logger = logging.getLogger(__name__)


class CoreMiddleware(flight.ClientMiddleware):
    def __init__(self, output_headers: dict):
        self.output_headers = output_headers
        self.input_headers = {}

    def sending_headers(self):
        # filter None values form the headers
        return _.chain(self.output_headers).omit_by(lambda x: x is None).value()

    def received_headers(self, headers) -> Any:
        self.input_headers = headers

    def update_headers(self, **headers: dict):
        self.output_headers = {**self.output_headers, **headers}
        logger.debug(f"Output headers {self.output_headers}")


class CoreMiddlewareFactory(flight.ClientMiddlewareFactory):
    def __init__(self, output_headers: dict):
        self.output_headers = output_headers
        self.middleware = CoreMiddleware(output_headers)

    def start_call(self, info):
        return self.middleware
