import pyarrow.flight as flight
import pydash as _


class CoreMiddleware(flight.ServerMiddleware):
    def __init__(self, input_headers: dict):
        self.raw_input_headers = input_headers

        request_id = _.chain(input_headers).get("request_id").nth(0).value()

        self.input_headers = {
            "database": _.chain(input_headers).get("database").nth(0).value(),
            "schema": _.chain(input_headers).get("schema").nth(0).value(),
            "request_id": request_id,
        }
        self.output_headers = {
            "request_id": request_id,
        }

    def sending_headers(self) -> dict:
        return _.chain(self.output_headers).omit_by(lambda x: x is None).value()

    def set_headers(self, **headers):
        self.output_headers = {**self.output_headers, **headers}


class CoreMiddleWareFactory(flight.ServerMiddlewareFactory):
    def __init__(self):
        super(CoreMiddleWareFactory, self).__init__()

    def start_call(self, info, headers):
        return CoreMiddleware(headers)
