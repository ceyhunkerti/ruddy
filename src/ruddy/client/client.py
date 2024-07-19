import functools
import logging
import uuid
from typing import Generator

import pyarrow as pa
import pyarrow.flight as flight

from ruddy.client.middleware import CoreMiddlewareFactory
from ruddy.models.table import Table
from ruddy.url import URL

logger = logging.getLogger(__name__)


def request(func):
    @functools.wraps(func)
    def wrapper(self: "Client", *args, **kwargs):
        request_id = uuid.uuid4()
        self.core_middleware.middleware.update_headers(request_id=str(request_id))
        return func(self, *args, **kwargs)

    return wrapper


class Client:
    def __init__(self, url: str | URL):
        self.url = URL.init(url)
        headers = {"database": self.url.database, "schema": self.url.schema}
        self.core_middleware = CoreMiddlewareFactory(output_headers=headers)
        self.client = flight.FlightClient(
            self.url.location,
            middleware=[self.core_middleware],
        )
        logger.debug(f"Initialized client with {self.url.string()}")

    def to_table(self, name: str) -> Table:
        path = name.split(".")
        defaults = {}
        if self.url.database:
            defaults["default_database"] = self.url.database
        if self.url.schema:
            defaults["default_schema"] = self.url.schema

        return Table.from_path(path, defaults=defaults)

    def list_flights(self) -> Generator[flight.FlightInfo, None, None]:
        for flight_info in self.client.list_flights():
            yield flight_info

    def get_flight_info_for_path(self, *path: str) -> flight.FlightInfo:
        descriptor = flight.FlightDescriptor.for_path(*path)
        return self.client.get_flight_info(descriptor)

    def get_flight_info_for_command(self, command: str) -> flight.FlightInfo:
        descriptor = flight.FlightDescriptor.for_command(command)
        return self.client.get_flight_info(descriptor)

    def table_reader(self, name: str) -> flight.FlightStreamReader:
        table = self.to_table(name)
        flight_info = self.get_flight_info_for_path(
            table.database_or_default(),
            table.schema_or_default(),
            table.name,
        )
        reader: flight.FlightStreamReader = self.client.do_get(
            flight_info.endpoints[0].ticket
        )
        return reader

    def read_table(self, table: str) -> pa.Table:
        reader = self.table_reader(table)
        return reader.read_all()

    def query_reader(self, query: str) -> flight.FlightStreamReader:
        flight_info = self.get_flight_info_for_command(query)
        reader: flight.FlightStreamReader = self.client.do_get(
            flight_info.endpoints[0].ticket
        )
        return reader

    @request
    def read_query(self, query: str) -> pa.Table:
        reader = self.query_reader(query)
        return reader.read_all()

    @request
    def do_put(self, name: str, data: pa.Table):
        table = self.to_table(name)
        descriptor = flight.FlightDescriptor.for_path(
            table.database_or_default(),
            table.schema_or_default(),
            table.name,
        )
        writer, _ = self.client.do_put(descriptor, data.schema)
        writer.write_table(data)
        writer.close()

    def do_action(self):
        pass
