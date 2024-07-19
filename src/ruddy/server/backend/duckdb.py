import logging
from typing import Any, Generator

import duckdb
import pyarrow as pa
import pyarrow.flight as flight
import pydash as _

from ruddy.constants import DUCKDB_DEFAULT_DATABASE, DUCKDB_DEFAULT_SCHEMA
from ruddy.models.endpoint_wrapper import EndpointWrapper
from ruddy.models.table import Table
from ruddy.models.ticket_wrapper import TicketWrapper

logger = logging.getLogger(__name__)


class Duckdb:
    def __init__(self, config: dict = None):
        self.config = {
            "database": DUCKDB_DEFAULT_DATABASE,
            "schema": DUCKDB_DEFAULT_SCHEMA,
            **(config or {}),
        }
        if not config.get("location"):
            raise ValueError("'location' must be specified in config: dict")

        self.conn: duckdb.DuckDBPyConnection = None

    def connect(self) -> "Duckdb":
        self.conn = duckdb.connect(database=self.config.get("database"))
        if schema := self.config.get("schema"):
            self.conn.execute(f"SET schema = '{schema}'")

        return self

    @property
    def location(self):
        return self.config.get("location")

    @classmethod
    def arrow_type_map(cls) -> Any:
        return {
            "BOOLEAN": pa.bool_(),
            "TINYINT": pa.int8(),
            "SMALLINT": pa.int16(),
            "INTEGER": pa.int32(),
            "BIGINT": pa.int64(),
            "UTINYINT": pa.uint8(),
            "USMALLINT": pa.uint16(),
            "UINTEGER": pa.uint32(),
            "UBIGINT": pa.uint64(),
            "FLOAT": pa.float32(),
            "DOUBLE": pa.float64(),
            "DECIMAL": pa.decimal128(38, 18),
            "DATE": pa.date32(),
            "TIME": pa.time32("s"),
            "TIMESTAMP": pa.timestamp("ns"),
            "VARCHAR": pa.string(),
            "BLOB": pa.binary(),
        }

    def to_pyarrow_type(self, type_name: str) -> Any:
        return self.arrow_type_map().get(type_name.upper(), pa.string())

    def flights(
        self, options: dict, filters: dict = None
    ) -> Generator[flight.FlightInfo, None, None]:
        query = """
            select
                rank_dense() over (order by table_catalog, table_schema, table_name) table_id,
                table_catalog, table_schema, table_name, column_name, data_type
            from information_schema.columns"""
        if filters:
            condition = (
                _.chain(filters).map(lambda v, k: f"{k}='{v}'").join(" and\n").value()
            )
            query = f"""{query}
            where
                {condition}
            """
        query = (
            f"{query} order by table_catalog, table_schema, table_name,ordinal_position"
        )
        logger.debug(query)
        id = None
        descriptor, endpoint = None, None
        columns: list = []

        result = self.conn.query(query).fetchall()
        for (
            table_id,
            table_catalog,
            table_schema,
            table_name,
            column_name,
            data_type,
        ) in result:
            if id is None:
                id = table_id
            elif id != table_id:
                yield flight.FlightInfo(
                    pa.schema(columns), descriptor, [endpoint.flight_endpoint], -1, -1
                )
                descriptor, endpoint = None, None
                id = table_id

            if descriptor is None:
                descriptor = flight.FlightDescriptor.for_path(
                    table_catalog, table_schema, table_name
                )
                endpoint = EndpointWrapper.from_table(
                    Table(
                        name=table_name,
                        database=options.get("database"),
                        catalog_name=table_catalog,
                        schema_name=table_schema,
                    ),
                    [self.config.get("location")],
                )

            columns.append(
                (
                    column_name,
                    self.to_pyarrow_type(data_type),
                ),
            )

        if descriptor:
            yield flight.FlightInfo(
                pa.schema(columns), descriptor, [endpoint.flight_endpoint], -1, -1
            )
        else:
            raise RuntimeError("Could not find any dataset")

    def list_flights(
        self, options: dict = None
    ) -> Generator[flight.FlightInfo, None, None]:
        yield self.flights(options)

    def get_flight_info(self, options: dict, descriptor):
        if descriptor.descriptor_type == flight.DescriptorType.PATH:
            table = Table.from_path(
                [options.get("database"), options.get("schema")]
                + list(descriptor.path),
            )
            filters = {
                "table_catalog": table.catalog_name,
                "table_schema": table.schema_or_default(),
                "table_name": table.name,
            }
            for info in self.flights(options, filters):
                return info
            else:
                raise ValueError("Couldn't find any dataset")

        query = descriptor.command.decode("utf-8")
        logger.debug(query)
        cursor = self.conn.cursor()
        cursor.execute(query)
        columns = [(col[0], self.to_pyarrow_type(col[1])) for col in cursor.description]
        endpoint = flight.FlightEndpoint(
            TicketWrapper.ticket_from_command(descriptor.command),
            [self.location],
        )
        return flight.FlightInfo(pa.schema(columns), descriptor, [endpoint], -1, -1)

    def do_get(self, ticket: flight.Ticket, options: dict) -> flight.RecordBatchStream:
        tw = TicketWrapper.deserialize(ticket.ticket)
        if isinstance(tw.data, Table):
            query = f"SELECT * from {tw.data.qual_name}"
        else:
            query = tw.data

        logger.debug(query)
        table = self.conn.query(query).arrow()

        return flight.RecordBatchStream(table)

    def do_put(self, table: Table, data: pa.Table):
        query = f"CREATE TABLE IF NOT EXISTS {table.qual_name} AS SELECT * FROM data LIMIT 0"
        logger.debug(query)
        self.conn.execute(query)
        query = f"INSERT INTO {table.qual_name} SELECT * FROM data"
        logger.debug(query)
        self.conn.execute(query)
