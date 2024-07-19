import asyncio
import logging

import pyarrow as pa
import pyarrow.flight as flight

from ruddy.models.table import Table
from ruddy.server.backend import Duckdb
from ruddy.server.middleware import (
    CORE_MIDDLEWARE,
    CoreMiddleware,
    CoreMiddleWareFactory,
)
from ruddy.url import URL

logger = logging.getLogger(__name__)


class Server(flight.FlightServerBase):
    def __init__(self, url: str | URL):
        self.url = URL.init(url)
        super().__init__(
            self.url.location,
            middleware={
                CORE_MIDDLEWARE: CoreMiddleWareFactory(),
            },
        )

        backend_config = {"location": self.url.location}
        if self.url.database:
            backend_config["database"] = self.url.database
        if self.url.schema:
            backend_config["schema"] = self.url.schema
        self.backend = Duckdb(config=backend_config)
        logger.debug("Initialized server.")

    def list_actions(self, context: flight.ServerCallContext):
        # todo
        return [("get-trace-id", "Get the trace context ID.")]

    def list_flights(self, context: flight.ServerCallContext, criteria: bytes):
        cm: CoreMiddleware = context.get_middleware(CORE_MIDDLEWARE)
        return self.backend.list_flights(cm.input_headers, context, criteria)

    def get_flight_info(
        self, context: flight.ServerCallContext, descriptor: flight.FlightDescriptor
    ):
        cm: CoreMiddleware = context.get_middleware(CORE_MIDDLEWARE)
        return self.backend.get_flight_info(cm.input_headers, descriptor)

    def do_get(self, context: flight.ServerCallContext, ticket: flight.Ticket):
        cm: CoreMiddleware = context.get_middleware(CORE_MIDDLEWARE)
        return self.backend.do_get(ticket, cm.input_headers)

    def do_put(
        self,
        context: flight.ServerCallContext,
        descriptor: flight.FlightDescriptor,
        reader: flight.MetadataRecordBatchReader,
        writer: flight.FlightMetadataWriter,
    ):
        table = Table.from_path(descriptor.path)
        logger.info(f"Receiving data for table: {table.qual_name}")

        batches = []
        try:
            while True:
                chunk: flight.FlightStreamChunk = reader.read_chunk()
                batches.append(chunk.data)
        except StopIteration:
            pass  # Handle end of data gracefully

        if batches:
            self.backend.do_put(
                table=Table.from_path(descriptor.path),
                data=pa.Table.from_batches(batches),
            )
        else:
            logger.info("Nothing to write!")

        try:
            writer.close()
        except Exception as e:
            logger.error(f"Error closing writer: {e}")

    async def async_operation(self):
        await asyncio.sleep(5)  # Simulate a long operation
        return pa.scalar("Operation completed")

    async def async_operation(self, action_id):
        await asyncio.sleep(5)  # Simulate a long operation
        self.results[action_id] = "Operation completed"

    def do_action(self, context, action):
        action_id = action.body.to_pybytes().decode()
        asyncio.create_task(self.async_operation(action_id))
        return iter([flight.Result(pa.scalar("Action started").to_string())])

    def serve(self) -> None:
        self.backend.connect()
        logger.info(f"Connected to backend and started on {self.url}")
        super().serve()
