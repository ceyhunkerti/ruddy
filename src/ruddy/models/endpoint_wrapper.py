import pyarrow.flight as flight
from pydantic import BaseModel

from ruddy.models.table import Table
from ruddy.models.ticket_wrapper import TicketWrapper


class EndpointWrapper(BaseModel, arbitrary_types_allowed=True):
    flight_endpoint: flight.FlightEndpoint

    @classmethod
    def from_table(cls, table: Table, locations: list) -> "EndpointWrapper":
        return cls(
            flight_endpoint=flight.FlightEndpoint(
                TicketWrapper.ticket_from_table(table),
                locations=locations,
            )
        )
