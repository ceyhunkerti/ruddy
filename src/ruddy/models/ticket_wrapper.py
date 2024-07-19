import json
import logging
from typing import Any, Optional

import pyarrow.flight as flight
from pydantic import BaseModel

from ruddy.models.table import Table

logger = logging.getLogger(__name__)


class DataType:
    COMMAND: str = "command"
    TABLE: str = "table"


class TicketWrapper(BaseModel, arbitrary_types_allowed=True):
    data_type: Optional[str] = DataType.TABLE
    data: Table | str

    def model_post_init(self, __context: Any) -> None:
        return super().model_post_init(__context)

    def serialize(self) -> bytes:
        if isinstance(self.data, Table):
            data = self.data.to_dict()
        else:
            data = self.data
        return json.dumps(
            {
                "data_type": self.data_type,
                "data": data,
            }
        ).encode("utf-8")

    @classmethod
    def deserialize(cls, ticket: str | bytes) -> "TicketWrapper":
        if isinstance(ticket, bytes):
            ticket = ticket.decode("utf-8")
        payload = json.loads(ticket)
        data_type = payload["data_type"]
        if data_type == DataType.COMMAND:
            data = payload["data"]
        elif data_type == DataType.TABLE:
            data = Table.from_dict(payload["data"])
        else:
            raise ValueError("Invalid data type")

        return cls(data_type=data_type, data=data)

    @property
    def ticket(self) -> flight.Ticket:
        return flight.Ticket(self.serialize())

    @classmethod
    def from_table(cls, table: Table) -> "TicketWrapper":
        return cls(data_type=DataType.TABLE, data=table)

    @classmethod
    def ticket_from_table(cls, table: Table) -> flight.Ticket:
        return cls.from_table(table).ticket

    @classmethod
    def from_command(cls, command: str | bytes) -> "TicketWrapper":
        if isinstance(command, bytes):
            command = command.decode("utf-8")
        return cls(data_type=DataType.COMMAND, data=command)

    @classmethod
    def ticket_from_command(cls, command: str | bytes) -> flight.Ticket:
        return cls.from_command(command).ticket
