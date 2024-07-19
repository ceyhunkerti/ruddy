import json
import os
from typing import Any, Optional

import pyarrow.flight as flight
from pydantic import BaseModel

from ruddy.constants import DUCKDB_DEFAULT_DATABASE, DUCKDB_DEFAULT_SCHEMA


class Base(BaseModel, extra="ignore"):
    name: str
    database: Optional[str] = None
    schema_name: Optional[str] = None

    @classmethod
    def from_path(cls, path: list[str | bytes], defaults: dict = None) -> "Table":
        raise NotImplementedError


class Table(Base):
    catalog_name: Optional[str] = None

    def model_post_init(self, __context: Any) -> None:
        super().model_post_init(__context)
        if self.catalog_name is None:
            database = self.database_or_default()
            if database == ":memory:":
                self.catalog_name = "memory"
            else:
                self.catalog_name = [
                    f for f in os.path.basename(database).split(".") if f != "."
                ][0]

    @classmethod
    def from_json(cls, data: str) -> "Table":
        cls.from_dict(json.loads(data))

    @classmethod
    def from_dict(cls, data: dict) -> "Table":
        return cls(
            name=data["name"],
            catalog_name=data["catalog_name"],
            database=data["database"],
            schema_name=data["schema"],
        )

    @classmethod
    def from_path(cls, path: list[str | bytes], defaults: dict = None) -> "Table":
        defaults = {
            "default_database": DUCKDB_DEFAULT_DATABASE,
            "default_schema": DUCKDB_DEFAULT_SCHEMA,
            **(defaults or {}),
        }
        database, schema, name = ([None, None] + path)[-3:]

        if not database:
            database = defaults["default_database"]
        elif isinstance(database, bytes):
            database = database.decode("utf-8")

        if not schema:
            schema = defaults["default_schema"]
        elif isinstance(schema, bytes):
            schema = schema.decode("utf-8")

        if isinstance(name, bytes):
            name = name.decode("utf-8")

        if not name:
            raise ValueError("Expected table name")

        return cls(database=database, schema_name=schema, name=name)

    @property
    def qual_name(self):
        items = [self.name]
        if self.catalog_name and self.schema_name:
            items = [self.catalog_name, self.schema_name] + items
        return ".".join(items)

    def database_or_default(self, default: str = DUCKDB_DEFAULT_DATABASE) -> str:
        return self.database or default

    def schema_or_default(self, default: str = DUCKDB_DEFAULT_SCHEMA) -> str:
        return self.schema_name or default

    def to_ticket(self) -> flight.Ticket:
        return flight.Ticket(self.to_json().encode("utf-8"))

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    def to_dict(self) -> dict[str, Any]:
        return {
            "database": self.database_or_default(),
            "catalog_name": self.catalog_name,
            "schema": self.schema_or_default(),
            "name": self.name,
        }
