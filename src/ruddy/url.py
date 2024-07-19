import json
from typing import Union

from yarl import URL as YURL


class URL:
    def __init__(self, raw: str):
        self.raw = raw
        self.yurl = YURL(raw)

    @classmethod
    def init(cls, url: Union[str, "URL"]) -> "URL":
        if isinstance(url, str):
            return cls(url)
        return url

    @property
    def scheme(self):
        return self.yurl.scheme

    @property
    def host(self):
        return self.yurl.host

    @property
    def port(self):
        return self.yurl.port

    @property
    def location(self) -> str:
        return "{scheme}://{host}:{port}".format(
            scheme=self.scheme, host=self.host, port=self.port or 1881
        )

    @property
    def database(self) -> str:
        return self.yurl.query.get("database")

    @property
    def schema(self) -> str:
        return self.yurl.query.get("schema")

    def to_dict(self) -> dict:
        return {
            "raw": self.raw,
            "scheme": self.scheme,
            "host": self.host,
            "port": self.port,
            "location": self.location,
            "query": self.yurl.query,
        }

    def query(self, param: str) -> str:
        return self.yurl.query.get(param)

    def __str__(self):
        return self.string()

    def string(self) -> str:
        location = self.location
        if self.yurl.query:
            return f"{location}?{self.yurl.query_string}"
        return location
