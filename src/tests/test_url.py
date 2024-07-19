from ruddy.url import URL


def test_url_ok():
    raw = "grpc://localhost:1521?database=/path/to/duckdb.db&schema=my_schema"
    url = URL(raw)
    assert url.host == "localhost"
    assert url.port == 1521
    assert url.scheme == "grpc"
    assert url.location == "grpc://localhost:1521"
    assert url.database == "/path/to/duckdb.db"
    assert url.schema == "my_schema"
    assert url.query("schema") == "my_schema"
