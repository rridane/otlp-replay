import pytest

from otlp_replay.domain.columns import Column, ColumnType, Source
from otlp_replay.domain.span import span_schema


def column(*sources, kind=ColumnType.STRING):
    return Column("c", tuple(Source.parse(s) for s in sources), kind)


def test_first_present_source_wins():
    c = column("http.request.method", "http.method")
    assert c.value({"http.method": "GET", "http.request.method": "POST"}, {}) == "POST"
    assert c.value({"http.method": "GET"}, {}) == "GET"
    assert c.value({}, {}) is None


def test_resource_sources_read_resource_attributes():
    c = column("resource:k8s.pod.name")
    assert c.value({"k8s.pod.name": "span"}, {"k8s.pod.name": "cart-0"}) == "cart-0"
    assert str(c.sources[0]) == "resource:k8s.pod.name"


@pytest.mark.parametrize(
    ("kind", "text", "expected"),
    [
        (ColumnType.INT, "404", 404),
        (ColumnType.INT, "n/a", None),
        (ColumnType.DOUBLE, "0.5", 0.5),
        (ColumnType.BOOLEAN, "True", True),
        (ColumnType.BOOLEAN, "maybe", None),
        (ColumnType.GRPC_STATUS, "14", "UNAVAILABLE"),
        (ColumnType.GRPC_STATUS, "OK", "OK"),
    ],
)
def test_values_are_converted_to_the_column_type(kind, text, expected):
    assert column("a", kind=kind).value({"a": text}, {}) == expected


def test_invalid_columns_are_refused():
    with pytest.raises(ValueError, match="invalid column name"):
        Column("Http-Method", (Source("http.method"),))
    with pytest.raises(ValueError, match="no source"):
        Column("http_method", ())


def test_schema_places_promoted_columns_before_the_maps():
    schema = span_schema([Column("tenant", (Source("tenant.id"),))])
    names = schema.names
    assert (
        names.index("scope_version") < names.index("tenant") < names.index("attributes")
    )


def test_schema_refuses_clashes_and_duplicates():
    with pytest.raises(ValueError, match="core columns"):
        span_schema([Column("service", (Source("x"),))])
    with pytest.raises(ValueError, match="unique"):
        span_schema([Column("a", (Source("x"),)), Column("a", (Source("y"),))])
