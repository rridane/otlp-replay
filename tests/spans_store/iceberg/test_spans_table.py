from datetime import UTC, datetime

import pytest
from otlp_factory import payload, span
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType

from otlp_replay.decoding.otlp.decoder import OtlpTraceDecoder
from otlp_replay.spans_store.iceberg.schema import (
    NAMESPACE,
    SPANS_SCHEMA,
    TABLE,
)
from otlp_replay.spans_store.iceberg.spans_table import (
    SchemaDriftError,
    SpansTable,
    hour_properties,
)


@pytest.fixture
def catalog(tmp_path):
    catalog = SqlCatalog(
        "test", uri="sqlite:///:memory:", warehouse=f"file://{tmp_path}"
    )
    catalog.create_namespace(NAMESPACE)
    return catalog


def test_creates_table_with_contract(catalog):
    SpansTable(catalog).ensure()
    table = SpansTable(catalog).create_or_load()
    assert [f.name for f in table.schema().fields] == [
        f.name for f in SPANS_SCHEMA.fields
    ]
    assert str(table.spec().fields[0].transform) == "day"
    assert table.properties["write.parquet.compression-codec"] == "zstd"


def test_second_call_reuses_existing_table(catalog):
    first = SpansTable(catalog).create_or_load()
    SpansTable(catalog).ensure()
    second = SpansTable(catalog).create_or_load()
    assert second.metadata.table_uuid == first.metadata.table_uuid


def test_refuses_table_with_other_schema(catalog):
    catalog.create_table(
        (NAMESPACE, TABLE),
        schema=Schema(NestedField(1, "trace_id", StringType(), required=True)),
    )
    with pytest.raises(SchemaDriftError, match="schema"):
        SpansTable(catalog).ensure()


def test_refuses_table_with_other_partitioning(catalog):
    catalog.create_table(
        (NAMESPACE, TABLE),
        schema=SPANS_SCHEMA,
        partition_spec=UNPARTITIONED_PARTITION_SPEC,
    )
    with pytest.raises(SchemaDriftError, match="partition"):
        SpansTable(catalog).ensure()


def test_finds_the_last_snapshot_of_an_hour(catalog):
    h9 = datetime(2026, 10, 16, 9, tzinfo=UTC)
    spans = SpansTable(catalog)
    rows = OtlpTraceDecoder().decode(payload(span("0000000000000002", h9)), "a")
    table = spans.create_or_load()
    for _ in range(2):
        table.append(rows, snapshot_properties=hour_properties(h9))
    snapshot, current = spans.hour_snapshot(h9), table.refresh().current_snapshot()
    assert current is not None
    assert snapshot.snapshot_id == current.snapshot_id
    assert snapshot.added_records == 1
