from datetime import UTC, datetime, timedelta

import pytest
from otlp_factory import payload, span
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType

from otlp_replay.config import load_columns
from otlp_replay.decoding.otlp.decoder import OtlpTraceDecoder
from otlp_replay.domain.columns import Column, ColumnType, Source
from otlp_replay.domain.period import Period
from otlp_replay.spans_store.iceberg.schema import NAMESPACE, TABLE, spans_schema
from otlp_replay.spans_store.iceberg.spans_table import (
    SchemaDriftError,
    SpansTable,
    period_properties,
)

COLUMNS = load_columns()
TENANT = Column("tenant", (Source("tenant.id"),))


@pytest.fixture
def catalog(tmp_path):
    catalog = SqlCatalog(
        "test", uri="sqlite:///:memory:", warehouse=f"file://{tmp_path}"
    )
    catalog.create_namespace(NAMESPACE)
    return catalog


def column_names(catalog):
    return [f.name for f in catalog.load_table((NAMESPACE, TABLE)).schema().fields]


def test_creates_table_with_contract(catalog):
    SpansTable(catalog, COLUMNS).ensure()
    table = SpansTable(catalog, COLUMNS).create_or_load()
    assert column_names(catalog) == [f.name for f in spans_schema(COLUMNS).fields]
    assert str(table.spec().fields[0].transform) == "day"
    assert table.properties["write.parquet.compression-codec"] == "zstd"


def test_second_call_reuses_existing_table(catalog):
    first = SpansTable(catalog, COLUMNS).create_or_load()
    SpansTable(catalog, COLUMNS).ensure()
    second = SpansTable(catalog, COLUMNS).create_or_load()
    assert second.metadata.table_uuid == first.metadata.table_uuid


def test_new_configured_column_is_added_and_old_rows_read_null(catalog):
    h9 = datetime(2026, 10, 16, 9, tzinfo=UTC)
    before = SpansTable(catalog, COLUMNS)
    before.ensure()
    rows = OtlpTraceDecoder(COLUMNS).decode(payload(span("0000000000000002", h9)), "a")
    before.create_or_load().append(rows)
    SpansTable(catalog, [*COLUMNS, TENANT]).ensure()
    assert column_names(catalog)[-1] == "tenant"
    scanned = catalog.load_table((NAMESPACE, TABLE)).scan().to_arrow()
    assert scanned["tenant"].to_pylist() == [None]


def test_column_order_does_not_matter(catalog):
    SpansTable(catalog, COLUMNS).ensure()
    SpansTable(catalog, list(reversed(COLUMNS))).ensure()


def test_removing_a_column_is_refused(catalog):
    SpansTable(catalog, [*COLUMNS, TENANT]).ensure()
    with pytest.raises(SchemaDriftError, match="removing columns is refused"):
        SpansTable(catalog, COLUMNS).ensure()


def test_changing_a_type_is_refused(catalog):
    SpansTable(catalog, [*COLUMNS, TENANT]).ensure()
    retyped = Column("tenant", TENANT.sources, ColumnType.LONG)
    with pytest.raises(SchemaDriftError, match="changed type"):
        SpansTable(catalog, [*COLUMNS, retyped]).ensure()


def test_refuses_table_without_the_core_columns(catalog):
    catalog.create_table(
        (NAMESPACE, TABLE),
        schema=Schema(NestedField(1, "trace_id", StringType(), required=True)),
    )
    with pytest.raises(SchemaDriftError, match="core columns"):
        SpansTable(catalog, COLUMNS).ensure()


def test_refuses_table_with_other_partitioning(catalog):
    catalog.create_table(
        (NAMESPACE, TABLE),
        schema=spans_schema(COLUMNS),
        partition_spec=UNPARTITIONED_PARTITION_SPEC,
    )
    with pytest.raises(SchemaDriftError, match="partition"):
        SpansTable(catalog, COLUMNS).ensure()


def test_finds_the_last_snapshot_of_a_period(catalog):
    h9 = datetime(2026, 10, 16, 9, tzinfo=UTC)
    period = Period(h9, h9 + timedelta(hours=1))
    spans = SpansTable(catalog, COLUMNS)
    rows = OtlpTraceDecoder(COLUMNS).decode(payload(span("0000000000000002", h9)), "a")
    table = spans.create_or_load()
    for _ in range(2):
        table.append(rows, snapshot_properties=period_properties(period))
    snapshot, current = spans.snapshot(period), table.refresh().current_snapshot()
    assert current is not None
    assert snapshot.snapshot_id == current.snapshot_id
    assert snapshot.added_records == 1
