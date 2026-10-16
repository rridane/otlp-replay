"""SpansStore contract, for every implementation."""

from datetime import UTC, datetime, timedelta

import pytest
from pyiceberg.catalog.sql import SqlCatalog

from otlp_replay.config import load_columns
from otlp_replay.domain.period import Period
from otlp_replay.domain.span import span_schema
from otlp_replay.spans_store.iceberg.schema import NAMESPACE
from otlp_replay.spans_store.iceberg.spans_table import SpansTable
from otlp_replay.spans_store.memory.spans_store import (
    InMemorySpansStore,
)

H9 = datetime(2026, 10, 16, 9, tzinfo=UTC)
COLUMNS = load_columns()
PERIOD = Period(H9, H9 + timedelta(hours=1))


@pytest.fixture
def memory():
    return InMemorySpansStore(span_schema(COLUMNS))


@pytest.fixture
def iceberg(tmp_path):
    catalog = SqlCatalog(
        "test", uri="sqlite:///:memory:", warehouse=f"file://{tmp_path}"
    )
    catalog.create_namespace(NAMESPACE)
    return SpansTable(catalog, COLUMNS)


@pytest.fixture(params=["memory", "iceberg"])
def store(request):
    return request.getfixturevalue(request.param)


def test_has_a_name_for_lineage(store):
    assert store.name


def test_ensure_is_idempotent(store):
    store.ensure()
    store.ensure()


def test_period_never_written_has_no_snapshot(store):
    store.ensure()
    with pytest.raises(LookupError):
        store.snapshot(PERIOD)
