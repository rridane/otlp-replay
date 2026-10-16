"""SpansStore contract, for every implementation."""

from datetime import UTC, datetime

import pytest
from pyiceberg.catalog.sql import SqlCatalog

from otlp_replay.spans_store.iceberg.schema import NAMESPACE
from otlp_replay.spans_store.iceberg.spans_table import SpansTable
from otlp_replay.spans_store.memory.spans_store import (
    InMemorySpansStore,
)

H9 = datetime(2026, 10, 16, 9, tzinfo=UTC)


@pytest.fixture
def memory():
    return InMemorySpansStore()


@pytest.fixture
def iceberg(tmp_path):
    catalog = SqlCatalog(
        "test", uri="sqlite:///:memory:", warehouse=f"file://{tmp_path}"
    )
    catalog.create_namespace(NAMESPACE)
    return SpansTable(catalog)


@pytest.fixture(params=["memory", "iceberg"])
def store(request):
    return request.getfixturevalue(request.param)


def test_has_a_name_for_lineage(store):
    assert store.name


def test_ensure_is_idempotent(store):
    store.ensure()
    store.ensure()


def test_hour_never_written_has_no_snapshot(store):
    store.ensure()
    with pytest.raises(LookupError):
        store.hour_snapshot(H9)
