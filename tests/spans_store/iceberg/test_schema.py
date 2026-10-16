import pytest
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.transforms import DayTransform

from otlp_replay.config import load_columns
from otlp_replay.domain.columns import Column, ColumnType, Source
from otlp_replay.domain.span import span_schema
from otlp_replay.spans_store.iceberg.schema import (
    spans_partition_spec,
    spans_schema,
    spans_sort_order,
)

DEFAULTS = load_columns()
CUSTOM = [Column(f"c_{t.value}", (Source("x"),), t) for t in ColumnType]
SPANS = spans_schema(DEFAULTS)


def _all_field_ids(field_type, ids):
    for field in getattr(field_type, "fields", ()):
        ids.append(field.field_id)
        _all_field_ids(field.field_type, ids)
    for attr in ("key_field", "value_field", "element_field"):
        if hasattr(field_type, attr):
            nested = getattr(field_type, attr)
            ids.append(nested.field_id)
            _all_field_ids(nested.field_type, ids)
    return ids


def test_field_ids_are_unique():
    ids = _all_field_ids(SPANS.as_struct(), [])
    assert len(ids) == len(set(ids))


def test_partitioned_by_day_of_start_time():
    (field,) = spans_partition_spec(SPANS).fields
    assert SPANS.find_column_name(field.source_id) == "start_time"
    assert isinstance(field.transform, DayTransform)


def test_sorted_by_service_name_start_time():
    order = spans_sort_order(SPANS)
    assert [SPANS.find_column_name(f.source_id) for f in order.fields] == [
        "service",
        "name",
        "start_time",
    ]


@pytest.mark.parametrize(
    "columns", [DEFAULTS, CUSTOM, ()], ids=["defaults", "every type", "none"]
)
def test_iceberg_schema_matches_the_arrow_schema(columns):
    arrow = schema_to_pyarrow(spans_schema(columns), include_field_ids=False)
    assert arrow.equals(span_schema(columns), check_metadata=True)
