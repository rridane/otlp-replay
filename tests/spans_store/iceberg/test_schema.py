from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.transforms import DayTransform

from otlp_replay.domain.span import SPAN_SCHEMA
from otlp_replay.spans_store.iceberg.schema import (
    SPANS_PARTITION_SPEC,
    SPANS_SCHEMA,
    SPANS_SORT_ORDER,
)


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
    ids = _all_field_ids(SPANS_SCHEMA.as_struct(), [])
    assert len(ids) == len(set(ids))


def test_partitioned_by_day_of_start_time():
    (field,) = SPANS_PARTITION_SPEC.fields
    assert SPANS_SCHEMA.find_column_name(field.source_id) == "start_time"
    assert isinstance(field.transform, DayTransform)


def test_sorted_by_service_name_start_time():
    names = [
        SPANS_SCHEMA.find_column_name(f.source_id) for f in SPANS_SORT_ORDER.fields
    ]
    assert names == ["service", "name", "start_time"]


def test_partition_and_sort_columns_are_required():
    sources = [f.source_id for f in SPANS_PARTITION_SPEC.fields] + [
        f.source_id for f in SPANS_SORT_ORDER.fields
    ]
    assert all(SPANS_SCHEMA.find_field(i).required for i in sources)


def test_iceberg_schema_matches_the_domain_contract():
    arrow = schema_to_pyarrow(SPANS_SCHEMA, include_field_ids=False)
    assert arrow.equals(SPAN_SCHEMA, check_metadata=True)
