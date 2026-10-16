"""Iceberg `spans` table: one row per OTLP span, generated from the promoted columns."""

from collections.abc import Iterator, Sequence
from itertools import count

from pyiceberg import partitioning, schema, transforms, types
from pyiceberg.table import sorting

from otlp_replay.domain.columns import Column, ColumnType

# Catalog configured through the PYICEBERG_CATALOG__LAKEHOUSE__* variables.
CATALOG = "lakehouse"
NAMESPACE = "otel"
TABLE = "spans"

ICEBERG_TYPES: dict[ColumnType, types.PrimitiveType] = {
    ColumnType.STRING: types.StringType(),
    ColumnType.INT: types.IntegerType(),
    ColumnType.LONG: types.LongType(),
    ColumnType.DOUBLE: types.DoubleType(),
    ColumnType.BOOLEAN: types.BooleanType(),
    ColumnType.GRPC_STATUS: types.StringType(),
}


def promoted_field(field_id: int, column: Column) -> types.NestedField:
    return types.NestedField(
        field_id, column.name, ICEBERG_TYPES[column.type], required=False
    )


def spans_schema(columns: Sequence[Column]) -> schema.Schema:
    ids = count(1)
    return schema.Schema(
        *_head(ids),
        *(promoted_field(next(ids), c) for c in columns),
        *_tail(ids),
    )


def spans_partition_spec(spans: schema.Schema) -> partitioning.PartitionSpec:
    return partitioning.PartitionSpec(
        partitioning.PartitionField(
            source_id=spans.find_field("start_time").field_id,
            field_id=1000,
            transform=transforms.DayTransform(),
            name="start_time_day",
        )
    )


def spans_sort_order(spans: schema.Schema) -> sorting.SortOrder:
    return sorting.SortOrder(
        *(
            sorting.SortField(
                source_id=spans.find_field(name).field_id,
                transform=transforms.IdentityTransform(),
                direction=sorting.SortDirection.ASC,
                null_order=sorting.NullOrder.NULLS_LAST,
            )
            for name in ("service", "name", "start_time")
        )
    )


def _str(ids: Iterator[int], name: str, required: bool = False) -> types.NestedField:
    return types.NestedField(next(ids), name, types.StringType(), required=required)


def _map(ids: Iterator[int], name: str) -> types.NestedField:
    field_id, key_id, value_id = next(ids), next(ids), next(ids)
    return types.NestedField(
        field_id,
        name,
        types.MapType(
            key_id,
            types.StringType(),
            value_id,
            types.StringType(),
            value_required=False,
        ),
        required=False,
    )


def _head(ids: Iterator[int]) -> list[types.NestedField]:
    return [
        _str(ids, "trace_id", required=True),
        _str(ids, "span_id", required=True),
        _str(ids, "parent_span_id"),
        _str(ids, "trace_state"),
        types.NestedField(next(ids), "flags", types.IntegerType(), required=False),
        _str(ids, "name", required=True),
        _str(ids, "kind", required=True),
        _str(ids, "status_code", required=True),
        _str(ids, "status_message"),
        types.NestedField(
            next(ids), "start_time", types.TimestamptzType(), required=True
        ),
        types.NestedField(next(ids), "duration_ns", types.LongType(), required=True),
        _str(ids, "service", required=True),
        _str(ids, "scope_name"),
        _str(ids, "scope_version"),
    ]


def _tail(ids: Iterator[int]) -> list[types.NestedField]:
    attributes = _map(ids, "attributes")
    resource_attributes = _map(ids, "resource_attributes")
    events_id, event_id = next(ids), next(ids)
    events = types.NestedField(
        events_id,
        "events",
        types.ListType(
            event_id,
            types.StructType(
                types.NestedField(
                    next(ids), "time", types.TimestamptzType(), required=True
                ),
                _str(ids, "name", required=True),
                _map(ids, "attributes"),
            ),
            element_required=True,
        ),
        required=False,
    )
    links_id, link_id = next(ids), next(ids)
    links = types.NestedField(
        links_id,
        "links",
        types.ListType(
            link_id,
            types.StructType(
                _str(ids, "trace_id", required=True),
                _str(ids, "span_id", required=True),
                _map(ids, "attributes"),
            ),
            element_required=True,
        ),
        required=False,
    )
    return [
        attributes,
        resource_attributes,
        events,
        links,
        _str(ids, "raw_object", required=True),
    ]
