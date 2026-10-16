"""Iceberg `spans` table: one row per OTLP span."""

from pyiceberg import partitioning, schema, transforms, types
from pyiceberg.table import sorting

# Catalog configured through the PYICEBERG_CATALOG__LAKEHOUSE__* variables.
CATALOG = "lakehouse"
NAMESPACE = "otel"
TABLE = "spans"


def _str(field_id: int, name: str, required: bool = False) -> types.NestedField:
    return types.NestedField(field_id, name, types.StringType(), required=required)


def _map(field_id: int, name: str) -> types.NestedField:
    return types.NestedField(
        field_id,
        name,
        types.MapType(
            field_id + 1,
            types.StringType(),
            field_id + 2,
            types.StringType(),
            value_required=False,
        ),
        required=False,
    )


SPANS_SCHEMA = schema.Schema(
    _str(1, "trace_id", required=True),
    _str(2, "span_id", required=True),
    _str(3, "parent_span_id"),
    _str(4, "trace_state"),
    types.NestedField(5, "flags", types.IntegerType(), required=False),
    _str(6, "name", required=True),
    _str(7, "kind", required=True),
    _str(8, "status_code", required=True),
    _str(9, "status_message"),
    types.NestedField(10, "start_time", types.TimestamptzType(), required=True),
    types.NestedField(11, "duration_ns", types.LongType(), required=True),
    _str(12, "service", required=True),
    _str(13, "service_version"),
    _str(14, "service_instance_id"),
    _str(15, "k8s_namespace"),
    _str(16, "k8s_deployment"),
    _str(17, "k8s_pod"),
    _str(18, "k8s_node"),
    _str(19, "scope_name"),
    _str(20, "scope_version"),
    _str(21, "http_method"),
    _str(22, "http_route"),
    types.NestedField(23, "http_status_code", types.IntegerType(), required=False),
    _str(24, "rpc_system"),
    _str(25, "rpc_service"),
    _str(26, "rpc_method"),
    _str(27, "rpc_status_code"),
    _str(28, "db_system"),
    _str(29, "db_statement"),
    _str(30, "server_address"),
    _str(31, "peer_service"),
    _str(32, "messaging_system"),
    _str(33, "messaging_destination"),
    _map(40, "attributes"),
    _map(43, "resource_attributes"),
    types.NestedField(
        46,
        "events",
        types.ListType(
            47,
            types.StructType(
                types.NestedField(48, "time", types.TimestamptzType(), required=True),
                _str(49, "name", required=True),
                _map(50, "attributes"),
            ),
            element_required=True,
        ),
        required=False,
    ),
    types.NestedField(
        53,
        "links",
        types.ListType(
            54,
            types.StructType(
                _str(55, "trace_id", required=True),
                _str(56, "span_id", required=True),
                _map(57, "attributes"),
            ),
            element_required=True,
        ),
        required=False,
    ),
    _str(60, "raw_object", required=True),
)

SPANS_PARTITION_SPEC = partitioning.PartitionSpec(
    partitioning.PartitionField(
        source_id=10,
        field_id=1000,
        transform=transforms.DayTransform(),
        name="start_time_day",
    ),
)

SPANS_SORT_ORDER = sorting.SortOrder(
    *(
        sorting.SortField(
            source_id=i,
            transform=transforms.IdentityTransform(),
            direction=sorting.SortDirection.ASC,
            null_order=sorting.NullOrder.NULLS_LAST,
        )
        for i in (12, 6, 10)
    )
)
