"""One row per span: the contract of the `spans` table."""

import pyarrow as pa

TIMESTAMP = pa.timestamp("us", tz="UTC")
TEXT = pa.large_string()
ATTRIBUTES = pa.map_(pa.field("key", TEXT, nullable=False), TEXT)


def _text(name: str, required: bool = False) -> pa.Field:
    return pa.field(name, TEXT, nullable=not required)


def _list_of(*fields: pa.Field) -> pa.DataType:
    return pa.large_list(pa.field("element", pa.struct(fields), nullable=False))


SPAN_SCHEMA = pa.schema(
    [
        _text("trace_id", required=True),
        _text("span_id", required=True),
        _text("parent_span_id"),
        _text("trace_state"),
        pa.field("flags", pa.int32()),
        _text("name", required=True),
        _text("kind", required=True),
        _text("status_code", required=True),
        _text("status_message"),
        pa.field("start_time", TIMESTAMP, nullable=False),
        pa.field("duration_ns", pa.int64(), nullable=False),
        _text("service", required=True),
        _text("service_version"),
        _text("service_instance_id"),
        _text("k8s_namespace"),
        _text("k8s_deployment"),
        _text("k8s_pod"),
        _text("k8s_node"),
        _text("scope_name"),
        _text("scope_version"),
        _text("http_method"),
        _text("http_route"),
        pa.field("http_status_code", pa.int32()),
        _text("rpc_system"),
        _text("rpc_service"),
        _text("rpc_method"),
        _text("rpc_status_code"),
        _text("db_system"),
        _text("db_statement"),
        _text("server_address"),
        _text("peer_service"),
        _text("messaging_system"),
        _text("messaging_destination"),
        pa.field("attributes", ATTRIBUTES),
        pa.field("resource_attributes", ATTRIBUTES),
        pa.field(
            "events",
            _list_of(
                pa.field("time", TIMESTAMP, nullable=False),
                _text("name", required=True),
                pa.field("attributes", ATTRIBUTES),
            ),
        ),
        pa.field(
            "links",
            _list_of(
                _text("trace_id", required=True),
                _text("span_id", required=True),
                pa.field("attributes", ATTRIBUTES),
            ),
        ),
        _text("raw_object", required=True),
    ]
)

SORT_KEYS = ["service", "name", "start_time"]
