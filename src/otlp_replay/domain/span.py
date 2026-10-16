"""One row per span: the contract of the `spans` table."""

from collections.abc import Sequence

import pyarrow as pa

from otlp_replay.domain.columns import Column, ColumnType

TIMESTAMP = pa.timestamp("us", tz="UTC")
TEXT = pa.large_string()
ATTRIBUTES = pa.map_(pa.field("key", TEXT, nullable=False), TEXT)


def _text(name: str, required: bool = False) -> pa.Field:
    return pa.field(name, TEXT, nullable=not required)


def _list_of(*fields: pa.Field) -> pa.DataType:
    return pa.large_list(pa.field("element", pa.struct(fields), nullable=False))


HEAD = (
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
    _text("scope_name"),
    _text("scope_version"),
)

TAIL = (
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
)

CORE_COLUMNS = frozenset(f.name for f in (*HEAD, *TAIL))

ARROW_TYPES: dict[ColumnType, pa.DataType] = {
    ColumnType.STRING: TEXT,
    ColumnType.INT: pa.int32(),
    ColumnType.LONG: pa.int64(),
    ColumnType.DOUBLE: pa.float64(),
    ColumnType.BOOLEAN: pa.bool_(),
    ColumnType.GRPC_STATUS: TEXT,
}


def span_schema(columns: Sequence[Column]) -> pa.Schema:
    """Core columns, with the promoted columns between the span fields and the maps."""
    clashes = sorted(c.name for c in columns if c.name in CORE_COLUMNS)
    if clashes:
        raise ValueError(f"promoted columns clash with core columns: {clashes}")
    names = [c.name for c in columns]
    if len(names) != len(set(names)):
        raise ValueError("promoted column names must be unique")
    promoted = [pa.field(c.name, ARROW_TYPES[c.type]) for c in columns]
    return pa.schema([*HEAD, *promoted, *TAIL])


SORT_KEYS = ["service", "name", "start_time"]
