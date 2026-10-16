"""OTLP values (AnyValue) to text."""

import base64
import json
from collections.abc import Iterable

from opentelemetry.proto.common.v1 import common_pb2


def to_text(value: common_pb2.AnyValue) -> str | None:
    match value.WhichOneof("value"):
        case "string_value":
            return value.string_value
        case "bool_value":
            return "true" if value.bool_value else "false"
        case "int_value":
            return str(value.int_value)
        case "double_value":
            return repr(value.double_value)
        case None:
            return None
        case _:
            return json.dumps(_to_json(value), separators=(",", ":"))


def to_pairs(attributes: Iterable[common_pb2.KeyValue]) -> list[tuple[str, str | None]]:
    return [(kv.key, to_text(kv.value)) for kv in attributes]


def _to_json(value: common_pb2.AnyValue):
    match value.WhichOneof("value"):
        case "array_value":
            return [_to_json(v) for v in value.array_value.values]
        case "kvlist_value":
            return {kv.key: _to_json(kv.value) for kv in value.kvlist_value.values}
        case "bytes_value":
            return base64.b64encode(value.bytes_value).decode()
        case None:
            return None
        case kind:
            return getattr(value, kind)
