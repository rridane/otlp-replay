from opentelemetry.proto.common.v1.common_pb2 import (
    AnyValue,
    ArrayValue,
    KeyValue,
    KeyValueList,
)

from otlp_replay.decoding.otlp.values import to_pairs, to_text


def test_scalars():
    assert to_text(AnyValue(string_value="cart")) == "cart"
    assert to_text(AnyValue(bool_value=True)) == "true"
    assert to_text(AnyValue(int_value=3)) == "3"
    assert to_text(AnyValue(double_value=12.5)) == "12.5"
    assert to_text(AnyValue()) is None


def test_composites_become_compact_json():
    array = ArrayValue(values=[AnyValue(string_value="a"), AnyValue(int_value=1)])
    kvlist = KeyValueList(values=[KeyValue(key="k", value=AnyValue(bool_value=False))])
    assert to_text(AnyValue(array_value=array)) == '["a",1]'
    assert to_text(AnyValue(kvlist_value=kvlist)) == '{"k":false}'
    assert to_text(AnyValue(bytes_value=b"\x00\xff")) == '"AP8="'


def test_pairs_keep_order_and_keys():
    attributes = [
        KeyValue(key="b", value=AnyValue(int_value=2)),
        KeyValue(key="a", value=AnyValue(string_value="x")),
    ]
    assert to_pairs(attributes) == [("b", "2"), ("a", "x")]
