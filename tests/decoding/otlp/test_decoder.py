from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import (
    ExportTraceServiceRequest,
)
from opentelemetry.proto.common.v1.common_pb2 import AnyValue, KeyValue
from opentelemetry.proto.trace.v1.trace_pb2 import Span, Status

from otlp_replay.config import load_columns
from otlp_replay.decoding.otlp.decoder import (
    OtlpTraceDecoder,
)
from otlp_replay.domain.columns import Column, ColumnType, Source

TRACE_ID = bytes.fromhex("0af7651916cd43dd8448eb211c80319c")
START = 1_760_605_200_123_456_789
RAW_OBJECT = "minute=00/x.binpb.gz"
DECODER = OtlpTraceDecoder(load_columns())


def kv(key, value):
    return KeyValue(key=key, value=AnyValue(string_value=value))


def span(span_id, **fields):
    return Span(
        trace_id=TRACE_ID,
        span_id=bytes.fromhex(span_id),
        name="GET /api/cart",
        kind=Span.SPAN_KIND_SERVER,
        start_time_unix_nano=START,
        end_time_unix_nano=START + 1_500,
        **fields,
    )


def payload(*batches):
    """batches: (resource attributes, scope name, spans)."""
    request = ExportTraceServiceRequest()
    for resource, scope, spans in batches:
        resource_spans = request.resource_spans.add()
        resource_spans.resource.attributes.extend(resource)
        scope_spans = resource_spans.scope_spans.add()
        scope_spans.scope.name = scope
        scope_spans.spans.extend(spans)
    return request.SerializeToString()


def convert(*batches, converter=None):
    return (converter or DECODER).decode(payload(*batches), RAW_OBJECT)


CART = [kv("service.name", "cart"), kv("k8s.pod.name", "cart-0")]


def test_output_follows_the_decoder_schema():
    assert (
        convert((CART, "otelgrpc", [span("00f067aa0ba902b7")])).schema == DECODER.schema
    )


def test_flattens_resources_scopes_and_spans():
    table = convert(
        (CART, "otelgrpc", [span("1111111111111111"), span("2222222222222222")]),
        ([kv("service.name", "email")], "sinatra", [span("3333333333333333")]),
    )
    rows = table.to_pylist()
    assert [(r["service"], r["scope_name"], r["span_id"]) for r in rows] == [
        ("cart", "otelgrpc", "1111111111111111"),
        ("cart", "otelgrpc", "2222222222222222"),
        ("email", "sinatra", "3333333333333333"),
    ]
    assert rows[0]["k8s_pod"] == "cart-0"
    assert rows[2]["k8s_pod"] is None


def test_identity_time_and_lineage():
    (row,) = convert((CART, "otelgrpc", [span("00f067aa0ba902b7")])).to_pylist()
    assert row["trace_id"] == TRACE_ID.hex()
    assert row["parent_span_id"] is None
    assert (row["kind"], row["status_code"]) == ("SERVER", "UNSET")
    assert row["start_time"].timestamp() == START // 1_000 / 1e6
    assert row["duration_ns"] == 1_500
    assert row["raw_object"] == RAW_OBJECT


def test_child_span_with_error():
    child = span(
        "1111111111111111",
        parent_span_id=bytes.fromhex("00f067aa0ba902b7"),
        status=Status(code=Status.STATUS_CODE_ERROR, message="boom"),
    )
    (row,) = convert((CART, "otelgrpc", [child])).to_pylist()
    assert row["parent_span_id"] == "00f067aa0ba902b7"
    assert (row["status_code"], row["status_message"]) == ("ERROR", "boom")


def test_missing_service_name_falls_back_to_spec_default():
    (row,) = convert(([], "otelgrpc", [span("00f067aa0ba902b7")])).to_pylist()
    assert row["service"] == "unknown_service"


def test_attributes_are_kept_and_normalized():
    s = span("00f067aa0ba902b7", attributes=[kv("http.method", "GET")])
    (row,) = convert((CART, "otelgrpc", [s])).to_pylist()
    assert row["http_method"] == "GET"
    assert row["attributes"] == [("http.method", "GET")]
    assert dict(row["resource_attributes"]) == {
        "service.name": "cart",
        "k8s.pod.name": "cart-0",
    }


def test_only_the_configured_columns_are_promoted():
    columns = [
        Column("verb", (Source("custom.verb"),)),
        Column("pod", (Source("k8s.pod.name", resource=True),)),
        Column("retries", (Source("retry.count"),), ColumnType.INT),
    ]
    s = span(
        "00f067aa0ba902b7",
        attributes=[kv("custom.verb", "PUT"), kv("retry.count", "3")],
    )
    (row,) = convert(
        (CART, "otelgrpc", [s]), converter=OtlpTraceDecoder(columns)
    ).to_pylist()
    assert (row["verb"], row["pod"], row["retries"]) == ("PUT", "cart-0", 3)
    assert "http_method" not in row


def test_events_and_links():
    s = span("00f067aa0ba902b7")
    event = s.events.add(time_unix_nano=START + 1_000_000, name="exception")
    event.attributes.append(kv("exception.type", "Timeout"))
    s.links.add(trace_id=TRACE_ID, span_id=bytes.fromhex("2222222222222222"))
    (row,) = convert((CART, "otelgrpc", [s])).to_pylist()
    (ev,) = row["events"]
    assert (ev["name"], ev["attributes"]) == (
        "exception",
        [("exception.type", "Timeout")],
    )
    assert row["links"][0]["span_id"] == "2222222222222222"
