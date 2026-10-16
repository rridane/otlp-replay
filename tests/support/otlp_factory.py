"""Synthetic OTLP requests for tests."""

from datetime import datetime

from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import (
    ExportTraceServiceRequest,
)
from opentelemetry.proto.common.v1.common_pb2 import AnyValue, KeyValue
from opentelemetry.proto.trace.v1.trace_pb2 import Span

TRACE_ID = bytes.fromhex("0af7651916cd43dd8448eb211c80319c")


def nanos(moment: datetime) -> int:
    return int(moment.timestamp()) * 1_000_000_000 + moment.microsecond * 1_000


def span(span_id: str, start: datetime, duration_s: float = 0.001, **fields) -> Span:
    return Span(
        trace_id=TRACE_ID,
        span_id=bytes.fromhex(span_id),
        name="GET /api/cart",
        kind=Span.SPAN_KIND_SERVER,
        start_time_unix_nano=nanos(start),
        end_time_unix_nano=nanos(start) + int(duration_s * 1e9),
        **fields,
    )


def payload(*spans: Span, service: str = "cart") -> bytes:
    request = ExportTraceServiceRequest()
    resource_spans = request.resource_spans.add()
    resource_spans.resource.attributes.append(
        KeyValue(key="service.name", value=AnyValue(string_value=service))
    )
    resource_spans.scope_spans.add().spans.extend(spans)
    return request.SerializeToString()
