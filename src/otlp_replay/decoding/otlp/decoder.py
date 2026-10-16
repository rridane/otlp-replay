"""OTLP request (ExportTraceServiceRequest) to spans following SPAN_SCHEMA."""

from collections.abc import Iterator, Mapping

import pyarrow as pa
from opentelemetry.proto.collector.trace.v1 import trace_service_pb2
from opentelemetry.proto.common.v1 import common_pb2
from opentelemetry.proto.trace.v1 import trace_pb2

from otlp_replay.decoding.otlp.values import to_pairs
from otlp_replay.decoding.trace_decoder import TraceDecoder
from otlp_replay.domain.semantic_conventions import SemanticConventions
from otlp_replay.domain.span import SPAN_SCHEMA

# Value mandated by the OTLP specification when service.name is missing.
UNKNOWN_SERVICE = "unknown_service"

RESOURCE_COLUMNS = {
    "service_version": "service.version",
    "service_instance_id": "service.instance.id",
    "k8s_namespace": "k8s.namespace.name",
    "k8s_deployment": "k8s.deployment.name",
    "k8s_pod": "k8s.pod.name",
    "k8s_node": "k8s.node.name",
}

KIND = {
    k: trace_pb2.Span.SpanKind.Name(k).removeprefix("SPAN_KIND_")
    for k in trace_pb2.Span.SpanKind.values()
}
STATUS = {
    c: trace_pb2.Status.StatusCode.Name(c).removeprefix("STATUS_CODE_")
    for c in trace_pb2.Status.StatusCode.values()
}


class OtlpTraceDecoder(TraceDecoder):
    """Flattens resource, scope and span into one row per span."""

    def __init__(self, conventions: SemanticConventions | None = None) -> None:
        self._conventions = conventions or SemanticConventions()

    def decode(self, payload: bytes, source: str) -> pa.Table:
        request = trace_service_pb2.ExportTraceServiceRequest.FromString(payload)
        return pa.Table.from_pylist(
            list(self._rows(request, source)), schema=SPAN_SCHEMA
        )

    def _rows(
        self, request: trace_service_pb2.ExportTraceServiceRequest, raw_object: str
    ) -> Iterator[dict]:
        for resource_spans in request.resource_spans:
            resource = dict(to_pairs(resource_spans.resource.attributes))
            for scope_spans in resource_spans.scope_spans:
                for span in scope_spans.spans:
                    yield self._span_row(span, resource, scope_spans.scope, raw_object)

    def _span_row(
        self,
        span: trace_pb2.Span,
        resource: Mapping[str, str | None],
        scope: common_pb2.InstrumentationScope,
        raw_object: str,
    ) -> dict:
        attributes = to_pairs(span.attributes)
        return {
            "trace_id": span.trace_id.hex(),
            "span_id": span.span_id.hex(),
            "parent_span_id": span.parent_span_id.hex() or None,
            "trace_state": span.trace_state or None,
            "flags": span.flags,
            "name": span.name,
            "kind": KIND[span.kind],
            "status_code": STATUS[span.status.code],
            "status_message": span.status.message or None,
            "start_time": span.start_time_unix_nano // 1_000,
            "duration_ns": span.end_time_unix_nano - span.start_time_unix_nano,
            "service": resource.get("service.name") or UNKNOWN_SERVICE,
            **{column: resource.get(key) for column, key in RESOURCE_COLUMNS.items()},
            "scope_name": scope.name or None,
            "scope_version": scope.version or None,
            **self._conventions.normalize(dict(attributes)),
            "attributes": attributes,
            "resource_attributes": list(resource.items()),
            "events": self._events(span),
            "links": self._links(span),
            "raw_object": raw_object,
        }

    @staticmethod
    def _events(span: trace_pb2.Span) -> list[dict]:
        return [
            {
                "time": event.time_unix_nano // 1_000,
                "name": event.name,
                "attributes": to_pairs(event.attributes),
            }
            for event in span.events
        ]

    @staticmethod
    def _links(span: trace_pb2.Span) -> list[dict]:
        return [
            {
                "trace_id": link.trace_id.hex(),
                "span_id": link.span_id.hex(),
                "attributes": to_pairs(link.attributes),
            }
            for link in span.links
        ]
