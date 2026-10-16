from datetime import UTC, datetime, timedelta

import pyarrow as pa

from otlp_replay.domain.period import Period
from otlp_replay.domain.raw_object import RawObject
from otlp_replay.domain.span import span_schema
from otlp_replay.domain.span_extractor import SpanExtractor

H9 = datetime(2026, 10, 16, 9, tzinfo=UTC)
SCHEMA = span_schema(())


class FakeDecoder:
    """Payload = minutes since 09:00, comma separated."""

    schema = SCHEMA

    def decode(self, payload: bytes, source: str) -> pa.Table:
        minutes = [int(m) for m in payload.decode().split(",")]
        return pa.Table.from_pylist(
            [
                {
                    "trace_id": "t",
                    "span_id": f"{source}{m}",
                    "name": "GET",
                    "kind": "SERVER",
                    "status_code": "UNSET",
                    "start_time": H9 + timedelta(minutes=m),
                    "duration_ns": 1,
                    "service": "cart",
                    "raw_object": source,
                }
                for m in minutes
            ],
            schema=SCHEMA,
        )


def test_decodes_every_object_and_keeps_the_period():
    extractor = SpanExtractor(FakeDecoder(), Period(H9, H9 + timedelta(hours=1)))
    spans = extractor.extract([RawObject("a", b"-1,0"), RawObject("b", b"59,60")])
    assert spans["span_id"].to_pylist() == ["a0", "b59"]


def test_no_object_gives_an_empty_table_with_the_contract():
    spans = SpanExtractor(FakeDecoder(), Period(H9, H9 + timedelta(hours=1))).extract(
        []
    )
    assert spans.schema == SCHEMA
    assert spans.num_rows == 0
