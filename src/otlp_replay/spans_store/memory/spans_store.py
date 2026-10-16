from itertools import count

import pyarrow as pa
import pyarrow.compute as pc

from otlp_replay.domain.extraction_report import Snapshot
from otlp_replay.domain.period import Period
from otlp_replay.domain.span import SPAN_SCHEMA
from otlp_replay.spans_store.spans_store import SpansStore


class InMemorySpansStore(SpansStore):
    """Same semantics as the Iceberg table: replacing a period overwrites the
    spans that started in it, whatever was written before."""

    def __init__(self, name: str = "memory.spans") -> None:
        self._name = name
        self._spans = SPAN_SCHEMA.empty_table()
        self._snapshots: dict[Period, Snapshot] = {}
        self._ids = count(1)

    @property
    def name(self) -> str:
        return self._name

    def ensure(self) -> None:
        return None

    def replace(self, period: Period, spans: pa.Table) -> None:
        start = pc.field("start_time")
        kept = self._spans.filter((start < period.start) | (start >= period.end))
        self._spans = pa.concat_tables([kept, spans])
        self._snapshots[period] = Snapshot(next(self._ids), spans.num_rows)

    def snapshot(self, period: Period) -> Snapshot:
        try:
            return self._snapshots[period]
        except KeyError:
            raise LookupError(f"no snapshot for {period}") from None

    def scan(self) -> pa.Table:
        return self._spans
