from datetime import datetime
from itertools import count

import pyarrow as pa

from otlp_replay.domain.extraction_report import HourSnapshot
from otlp_replay.domain.span import SPAN_SCHEMA
from otlp_replay.spans_store.spans_store import SpansStore


class InMemorySpansStore(SpansStore):
    def __init__(self, name: str = "memory.spans") -> None:
        self._name = name
        self._hours: dict[datetime, pa.Table] = {}
        self._snapshots: dict[datetime, HourSnapshot] = {}
        self._ids = count(1)

    @property
    def name(self) -> str:
        return self._name

    def ensure(self) -> None:
        return None

    def replace(self, hour: datetime, spans: pa.Table) -> None:
        self._hours[hour] = spans
        self._snapshots[hour] = HourSnapshot(next(self._ids), spans.num_rows)

    def hour_snapshot(self, hour: datetime) -> HourSnapshot:
        try:
            return self._snapshots[hour]
        except KeyError:
            raise LookupError(f"no snapshot for {hour.isoformat()}") from None

    def scan(self) -> pa.Table:
        return pa.concat_tables([SPAN_SCHEMA.empty_table(), *self._hours.values()])
