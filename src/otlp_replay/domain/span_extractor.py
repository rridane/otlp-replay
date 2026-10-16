"""Raw objects to the spans of the period."""

from collections.abc import Iterable

import pyarrow as pa

from otlp_replay.decoding.trace_decoder import TraceDecoder
from otlp_replay.domain.extractor import Extractor
from otlp_replay.domain.period import Period
from otlp_replay.domain.raw_object import RawObject


class SpanExtractor(Extractor):
    def __init__(self, decoder: TraceDecoder, period: Period) -> None:
        self._decoder = decoder
        self._period = period

    def extract(self, objects: Iterable[RawObject]) -> pa.Table:
        spans = pa.concat_tables(
            [self._decoder.schema.empty_table()]
            + [self._decoder.decode(o.payload, o.path) for o in objects]
        )
        return self._period.select(spans)
