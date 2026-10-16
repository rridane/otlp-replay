"""Raw objects to the spans of the hour."""

from collections.abc import Iterable

import pyarrow as pa

from otlp_replay.decoding.trace_decoder import TraceDecoder
from otlp_replay.domain.extractor import Extractor
from otlp_replay.domain.hour_window import HourWindow
from otlp_replay.domain.raw_object import RawObject
from otlp_replay.domain.span import SPAN_SCHEMA


class SpanExtractor(Extractor):
    def __init__(self, decoder: TraceDecoder, window: HourWindow) -> None:
        self._decoder = decoder
        self._window = window

    def extract(self, objects: Iterable[RawObject]) -> pa.Table:
        spans = pa.concat_tables(
            [SPAN_SCHEMA.empty_table()]
            + [self._decoder.decode(o.payload, o.path) for o in objects]
        )
        return self._window.select(spans)
