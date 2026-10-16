"""Streaming extraction on Ray Data: source, batched extraction, sort, sink."""

from collections.abc import Sequence

import pyarrow as pa

from otlp_replay.domain.extractor import Extractor
from otlp_replay.domain.hour_window import HourWindow
from otlp_replay.domain.raw_object import RawObject
from otlp_replay.domain.span import SORT_KEYS
from otlp_replay.pipeline.extraction_pipeline import ExtractionPipeline
from otlp_replay.pipeline.ray.hour_sink import HourSink
from otlp_replay.pipeline.ray.raw_object_source import (
    PATH,
    PAYLOAD,
    RawObjectSource,
)

OBJECTS_PER_BATCH = 8


class _ExtractBatch:
    def __init__(self, extractor: Extractor) -> None:
        self._extractor = extractor

    def __call__(self, batch: pa.Table) -> pa.Table:
        return self._extractor.extract(
            RawObject(path, payload)
            for path, payload in zip(
                batch[PATH].to_pylist(), batch[PAYLOAD].to_pylist(), strict=True
            )
        )


class RayDataExtractionPipeline(ExtractionPipeline):
    def __init__(self, source: RawObjectSource, sink: HourSink) -> None:
        self._source = source
        self._sink = sink

    def run(
        self, objects: Sequence[str], extractor: Extractor, window: HourWindow
    ) -> None:
        spans = (
            self._source.read(objects)
            .map_batches(
                _ExtractBatch(extractor),
                batch_format="pyarrow",
                batch_size=OBJECTS_PER_BATCH,
            )
            .sort(SORT_KEYS)
        )
        self._sink.replace(spans, window)
