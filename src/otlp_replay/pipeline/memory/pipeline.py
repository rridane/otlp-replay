from collections.abc import Sequence

from otlp_replay.domain.extractor import Extractor
from otlp_replay.domain.hour_window import HourWindow
from otlp_replay.domain.raw_object import RawObject
from otlp_replay.domain.span import SORT_KEYS
from otlp_replay.pipeline.extraction_pipeline import ExtractionPipeline
from otlp_replay.pipeline.memory.hour_writer import HourWriter
from otlp_replay.pipeline.memory.raw_payloads import RawPayloads


class InMemoryExtractionPipeline(ExtractionPipeline):
    def __init__(self, payloads: RawPayloads, writer: HourWriter) -> None:
        self._payloads = payloads
        self._writer = writer

    def run(
        self, objects: Sequence[str], extractor: Extractor, window: HourWindow
    ) -> None:
        spans = extractor.extract(RawObject(o, self._payloads.read(o)) for o in objects)
        self._writer.replace(
            window.start, spans.sort_by([(k, "ascending") for k in SORT_KEYS])
        )
