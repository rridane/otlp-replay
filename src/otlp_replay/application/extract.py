"""Use case: replace a period of the spans store from the raw objects."""

import time
from collections.abc import Callable

from otlp_replay.decoding.trace_decoder import TraceDecoder
from otlp_replay.domain.extraction_report import ExtractionReport
from otlp_replay.domain.period import Period
from otlp_replay.domain.span_extractor import SpanExtractor
from otlp_replay.pipeline.extraction_pipeline import ExtractionPipeline
from otlp_replay.raw_archive.raw_trace_archive import RawTraceArchive
from otlp_replay.spans_store.spans_store import SpansStore
from otlp_replay.tracking.extraction_tracker import ExtractionTracker


class Extract:
    def __init__(
        self,
        archive: RawTraceArchive,
        decoder: TraceDecoder,
        pipeline: ExtractionPipeline,
        store: SpansStore,
        tracker: ExtractionTracker,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._archive = archive
        self._decoder = decoder
        self._pipeline = pipeline
        self._store = store
        self._tracker = tracker
        self._clock = clock

    def __call__(self, period: Period) -> ExtractionReport:
        with self._tracker.track(period) as record:
            started = self._clock()
            self._store.ensure()
            objects = self._archive.objects(*period.deposits())
            if not objects:
                raise FileNotFoundError(f"no raw object for {period}")
            self._pipeline.run(objects, SpanExtractor(self._decoder, period), period)
            snapshot = self._store.snapshot(period)
            report = ExtractionReport(
                start=period.start,
                end=period.end,
                objects=len(objects),
                spans_written=snapshot.added_records,
                snapshot_id=snapshot.snapshot_id,
                duration_s=round(self._clock() - started, 1),
            )
            record(report)
        return report
