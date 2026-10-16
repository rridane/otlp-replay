from collections.abc import Callable, Iterator
from contextlib import contextmanager

from otlp_replay.domain.extraction_report import ExtractionReport
from otlp_replay.domain.period import Period
from otlp_replay.tracking.extraction_tracker import ExtractionTracker


class InMemoryExtractionTracker(ExtractionTracker):
    def __init__(self) -> None:
        self.reports: list[ExtractionReport] = []
        self.failures: list[tuple[Period, BaseException]] = []

    @contextmanager
    def track(self, period: Period) -> Iterator[Callable[[ExtractionReport], None]]:
        try:
            yield self.reports.append
        except Exception as error:
            self.failures.append((period, error))
            raise
