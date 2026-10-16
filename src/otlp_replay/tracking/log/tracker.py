import logging
from collections.abc import Callable, Iterator
from contextlib import contextmanager

from otlp_replay.domain.extraction_report import ExtractionReport
from otlp_replay.domain.period import Period
from otlp_replay.tracking.extraction_tracker import ExtractionTracker

log = logging.getLogger(__name__)


class LoggingExtractionTracker(ExtractionTracker):
    """Tracks extractions in the logs only, for setups without an experiment tracker."""

    @contextmanager
    def track(self, period: Period) -> Iterator[Callable[[ExtractionReport], None]]:
        log.info("extracting %s", period)
        try:
            yield self._record
        except Exception:
            log.exception("extraction of %s failed", period)
            raise

    @staticmethod
    def _record(report: ExtractionReport) -> None:
        log.info("extracted %s", report)
