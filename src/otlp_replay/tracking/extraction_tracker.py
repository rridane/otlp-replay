from abc import abstractmethod
from collections.abc import Callable
from contextlib import AbstractContextManager
from typing import Protocol

from otlp_replay.domain.extraction_report import ExtractionReport
from otlp_replay.domain.period import Period


class ExtractionTracker(Protocol):
    @abstractmethod
    def track(
        self, period: Period
    ) -> AbstractContextManager[Callable[[ExtractionReport], None]]:
        """Tracks an extraction; an exception marks it as failed."""
