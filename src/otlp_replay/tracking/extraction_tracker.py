from abc import abstractmethod
from collections.abc import Callable
from contextlib import AbstractContextManager
from datetime import datetime
from typing import Protocol

from otlp_replay.domain.extraction_report import ExtractionReport


class ExtractionTracker(Protocol):
    @abstractmethod
    def track(
        self, hour: datetime
    ) -> AbstractContextManager[Callable[[ExtractionReport], None]]:
        """Tracks an extraction; an exception marks it as failed."""
