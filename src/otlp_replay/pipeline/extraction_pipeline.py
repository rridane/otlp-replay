from abc import abstractmethod
from collections.abc import Sequence
from typing import Protocol

from otlp_replay.domain.extractor import Extractor
from otlp_replay.domain.hour_window import HourWindow


class ExtractionPipeline(Protocol):
    @abstractmethod
    def run(
        self, objects: Sequence[str], extractor: Extractor, window: HourWindow
    ) -> None:
        """Extracts the objects and atomically replaces the hour in the store."""
