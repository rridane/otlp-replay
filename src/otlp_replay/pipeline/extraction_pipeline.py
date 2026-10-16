from abc import abstractmethod
from collections.abc import Sequence
from typing import Protocol

from otlp_replay.domain.extractor import Extractor
from otlp_replay.domain.period import Period


class ExtractionPipeline(Protocol):
    @abstractmethod
    def run(self, objects: Sequence[str], extractor: Extractor, period: Period) -> None:
        """Extracts the objects and atomically replaces the period in the store."""
