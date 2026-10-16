from abc import abstractmethod
from typing import Protocol

from otlp_replay.domain.extraction_report import Snapshot
from otlp_replay.domain.period import Period


class SpansStore(Protocol):
    @property
    @abstractmethod
    def name(self) -> str:
        """Stable name of the destination, for lineage."""

    @abstractmethod
    def ensure(self) -> None:
        """Creates the destination if needed, otherwise checks its contract."""

    @abstractmethod
    def snapshot(self, period: Period) -> Snapshot:
        """Latest version written for this period; LookupError otherwise."""
