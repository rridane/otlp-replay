from abc import abstractmethod
from datetime import datetime
from typing import Protocol

from otlp_replay.domain.extraction_report import HourSnapshot


class SpansStore(Protocol):
    @property
    @abstractmethod
    def name(self) -> str:
        """Stable name of the destination, for lineage."""

    @abstractmethod
    def ensure(self) -> None:
        """Creates the destination if needed, otherwise checks its contract."""

    @abstractmethod
    def hour_snapshot(self, hour: datetime) -> HourSnapshot:
        """Latest version written for this hour; LookupError otherwise."""
