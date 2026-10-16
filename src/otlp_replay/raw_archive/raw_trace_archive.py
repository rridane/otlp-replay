from abc import abstractmethod
from datetime import datetime
from typing import Protocol


class RawTraceArchive(Protocol):
    @abstractmethod
    def objects(self, start: datetime, end: datetime) -> list[str]:
        """Objects deposited during [start, end), sorted."""
