from abc import abstractmethod
from datetime import datetime
from typing import Protocol

import pyarrow as pa


class HourWriter(Protocol):
    @abstractmethod
    def replace(self, hour: datetime, spans: pa.Table) -> None:
        """Replaces the hour with these spans."""
