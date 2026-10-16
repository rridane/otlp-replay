from abc import abstractmethod
from typing import Protocol

import pyarrow as pa

from otlp_replay.domain.period import Period


class PeriodWriter(Protocol):
    @abstractmethod
    def replace(self, period: Period, spans: pa.Table) -> None:
        """Replaces the period with these spans."""
