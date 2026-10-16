from abc import abstractmethod
from typing import Protocol

import ray.data

from otlp_replay.domain.period import Period


class PeriodSink(Protocol):
    @abstractmethod
    def replace(self, spans: ray.data.Dataset, period: Period) -> None:
        """Replaces the period with these spans, in one commit."""
