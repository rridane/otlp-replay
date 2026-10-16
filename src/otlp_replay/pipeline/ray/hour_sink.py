from abc import abstractmethod
from typing import Protocol

import ray.data

from otlp_replay.domain.hour_window import HourWindow


class HourSink(Protocol):
    @abstractmethod
    def replace(self, spans: ray.data.Dataset, window: HourWindow) -> None:
        """Replaces the hour with these spans, in one commit."""
