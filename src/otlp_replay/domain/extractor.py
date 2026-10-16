from abc import abstractmethod
from collections.abc import Iterable
from typing import Protocol

import pyarrow as pa

from otlp_replay.domain.raw_object import RawObject


class Extractor(Protocol):
    @abstractmethod
    def extract(self, objects: Iterable[RawObject]) -> pa.Table:
        """Raw objects to selected spans, following SPAN_SCHEMA."""
