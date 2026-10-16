from abc import abstractmethod
from typing import Protocol

import pyarrow as pa


class TraceDecoder(Protocol):
    @abstractmethod
    def decode(self, payload: bytes, source: str) -> pa.Table:
        """One raw object to its spans, following SPAN_SCHEMA."""
