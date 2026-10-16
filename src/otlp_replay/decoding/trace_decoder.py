from abc import abstractmethod
from typing import Protocol

import pyarrow as pa


class TraceDecoder(Protocol):
    @property
    @abstractmethod
    def schema(self) -> pa.Schema:
        """Schema of the decoded spans."""

    @abstractmethod
    def decode(self, payload: bytes, source: str) -> pa.Table:
        """One raw object to its spans, following `schema`."""
