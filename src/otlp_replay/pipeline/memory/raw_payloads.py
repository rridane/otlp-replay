from abc import abstractmethod
from typing import Protocol


class RawPayloads(Protocol):
    @abstractmethod
    def read(self, key: str) -> bytes:
        """Decompressed content of a raw object."""
