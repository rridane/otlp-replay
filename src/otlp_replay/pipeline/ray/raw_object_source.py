from abc import abstractmethod
from collections.abc import Sequence
from typing import Protocol

import ray.data

PATH = "path"
PAYLOAD = "bytes"


class RawObjectSource(Protocol):
    @abstractmethod
    def read(self, objects: Sequence[str]) -> ray.data.Dataset:
        """Decompressed raw objects, with PATH and PAYLOAD columns."""
