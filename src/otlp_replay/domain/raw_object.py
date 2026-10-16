from dataclasses import dataclass


@dataclass(frozen=True)
class RawObject:
    """A raw object read from the archive: its path and decompressed content."""

    path: str
    payload: bytes
