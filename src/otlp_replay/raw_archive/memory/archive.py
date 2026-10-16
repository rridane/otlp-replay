from datetime import datetime

from otlp_replay.raw_archive.raw_trace_archive import RawTraceArchive


class InMemoryRawArchive(RawTraceArchive):
    def __init__(self) -> None:
        self._deposits: dict[str, tuple[datetime, bytes]] = {}

    def deposit(self, minute: datetime, key: str, payload: bytes) -> str:
        self._deposits[key] = (minute, payload)
        return key

    def objects(self, start: datetime, end: datetime) -> list[str]:
        return sorted(k for k, (m, _) in self._deposits.items() if start <= m < end)

    def read(self, key: str) -> bytes:
        return self._deposits[key][1]
