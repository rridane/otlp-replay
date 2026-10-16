from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class Snapshot:
    """Table version written for a period."""

    snapshot_id: int
    added_records: int


@dataclass(frozen=True)
class ExtractionReport:
    start: datetime
    end: datetime
    objects: int
    spans_written: int
    snapshot_id: int
    duration_s: float
