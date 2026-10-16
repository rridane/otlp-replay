from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class HourSnapshot:
    """Table version written for an hour."""

    snapshot_id: int
    added_records: int


@dataclass(frozen=True)
class ExtractionReport:
    hour: datetime
    objects: int
    spans_written: int
    snapshot_id: int
    duration_s: float
