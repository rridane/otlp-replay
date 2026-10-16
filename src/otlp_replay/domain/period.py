"""The extracted period and the deposit window of the raw objects that contain it."""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import pyarrow as pa
import pyarrow.compute as pc

# A span is exported after it ends: longest observed span (flagd gRPC stream,
# 600 s) + export delay (67 s) + margin.
LATENESS = timedelta(minutes=12)


@dataclass(frozen=True)
class Period:
    """Spans that started in [start, end), in UTC."""

    start: datetime
    end: datetime
    lateness: timedelta = LATENESS

    def __post_init__(self) -> None:
        if self.start.tzinfo is None or self.end.tzinfo is None:
            raise ValueError("period bounds must be timezone-aware")
        if self.start >= self.end:
            raise ValueError("period start must be before its end")
        object.__setattr__(self, "start", self.start.astimezone(UTC))
        object.__setattr__(self, "end", self.end.astimezone(UTC))

    def deposits(self) -> tuple[datetime, datetime]:
        """Deposit window to read: [start, end + lateness)."""
        return self.start, self.end + self.lateness

    def select(self, spans: pa.Table) -> pa.Table:
        """Spans that started within the period."""
        start = pc.field("start_time")
        return spans.filter((start >= self.start) & (start < self.end))

    def __str__(self) -> str:
        return f"{self.start:%Y-%m-%dT%H:%M}/{self.end:%Y-%m-%dT%H:%M}"
