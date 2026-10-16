"""The extracted hour and the deposit window of the raw objects that contain it."""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import pyarrow as pa
import pyarrow.compute as pc

HOUR = timedelta(hours=1)

# A span is exported after it ends: longest observed span (flagd gRPC stream,
# 600 s) + export delay (67 s) + margin.
LATENESS = timedelta(minutes=12)


@dataclass(frozen=True)
class HourWindow:
    start: datetime
    lateness: timedelta = LATENESS

    @classmethod
    def containing(
        cls, moment: datetime, lateness: timedelta = LATENESS
    ) -> "HourWindow":
        if moment.tzinfo is None:
            raise ValueError("the hour must be timezone-aware (UTC)")
        start = moment.astimezone(UTC).replace(minute=0, second=0, microsecond=0)
        return cls(start, lateness)

    @property
    def end(self) -> datetime:
        return self.start + HOUR

    def deposits(self) -> tuple[datetime, datetime]:
        """Deposit minutes to read: [start, end + lateness)."""
        return self.start, self.end + self.lateness

    def select(self, spans: pa.Table) -> pa.Table:
        """Spans that started within the hour."""
        start = pc.field("start_time")
        return spans.filter((start >= self.start) & (start < self.end))
