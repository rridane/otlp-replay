from datetime import UTC, datetime, timedelta, timezone

import pyarrow as pa
import pytest

from otlp_replay.domain.hour_window import HourWindow

H9 = datetime(2026, 10, 16, 9, tzinfo=UTC)


def test_truncates_to_the_utc_hour():
    paris = timezone(timedelta(hours=2))
    window = HourWindow.containing(datetime(2026, 10, 16, 11, 42, tzinfo=paris))
    assert (window.start, window.end) == (H9, H9 + timedelta(hours=1))


def test_naive_moment_is_refused():
    with pytest.raises(ValueError, match="timezone"):
        HourWindow.containing(datetime(2026, 10, 16, 9))  # noqa: DTZ001


def test_deposits_extend_past_the_hour_by_the_lateness():
    window = HourWindow.containing(H9, lateness=timedelta(minutes=12))
    assert window.deposits() == (H9, H9 + timedelta(hours=1, minutes=12))


def test_selects_spans_started_in_the_hour():
    starts = [
        H9 - timedelta(microseconds=1),
        H9,
        H9 + timedelta(minutes=59),
        H9 + timedelta(hours=1),
    ]
    spans = pa.table({"start_time": pa.array(starts, pa.timestamp("us", tz="UTC"))})
    selected = HourWindow.containing(H9).select(spans)
    assert selected["start_time"].to_pylist() == starts[1:3]
