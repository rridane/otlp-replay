from datetime import UTC, datetime, timedelta, timezone

import pyarrow as pa
import pytest

from otlp_replay.domain.period import Period

H9 = datetime(2026, 10, 16, 9, tzinfo=UTC)
H10 = H9 + timedelta(hours=1)


def test_bounds_are_converted_to_utc():
    paris = timezone(timedelta(hours=2))
    period = Period(H9.astimezone(paris), H10.astimezone(paris))
    assert (period.start.tzinfo, period.end.tzinfo) == (UTC, UTC)
    assert (period.start, period.end) == (H9, H10)


def test_naive_bound_is_refused():
    with pytest.raises(ValueError, match="timezone"):
        Period(datetime(2026, 10, 16, 9), H10)  # noqa: DTZ001


def test_empty_period_is_refused():
    with pytest.raises(ValueError, match="before"):
        Period(H10, H9)


def test_deposits_extend_past_the_end_by_the_lateness():
    period = Period(H9, H9 + timedelta(hours=3), lateness=timedelta(minutes=12))
    assert period.deposits() == (H9, H9 + timedelta(hours=3, minutes=12))


def test_selects_spans_started_in_the_period():
    end = H9 + timedelta(minutes=90)
    starts = [H9 - timedelta(microseconds=1), H9, end - timedelta(microseconds=1), end]
    spans = pa.table({"start_time": pa.array(starts, pa.timestamp("us", tz="UTC"))})
    selected = Period(H9, end).select(spans)
    assert selected["start_time"].to_pylist() == starts[1:3]


def test_reads_as_an_interval():
    assert str(Period(H9, H10)) == "2026-10-16T09:00/2026-10-16T10:00"
