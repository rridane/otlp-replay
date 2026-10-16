from datetime import UTC, datetime, timedelta
from itertools import count

import pytest
from otlp_factory import payload, span

from otlp_replay.application.extract import Extract
from otlp_replay.decoding.otlp.decoder import OtlpTraceDecoder
from otlp_replay.domain.period import Period
from otlp_replay.pipeline.memory.pipeline import (
    InMemoryExtractionPipeline,
)
from otlp_replay.raw_archive.memory.archive import InMemoryRawArchive
from otlp_replay.spans_store.memory.spans_store import (
    InMemorySpansStore,
)
from otlp_replay.tracking.memory.tracker import (
    InMemoryExtractionTracker,
)

H9 = datetime(2026, 10, 16, 9, tzinfo=UTC)
PERIOD = Period(H9, H9 + timedelta(hours=1))


class SpyArchive(InMemoryRawArchive):
    def __init__(self) -> None:
        super().__init__()
        self.windows: list[tuple[datetime, datetime]] = []

    def objects(self, start: datetime, end: datetime) -> list[str]:
        self.windows.append((start, end))
        return super().objects(start, end)


@pytest.fixture
def archive():
    return SpyArchive()


@pytest.fixture
def store():
    return InMemorySpansStore()


@pytest.fixture
def tracker():
    return InMemoryExtractionTracker()


@pytest.fixture
def extract(archive, store, tracker):
    seconds = count(start=100, step=7)
    return Extract(
        archive,
        OtlpTraceDecoder(),
        InMemoryExtractionPipeline(archive, store),
        store,
        tracker,
        clock=lambda: float(next(seconds)),
    )


def test_reads_late_deposits_and_keeps_the_period(archive, store, tracker, extract):
    archive.deposit(
        H9.replace(minute=1),
        "a",
        payload(
            span("0000000000000001", H9 - timedelta(minutes=1)),
            span("0000000000000002", H9),
        ),
    )
    archive.deposit(
        H9 + timedelta(minutes=70),
        "b",
        payload(span("0000000000000003", H9.replace(minute=59), duration_s=600)),
    )
    archive.deposit(
        H9 + timedelta(minutes=72), "c", payload(span("0000000000000004", H9))
    )
    report = extract(PERIOD)
    assert archive.windows == [(H9, H9 + timedelta(hours=1, minutes=12))]
    assert sorted(store.scan()["span_id"].to_pylist()) == [
        "0000000000000002",
        "0000000000000003",
    ]
    assert (report.start, report.end) == (PERIOD.start, PERIOD.end)
    assert (report.objects, report.spans_written) == (2, 2)
    assert report.snapshot_id == store.snapshot(PERIOD).snapshot_id
    assert report.duration_s == 7.0
    assert tracker.reports == [report]


def test_period_without_raw_objects_fails_and_is_tracked(tracker, extract):
    with pytest.raises(FileNotFoundError):
        extract(PERIOD)
    assert [period for period, _ in tracker.failures] == [PERIOD]
    assert tracker.reports == []
