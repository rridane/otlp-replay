"""RawTraceArchive contract, for every implementation."""

import gzip
from datetime import UTC, datetime

import pytest
from pyarrow.fs import LocalFileSystem

from otlp_replay.raw_archive.filesystem.archive import (
    FileSystemRawArchive,
)
from otlp_replay.raw_archive.memory.archive import InMemoryRawArchive


def at(hour, minute):
    return datetime(2026, 10, 16, hour, minute, tzinfo=UTC)


@pytest.fixture
def memory():
    archive = InMemoryRawArchive()
    return archive, lambda minute, name: archive.deposit(minute, name, b"otlp")


@pytest.fixture
def filesystem(tmp_path):
    def deposit(minute, name):
        directory = tmp_path / f"{minute:year=%Y/month=%m/day=%d/hour=%H/minute=%M}"
        directory.mkdir(parents=True, exist_ok=True)
        (directory / name).write_bytes(gzip.compress(b"otlp"))
        return str(directory / name)

    return FileSystemRawArchive(LocalFileSystem(), str(tmp_path)), deposit


@pytest.fixture(params=["memory", "filesystem"])
def archive(request):
    return request.getfixturevalue(request.param)


def test_lists_deposits_of_the_window_across_hours_in_order(archive):
    archive, deposit = archive
    deposit(at(8, 59), "before")
    inside = [
        deposit(at(10, 11), "c"),
        deposit(at(9, 0), "a"),
        deposit(at(9, 59), "b"),
    ]
    deposit(at(10, 12), "after")
    assert archive.objects(at(9, 0), at(10, 12)) == sorted(inside)


def test_window_without_deposits_is_empty(archive):
    archive, _ = archive
    assert archive.objects(at(9, 0), at(10, 0)) == []
