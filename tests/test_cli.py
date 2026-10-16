from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from otlp_replay.cli import parse

H9 = datetime(2026, 10, 16, 9, tzinfo=UTC)
REQUIRED = ["extract", "--from", "2026-10-16T09:00Z", "--to", "2026-10-16T12:00Z"]


def test_minimal_request_uses_the_defaults():
    request = parse([*REQUIRED, "--archive", "s3://bucket/traces"])
    assert (request.period.start, request.period.end) == (H9, H9 + timedelta(hours=3))
    assert request.period.lateness == timedelta(minutes=12)
    assert (request.catalog, request.namespace, request.tracking) == (
        "default",
        "otel",
        "log",
    )
    assert (request.columns, request.tags) == (None, {})


def test_every_option_is_read():
    request = parse(
        [
            *REQUIRED,
            "--archive", "file:///data/traces",
            "--catalog", "lakehouse",
            "--namespace", "trace_transformer",
            "--columns", "columns.yaml",
            "--lateness", "5",
            "--tracking", "mlflow",
            "--tag", "git.commit=abc1234",
            "--tag", "dag=spans_hourly",
        ]
    )  # fmt: skip
    assert request.period.lateness == timedelta(minutes=5)
    assert (request.catalog, request.namespace) == ("lakehouse", "trace_transformer")
    assert request.columns == Path("columns.yaml")
    assert request.tags == {"git.commit": "abc1234", "dag": "spans_hourly"}


@pytest.mark.parametrize(
    "argv",
    [
        [
            "extract",
            "--from",
            "2026-10-16T12:00Z",
            "--to",
            "2026-10-16T09:00Z",
            "--archive",
            "x",
        ],
        [
            "extract",
            "--from",
            "2026-10-16T09:00",
            "--to",
            "2026-10-16T12:00",
            "--archive",
            "x",
        ],
        [*REQUIRED, "--archive", "x", "--tag", "no-equal-sign"],
        [*REQUIRED, "--archive", "x", "--tracking", "wandb"],
        [*REQUIRED],
    ],
    ids=["reversed period", "naive time", "bad tag", "unknown tracking", "no archive"],
)
def test_invalid_requests_exit_with_a_usage_error(argv):
    with pytest.raises(SystemExit) as exit_info:
        parse(argv)
    assert exit_info.value.code == 2
