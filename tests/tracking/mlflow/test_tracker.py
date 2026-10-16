from datetime import UTC, datetime

import mlflow
import pytest

from otlp_replay.domain.extraction_report import ExtractionReport
from otlp_replay.tracking.mlflow.tracker import MlflowExtractionTracker

H9 = datetime(2026, 10, 16, 9, tzinfo=UTC)
REPORT = ExtractionReport(
    hour=H9,
    objects=72,
    spans_written=236_000,
    snapshot_id=7757998124640709241,
    duration_s=14.2,
)


@pytest.fixture(autouse=True)
def local_tracking(tmp_path, monkeypatch):
    monkeypatch.setenv("MLFLOW_TRACKING_URI", f"sqlite:///{tmp_path}/mlflow.db")


def tracker():
    return MlflowExtractionTracker("otel.spans", git_commit="abc1234")


def runs():
    return mlflow.search_runs(
        experiment_names=["spans-extraction"], output_format="list"
    )


def test_successful_extraction_is_recorded_with_lineage():
    with tracker().track(H9) as record:
        record(REPORT)
    (run,) = runs()
    assert run.info.status == "FINISHED"
    assert run.info.run_name == "spans 2026-10-16T09"
    assert run.data.params == {
        "hour": "2026-10-16T09:00:00+00:00",
        "store": "otel.spans",
    }
    assert run.data.metrics == {
        "objects": 72,
        "spans_written": 236_000,
        "duration_s": 14.2,
    }
    assert run.data.tags["store.snapshot_id"] == "7757998124640709241"
    assert run.data.tags["git.commit"] == "abc1234"


def test_failed_extraction_is_recorded_as_failed():
    with pytest.raises(RuntimeError), tracker().track(H9):
        raise RuntimeError("Polaris injoignable")
    (run,) = runs()
    assert run.info.status == "FAILED"
    assert "store.snapshot_id" not in run.data.tags
