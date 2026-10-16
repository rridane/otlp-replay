"""One MLflow run per extracted hour: metrics and lineage to the written version."""

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import datetime

import mlflow

from otlp_replay.domain.extraction_report import ExtractionReport
from otlp_replay.tracking.extraction_tracker import ExtractionTracker

EXPERIMENT = "spans-extraction"


class MlflowExtractionTracker(ExtractionTracker):
    def __init__(
        self,
        store: str,
        experiment: str = EXPERIMENT,
        git_commit: str | None = None,
    ) -> None:
        self._store = store
        self._experiment = experiment
        self._git_commit = git_commit

    @contextmanager
    def track(self, hour: datetime) -> Iterator[Callable[[ExtractionReport], None]]:
        """Run opened before the extraction: an exception ends it as FAILED."""
        mlflow.set_experiment(self._experiment)
        with mlflow.start_run(run_name=f"spans {hour:%Y-%m-%dT%H}"):
            mlflow.log_params({"hour": hour.isoformat(), "store": self._store})
            if self._git_commit:
                mlflow.set_tag("git.commit", self._git_commit)
            yield self._record

    @staticmethod
    def _record(report: ExtractionReport) -> None:
        mlflow.log_metrics(
            {
                "objects": report.objects,
                "spans_written": report.spans_written,
                "duration_s": report.duration_s,
            }
        )
        mlflow.set_tag("store.snapshot_id", str(report.snapshot_id))
