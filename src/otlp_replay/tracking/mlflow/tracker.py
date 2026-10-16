"""One MLflow run per extracted period: metrics and lineage to the written version."""

from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager

import mlflow

from otlp_replay.domain.extraction_report import ExtractionReport
from otlp_replay.domain.period import Period
from otlp_replay.tracking.extraction_tracker import ExtractionTracker

EXPERIMENT = "spans-extraction"


class MlflowExtractionTracker(ExtractionTracker):
    def __init__(
        self,
        store: str,
        experiment: str = EXPERIMENT,
        tags: Mapping[str, str] | None = None,
    ) -> None:
        self._store = store
        self._experiment = experiment
        self._tags = dict(tags or {})

    @contextmanager
    def track(self, period: Period) -> Iterator[Callable[[ExtractionReport], None]]:
        """Run opened before the extraction: an exception ends it as FAILED."""
        mlflow.set_experiment(self._experiment)
        with mlflow.start_run(run_name=f"spans {period}"):
            mlflow.log_params(
                {
                    "start": period.start.isoformat(),
                    "end": period.end.isoformat(),
                    "store": self._store,
                }
            )
            mlflow.set_tags(self._tags)
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
