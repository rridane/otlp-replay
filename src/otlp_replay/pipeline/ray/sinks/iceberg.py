import ray.data
from pyiceberg import catalog
from ray.data import expressions

from otlp_replay.domain.period import Period
from otlp_replay.pipeline.ray.period_sink import PeriodSink
from otlp_replay.spans_store.iceberg.spans_table import period_properties


class IcebergPeriodSink(PeriodSink):
    """Workers write the Parquet files; the driver commits delete + append."""

    def __init__(self, lakehouse: catalog.Catalog, identifier: str) -> None:
        self._catalog_kwargs = {"name": lakehouse.name, **lakehouse.properties}
        self._identifier = identifier

    def replace(self, spans: ray.data.Dataset, period: Period) -> None:
        start = expressions.col("start_time")
        spans.write_iceberg(
            self._identifier,
            catalog_kwargs=self._catalog_kwargs,
            mode=ray.data.SaveMode.OVERWRITE,
            overwrite_filter=(start >= expressions.lit(period.start.isoformat()))
            & (start < expressions.lit(period.end.isoformat())),
            snapshot_properties=period_properties(period),
        )
