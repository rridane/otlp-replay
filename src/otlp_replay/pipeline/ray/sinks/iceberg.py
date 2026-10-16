import ray.data
from pyiceberg import catalog
from ray.data import expressions

from otlp_replay.domain.hour_window import HourWindow
from otlp_replay.pipeline.ray.hour_sink import HourSink
from otlp_replay.spans_store.iceberg.spans_table import hour_properties


class IcebergHourSink(HourSink):
    """Workers write the Parquet files; the driver commits delete + append."""

    def __init__(self, lakehouse: catalog.Catalog, identifier: str) -> None:
        self._catalog_kwargs = {"name": lakehouse.name, **lakehouse.properties}
        self._identifier = identifier

    def replace(self, spans: ray.data.Dataset, window: HourWindow) -> None:
        start = expressions.col("start_time")
        spans.write_iceberg(
            self._identifier,
            catalog_kwargs=self._catalog_kwargs,
            mode=ray.data.SaveMode.OVERWRITE,
            overwrite_filter=(start >= expressions.lit(window.start.isoformat()))
            & (start < expressions.lit(window.end.isoformat())),
            snapshot_properties=hour_properties(window.start),
        )
