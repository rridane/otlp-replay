"""The Iceberg `spans` table in the catalog."""

from datetime import datetime

from pyiceberg import catalog, partitioning, schema, table
from pyiceberg.table import sorting

from otlp_replay.domain.extraction_report import HourSnapshot
from otlp_replay.spans_store.iceberg.schema import (
    NAMESPACE,
    SPANS_PARTITION_SPEC,
    SPANS_SCHEMA,
    SPANS_SORT_ORDER,
    TABLE,
)
from otlp_replay.spans_store.spans_store import SpansStore

TABLE_PROPERTIES = {
    "format-version": "2",
    "write.parquet.compression-codec": "zstd",
    "write.metadata.delete-after-commit.enabled": "true",
    "write.metadata.previous-versions-max": "100",
}


HOUR_PROPERTY = "trace-transformer.hour"


class SchemaDriftError(RuntimeError):
    pass


def hour_properties(hour: datetime) -> dict[str, str]:
    """Properties of the snapshot that replaces an hour."""
    return {HOUR_PROPERTY: hour.isoformat()}


class SpansTable(SpansStore):
    def __init__(self, lakehouse: catalog.Catalog, namespace: str = NAMESPACE) -> None:
        self._lakehouse = lakehouse
        self._identifier = (namespace, TABLE)

    @property
    def name(self) -> str:
        return ".".join(self._identifier)

    def ensure(self) -> None:
        self._check_contract(self.create_or_load())

    def create_or_load(self) -> table.Table:
        return self._lakehouse.create_table_if_not_exists(
            self._identifier,
            schema=SPANS_SCHEMA,
            partition_spec=SPANS_PARTITION_SPEC,
            sort_order=SPANS_SORT_ORDER,
            properties=TABLE_PROPERTIES,
        )

    def hour_snapshot(self, hour: datetime) -> HourSnapshot:
        loaded = self._lakehouse.load_table(self._identifier)
        for snapshot in reversed(loaded.snapshots()):
            properties = (
                snapshot.summary.additional_properties if snapshot.summary else {}
            )
            if properties.get(HOUR_PROPERTY) == hour.isoformat():
                return HourSnapshot(
                    snapshot_id=snapshot.snapshot_id,
                    added_records=int(properties.get("added-records", 0)),
                )
        raise LookupError(f"no snapshot for {hour.isoformat()}")

    @staticmethod
    def _check_contract(loaded: table.Table) -> None:
        current = loaded.schema()
        if _structure(current) != _structure(SPANS_SCHEMA):
            raise SchemaDriftError("schema differs from SPANS_SCHEMA")
        if _partitioning(current, loaded.spec()) != _partitioning(
            SPANS_SCHEMA, SPANS_PARTITION_SPEC
        ):
            raise SchemaDriftError("partitioning differs from SPANS_PARTITION_SPEC")
        if _sorting(current, loaded.sort_order()) != _sorting(
            SPANS_SCHEMA, SPANS_SORT_ORDER
        ):
            raise SchemaDriftError("sort order differs from SPANS_SORT_ORDER")


# The catalog renumbers fields on creation: compare names, types and
# nullability, not field ids.
def _structure(of: schema.Schema) -> schema.Schema:
    return schema.assign_fresh_schema_ids(of)


def _partitioning(
    of: schema.Schema, spec: partitioning.PartitionSpec
) -> list[tuple[str, str]]:
    return [(of.find_field(f.source_id).name, str(f.transform)) for f in spec.fields]


def _sorting(of: schema.Schema, order: sorting.SortOrder) -> list[tuple[str, str]]:
    return [(of.find_field(f.source_id).name, str(f.direction)) for f in order.fields]
