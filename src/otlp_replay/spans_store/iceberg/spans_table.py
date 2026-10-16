"""The Iceberg `spans` table in the catalog."""

from collections.abc import Sequence

from pyiceberg import catalog, partitioning, schema, table, types
from pyiceberg.table import sorting

from otlp_replay.domain.columns import Column
from otlp_replay.domain.extraction_report import Snapshot
from otlp_replay.domain.period import Period
from otlp_replay.domain.span import CORE_COLUMNS
from otlp_replay.spans_store.iceberg.schema import (
    NAMESPACE,
    TABLE,
    spans_partition_spec,
    spans_schema,
    spans_sort_order,
)
from otlp_replay.spans_store.spans_store import SpansStore

TABLE_PROPERTIES = {
    "format-version": "2",
    "write.parquet.compression-codec": "zstd",
    "write.metadata.delete-after-commit.enabled": "true",
    "write.metadata.previous-versions-max": "100",
}


START_PROPERTY = "otlp-replay.start"
END_PROPERTY = "otlp-replay.end"


class SchemaDriftError(RuntimeError):
    pass


def period_properties(period: Period) -> dict[str, str]:
    """Properties of the snapshot that replaces a period."""
    return {
        START_PROPERTY: period.start.isoformat(),
        END_PROPERTY: period.end.isoformat(),
    }


class SpansTable(SpansStore):
    def __init__(
        self,
        lakehouse: catalog.Catalog,
        columns: Sequence[Column],
        namespace: str = NAMESPACE,
    ) -> None:
        self._lakehouse = lakehouse
        self._identifier = (namespace, TABLE)
        self._schema = spans_schema(columns)

    @property
    def name(self) -> str:
        return ".".join(self._identifier)

    def ensure(self) -> None:
        loaded = self.create_or_load()
        self._evolve(loaded)
        self._check_layout(loaded)

    def create_or_load(self) -> table.Table:
        return self._lakehouse.create_table_if_not_exists(
            self._identifier,
            schema=self._schema,
            partition_spec=spans_partition_spec(self._schema),
            sort_order=spans_sort_order(self._schema),
            properties=TABLE_PROPERTIES,
        )

    def snapshot(self, period: Period) -> Snapshot:
        loaded = self._lakehouse.load_table(self._identifier)
        for snapshot in reversed(loaded.snapshots()):
            properties = (
                snapshot.summary.additional_properties if snapshot.summary else {}
            )
            if all(
                properties.get(k) == v for k, v in period_properties(period).items()
            ):
                return Snapshot(
                    snapshot_id=snapshot.snapshot_id,
                    added_records=int(properties.get("added-records", 0)),
                )
        raise LookupError(f"no snapshot for {period}")

    def _evolve(self, loaded: table.Table) -> None:
        """Adds the configured columns the table lacks; refuses any other change."""
        expected = {f.name: f for f in self._schema.fields}
        current = {f.name: f for f in loaded.schema().fields}
        removed = sorted(current.keys() - expected.keys())
        if removed:
            raise SchemaDriftError(
                f"columns {removed} are in the table but not in the configuration:"
                " removing columns is refused"
            )
        changed = sorted(
            name
            for name in current.keys() & expected.keys()
            if _shape(current[name]) != _shape(expected[name])
        )
        if changed:
            raise SchemaDriftError(f"columns {changed} changed type: refused")
        added = [f for name, f in expected.items() if name not in current]
        missing_core = sorted(f.name for f in added if f.name in CORE_COLUMNS)
        if missing_core:
            raise SchemaDriftError(f"core columns {missing_core} are missing")
        if added:
            with loaded.update_schema() as update:
                for field in added:
                    update.add_column(field.name, field.field_type)

    def _check_layout(self, loaded: table.Table) -> None:
        current = loaded.schema()
        if _partitioning(current, loaded.spec()) != _partitioning(
            self._schema, spans_partition_spec(self._schema)
        ):
            raise SchemaDriftError("partitioning differs from the expected layout")
        if _sorting(current, loaded.sort_order()) != _sorting(
            self._schema, spans_sort_order(self._schema)
        ):
            raise SchemaDriftError("sort order differs from the expected layout")


# The catalog renumbers fields on creation: compare names, types and
# nullability, not field ids.
def _shape(field: types.NestedField) -> schema.Schema:
    return schema.assign_fresh_schema_ids(schema.Schema(field))


def _partitioning(
    of: schema.Schema, spec: partitioning.PartitionSpec
) -> list[tuple[str, str]]:
    return [(of.find_field(f.source_id).name, str(f.transform)) for f in spec.fields]


def _sorting(of: schema.Schema, order: sorting.SortOrder) -> list[tuple[str, str]]:
    return [(of.find_field(f.source_id).name, str(f.direction)) for f in order.fields]
