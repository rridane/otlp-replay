"""ExtractionPipeline contract, for every implementation.

Ray: inside a Ray job, raw objects and warehouse under LAKE_TEST_ROOT (S3),
sqlite catalog on the driver.
"""

import gzip
import os
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import pyarrow as pa
import pytest
from otlp_factory import payload, span

from otlp_replay.decoding.otlp.decoder import OtlpTraceDecoder
from otlp_replay.domain.hour_window import HourWindow
from otlp_replay.domain.span_extractor import SpanExtractor
from otlp_replay.pipeline.extraction_pipeline import ExtractionPipeline
from otlp_replay.pipeline.memory.pipeline import (
    InMemoryExtractionPipeline,
)
from otlp_replay.raw_archive.memory.archive import InMemoryRawArchive
from otlp_replay.spans_store.memory.spans_store import (
    InMemorySpansStore,
)
from otlp_replay.spans_store.spans_store import SpansStore

H9 = datetime(2026, 10, 16, 9, tzinfo=UTC)
H10 = H9 + timedelta(hours=1)


@dataclass
class World:
    pipeline: ExtractionPipeline
    store: SpansStore
    deposit: Callable[[str, bytes], str]
    scan: Callable[[], pa.Table]

    def run(self, objects: list[str], hour: datetime) -> None:
        window = HourWindow.containing(hour)
        self.pipeline.run(objects, SpanExtractor(OtlpTraceDecoder(), window), window)

    def span_ids(self) -> list[str]:
        return sorted(self.scan()["span_id"].to_pylist())


@pytest.fixture
def memory():
    archive, store = InMemoryRawArchive(), InMemorySpansStore()
    return World(
        InMemoryExtractionPipeline(archive, store),
        store,
        lambda name, content: archive.deposit(H9, name, content),
        store.scan,
    )


@pytest.fixture
def ray(tmp_path):
    from pyarrow.fs import S3FileSystem
    from pyiceberg.catalog import load_catalog

    from otlp_replay.pipeline.ray.pipeline import (
        RayDataExtractionPipeline,
    )
    from otlp_replay.pipeline.ray.sinks.iceberg import IcebergHourSink
    from otlp_replay.pipeline.ray.sources.gzip_binary_files import (
        GzipBinaryFiles,
    )
    from otlp_replay.spans_store.iceberg.schema import NAMESPACE
    from otlp_replay.spans_store.iceberg.spans_table import SpansTable

    s3 = {
        "s3.endpoint": os.environ["OTLP_RAW_S3_ENDPOINT"],
        "s3.region": os.environ["OTLP_RAW_S3_REGION"],
    }
    fs = S3FileSystem(
        endpoint_override=s3["s3.endpoint"],
        region=s3["s3.region"],
        force_virtual_addressing=False,
    )
    root = f"{os.environ['LAKE_TEST_ROOT']}/{uuid.uuid4()}"
    catalog = load_catalog(
        "test",
        type="sql",
        uri=f"sqlite:///{tmp_path}/catalog.db",
        warehouse=f"s3://{root}/warehouse",
        **s3,
    )
    catalog.create_namespace(NAMESPACE)
    store = SpansTable(catalog)
    store.ensure()

    def deposit(name: str, content: bytes) -> str:
        path = f"{root}/raw/{name}.binpb.gz"
        with fs.open_output_stream(path, compression=None) as out:
            out.write(gzip.compress(content))
        return path

    yield World(
        RayDataExtractionPipeline(
            GzipBinaryFiles(fs), IcebergHourSink(catalog, store.name)
        ),
        store,
        deposit,
        lambda: catalog.load_table(store.name).scan().to_arrow(),
    )
    fs.delete_dir(root)


@pytest.fixture(params=["memory", pytest.param("ray", marks=pytest.mark.ray)])
def world(request) -> World:
    return request.getfixturevalue(request.param)


def test_writes_the_spans_of_the_hour(world):
    objects = [
        world.deposit(
            "a",
            payload(
                span("0000000000000001", H9 - timedelta(minutes=1)),
                span("0000000000000002", H9),
            ),
        ),
        world.deposit("b", payload(span("0000000000000003", H9.replace(minute=59)))),
    ]
    world.run(objects, H9)
    assert world.span_ids() == ["0000000000000002", "0000000000000003"]
    assert world.store.hour_snapshot(H9).added_records == 2


def test_rerun_replaces_the_hour_and_keeps_others(world):
    objects = [
        world.deposit("a", payload(span("0000000000000002", H9))),
        world.deposit("b", payload(span("0000000000000010", H10))),
    ]
    world.run(objects, H10)
    world.run(objects, H9)
    first = world.store.hour_snapshot(H9).snapshot_id
    world.run(objects, H9)
    assert world.span_ids() == ["0000000000000002", "0000000000000010"]
    assert world.store.hour_snapshot(H9).snapshot_id != first
    assert world.store.hour_snapshot(H10).added_records == 1
