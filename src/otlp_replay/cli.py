"""otlp-replay extract --from 2026-10-16T09:00Z --to 2026-10-16T12:00Z --archive s3://bucket/traces"""

import argparse
import json
import logging
from collections.abc import Sequence
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from pathlib import Path

from pyarrow import fs

from otlp_replay.application.extract import Extract
from otlp_replay.config import load_columns
from otlp_replay.decoding.otlp.decoder import OtlpTraceDecoder
from otlp_replay.decoding.trace_decoder import TraceDecoder
from otlp_replay.domain.extraction_report import ExtractionReport
from otlp_replay.domain.period import LATENESS, Period
from otlp_replay.pipeline.extraction_pipeline import ExtractionPipeline
from otlp_replay.raw_archive.filesystem.archive import FileSystemRawArchive
from otlp_replay.raw_archive.raw_trace_archive import RawTraceArchive
from otlp_replay.spans_store.spans_store import SpansStore
from otlp_replay.tracking.extraction_tracker import ExtractionTracker
from otlp_replay.tracking.log.tracker import LoggingExtractionTracker


@dataclass(frozen=True)
class ExtractRequest:
    period: Period
    archive: str
    catalog: str = "default"
    namespace: str = "otel"
    columns: Path | None = None
    tracking: str = "log"
    tags: dict[str, str] = field(default_factory=dict)


def parse(argv: Sequence[str] | None = None) -> ExtractRequest:
    parser = argparse.ArgumentParser(prog="otlp-replay")
    commands = parser.add_subparsers(dest="command", required=True)
    extract = commands.add_parser("extract", help="replace a period of the spans table")
    extract.add_argument(
        "--from", dest="start", required=True, type=datetime.fromisoformat
    )
    extract.add_argument("--to", dest="end", required=True, type=datetime.fromisoformat)
    extract.add_argument("--archive", required=True, help="raw OTLP archive URI")
    extract.add_argument("--catalog", default="default", help="pyiceberg catalog name")
    extract.add_argument("--namespace", default="otel")
    extract.add_argument("--columns", type=Path, help="promoted columns YAML file")
    extract.add_argument(
        "--lateness",
        type=int,
        default=int(LATENESS.total_seconds() // 60),
        help="minutes",
    )
    extract.add_argument("--tracking", choices=["log", "mlflow"], default="log")
    extract.add_argument(
        "--tag", action="append", default=[], type=_tag, metavar="KEY=VALUE"
    )
    args = parser.parse_args(argv)
    try:
        period = Period(args.start, args.end, timedelta(minutes=args.lateness))
    except ValueError as error:
        parser.error(str(error))
    return ExtractRequest(
        period=period,
        archive=args.archive,
        catalog=args.catalog,
        namespace=args.namespace,
        columns=args.columns,
        tracking=args.tracking,
        tags=dict(args.tag),
    )


def extract(request: ExtractRequest) -> ExtractionReport:
    # Optional extras, imported only when an extraction runs.
    import ray
    from pyiceberg import catalog

    from otlp_replay.pipeline.ray.period_sink import PeriodSink
    from otlp_replay.pipeline.ray.pipeline import RayDataExtractionPipeline
    from otlp_replay.pipeline.ray.raw_object_source import RawObjectSource
    from otlp_replay.pipeline.ray.sinks.iceberg import IcebergPeriodSink
    from otlp_replay.pipeline.ray.sources.gzip_binary_files import GzipBinaryFiles
    from otlp_replay.spans_store.iceberg.spans_table import SpansTable

    raw_fs, root = fs.FileSystem.from_uri(request.archive)
    lakehouse: catalog.Catalog = catalog.load_catalog(request.catalog)
    ray.init()

    columns = load_columns(request.columns)
    archive: RawTraceArchive = FileSystemRawArchive(raw_fs, root)
    decoder: TraceDecoder = OtlpTraceDecoder(columns)
    store: SpansStore = SpansTable(lakehouse, columns, request.namespace)
    source: RawObjectSource = GzipBinaryFiles(raw_fs)
    sink: PeriodSink = IcebergPeriodSink(lakehouse, store.name)
    pipeline: ExtractionPipeline = RayDataExtractionPipeline(source, sink)
    tracker: ExtractionTracker = _tracker(request, store.name)

    replay = Extract(
        archive=archive,
        decoder=decoder,
        pipeline=pipeline,
        store=store,
        tracker=tracker,
    )
    return replay(request.period)


def main(argv: Sequence[str] | None = None) -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
    )
    report = extract(parse(argv))
    print(json.dumps(asdict(report), default=str))


def _tracker(request: ExtractRequest, store: str) -> ExtractionTracker:
    if request.tracking == "mlflow":
        from otlp_replay.tracking.mlflow.tracker import MlflowExtractionTracker

        return MlflowExtractionTracker(store, tags=request.tags)
    return LoggingExtractionTracker()


def _tag(text: str) -> tuple[str, str]:
    key, sep, value = text.partition("=")
    if not sep or not key:
        raise argparse.ArgumentTypeError(f"expected KEY=VALUE, got {text!r}")
    return key, value
