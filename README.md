# otlp-replay

Replay raw OpenTelemetry trace archives into an Apache Iceberg table.

The OpenTelemetry Collector can write raw traces to object storage with the
awss3 exporter. otlp-replay reads those files back for any period, turns every
span into a row and replaces that period in an Iceberg table. Running it twice
on the same period gives the same table, so you can rebuild the history
whenever you change the columns.

## Install

otlp-replay is not on PyPI yet, it is coming. Until then, install it from
GitHub:

```
pip install "otlp-replay[iceberg,ray] @ git+https://github.com/rridane/otlp-replay"
```

Add the `mlflow` extra to record each run in MLflow.

## Usage

Configure the Iceberg catalog with the usual pyiceberg variables, then replay a
period:

```
export PYICEBERG_CATALOG__LAKEHOUSE__URI=http://polaris:8181/api/catalog

otlp-replay extract \
    --from 2026-10-16T09:00Z --to 2026-10-16T10:00Z \
    --archive "s3://my-bucket/traces?region=eu-west-1" \
    --catalog lakehouse
```

The report is printed as one line of JSON:

```
{"start": "2026-10-16 09:00:00+00:00", "end": "2026-10-16 10:00:00+00:00", "objects": 72, "spans_written": 239745, "snapshot_id": 1582498629019097372, "duration_s": 71.3}
```

Options:

- `--archive` any URI pyarrow understands: `s3://`, `gs://`, `abfs://`, `file://`
- `--catalog` the pyiceberg catalog name, `default` if omitted
- `--namespace` where the `spans` table lives, `otel` if omitted
- `--columns` a YAML file of promoted columns, see below
- `--lateness` minutes of deposits read after the end of the period, 12 by default
- `--tracking` `log` or `mlflow`, with `--tag key=value` for the MLflow run

Spans are selected on their start time. Because the exporter writes a span
after it ends, objects deposited up to `--lateness` minutes after the period
are read too.

## Columns

Every row has the span fields (ids, name, kind, status, start time, duration,
service, scope), the full attribute maps, the events, the links and the raw
object it came from.

Some attributes also get their own column. The package ships defaults for
HTTP, RPC, database, messaging and Kubernetes. Your file only lists what you
add or change:

```yaml
columns:
  tenant: [tenant.id]
  retries: {from: [http.request.resend_count], type: int}
  pod: [resource:k8s.pod.name]
```

The first attribute present wins, which lets you list a current semantic
convention before an older one. `resource:` reads a resource attribute. Types
are `string` (default), `int`, `long`, `double`, `boolean` and `grpc_status`.
Set `defaults: false` to start from an empty list.

When the table already exists, new columns are added and older rows read them
as null. Removing a column or changing its type is refused: replay the archive
into a new table instead.

## Python API

The command line is a thin layer over the same objects:

```python
from datetime import UTC, datetime, timedelta

import ray
from pyarrow import fs
from pyiceberg import catalog

from otlp_replay import Extract, Period, load_columns
from otlp_replay.decoding.otlp.decoder import OtlpTraceDecoder
from otlp_replay.pipeline.ray.pipeline import RayDataExtractionPipeline
from otlp_replay.pipeline.ray.sinks.iceberg import IcebergPeriodSink
from otlp_replay.pipeline.ray.sources.gzip_binary_files import GzipBinaryFiles
from otlp_replay.raw_archive.filesystem.archive import FileSystemRawArchive
from otlp_replay.spans_store.iceberg.spans_table import SpansTable
from otlp_replay.tracking.log.tracker import LoggingExtractionTracker

raw_fs, root = fs.FileSystem.from_uri("s3://my-bucket/traces?region=eu-west-1")
lakehouse = catalog.load_catalog("lakehouse")
ray.init()

columns = load_columns()
store = SpansTable(lakehouse, columns)
extract = Extract(
    archive=FileSystemRawArchive(raw_fs, root),
    decoder=OtlpTraceDecoder(columns),
    pipeline=RayDataExtractionPipeline(
        GzipBinaryFiles(raw_fs), IcebergPeriodSink(lakehouse, store.name)
    ),
    store=store,
    tracker=LoggingExtractionTracker(),
)

start = datetime(2026, 10, 16, 9, tzinfo=UTC)
report = extract(Period(start, start + timedelta(hours=3)))
```

`Extract` only depends on five interfaces. Each one has its own module and
can be replaced by your own class:

- `RawTraceArchive` lists the raw objects of a deposit window.
  Provided: `FileSystemRawArchive`, `InMemoryRawArchive`.
- `TraceDecoder` turns one raw object into rows.
  Provided: `OtlpTraceDecoder`.
- `ExtractionPipeline` decodes the objects and replaces the period.
  Provided: `RayDataExtractionPipeline`, `InMemoryExtractionPipeline`.
- `SpansStore` creates the destination and finds the version written for a period.
  Provided: `SpansTable` (Iceberg), `InMemorySpansStore`.
- `ExtractionTracker` follows each run.
  Provided: `LoggingExtractionTracker`, `MlflowExtractionTracker`, `InMemoryExtractionTracker`.

The in-memory implementations run the whole use case without a cluster, which
is what the tests do.

## License

Apache 2.0
