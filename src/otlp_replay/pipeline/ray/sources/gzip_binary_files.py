from collections.abc import Sequence

import ray.data
from pyarrow import fs

from otlp_replay.pipeline.ray.raw_object_source import RawObjectSource


class GzipBinaryFiles(RawObjectSource):
    """Gzip objects from a pyarrow FileSystem, read and decompressed by the workers."""

    def __init__(self, filesystem: fs.FileSystem) -> None:
        self._filesystem = filesystem

    def read(self, objects: Sequence[str]) -> ray.data.Dataset:
        return ray.data.read_binary_files(
            list(objects),
            include_paths=True,
            filesystem=self._filesystem,
            arrow_open_stream_args={"compression": "gzip"},
        )
