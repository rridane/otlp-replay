"""Objets OTLP bruts de l'exportateur awss3 : <racine>/year=…/hour=HH/minute=MM/*.binpb.gz

Tout FileSystem pyarrow convient : S3, GCS, Azure, disque local.
"""

import re
from datetime import UTC, datetime, timedelta

from pyarrow import fs

from otlp_replay.raw_archive.raw_trace_archive import RawTraceArchive

MINUTE = re.compile(
    r"year=(\d{4})/month=(\d{2})/day=(\d{2})/hour=(\d{2})/minute=(\d{2})/"
)


class FileSystemRawArchive(RawTraceArchive):
    def __init__(self, filesystem: fs.FileSystem, root: str) -> None:
        self._filesystem = filesystem
        self._root = root.rstrip("/")

    def objects(self, start: datetime, end: datetime) -> list[str]:
        paths = []
        hour = start.replace(minute=0, second=0, microsecond=0)
        while hour < end:
            for info in self._filesystem.get_file_info(
                fs.FileSelector(
                    self._hour_dir(hour), recursive=True, allow_not_found=True
                )
            ):
                if info.type == fs.FileType.File and start <= _minute(info.path) < end:
                    paths.append(info.path)
            hour += timedelta(hours=1)
        return sorted(paths)

    def _hour_dir(self, hour: datetime) -> str:
        return f"{self._root}/{hour:year=%Y/month=%m/day=%d/hour=%H}"


def _minute(path: str) -> datetime:
    match = MINUTE.search(path)
    if match is None:
        raise ValueError(f"path outside the deposit layout: {path}")
    return datetime(*map(int, match.groups()), tzinfo=UTC)
