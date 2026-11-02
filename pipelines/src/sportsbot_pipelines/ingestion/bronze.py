"""Bronze writers: append-only gzip JSON Lines, one file per (source, run).

Layout (same for HDFS and local):
    <root>/source=<source_id>/ingest_date=<YYYY-MM-DD>/ingest_hour=<HH>/part-<run_id>.jsonl.gz

Files are never overwritten: a run id is unique, and WebHDFS CREATE uses overwrite=false.
Spark reads the partition columns (source, ingest_date, ingest_hour) from the directory names.
"""

from __future__ import annotations

import gzip
import json
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol
from urllib.parse import quote

import httpx

from sportsbot_pipelines.collectors.rss import BronzeRecord


def bronze_relpath(source_id: str, collected_at: datetime, run_id: str) -> str:
    ts = collected_at.astimezone(UTC)
    return (
        f"source={source_id}/ingest_date={ts:%Y-%m-%d}/ingest_hour={ts:%H}/part-{run_id}.jsonl.gz"
    )


def encode(records: Iterable[BronzeRecord]) -> bytes:
    lines = "".join(json.dumps(r.to_dict(), ensure_ascii=False) + "\n" for r in records)
    return gzip.compress(lines.encode("utf-8"), mtime=0)


class BronzeWriter(Protocol):
    def write(self, relpath: str, data: bytes) -> str:
        """Write bytes at root/relpath; return the full path written."""


class LocalBronzeWriter:
    def __init__(self, root: Path) -> None:
        self.root = root

    def write(self, relpath: str, data: bytes) -> str:
        path = self.root / relpath
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "xb") as handle:  # "x": fail instead of overwriting
            handle.write(data)
        return str(path)


class WebHdfsError(Exception):
    pass


class WebHdfsBronzeWriter:
    """Two-step WebHDFS CREATE: the NameNode answers 307 with a DataNode URL, then PUT the data."""

    def __init__(
        self,
        webhdfs_url: str,
        root: str,
        user: str,
        timeout: float = 30.0,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self.base = webhdfs_url.rstrip("/")
        self.root = "/" + root.strip("/")
        self.user = user
        self._client = httpx.Client(timeout=timeout, transport=transport)

    def write(self, relpath: str, data: bytes) -> str:
        path = f"{self.root}/{relpath}"
        url = f"{self.base}/webhdfs/v1{quote(path)}"
        params = {"op": "CREATE", "overwrite": "false", "user.name": self.user}
        step1 = self._client.put(url, params=params, follow_redirects=False)
        if step1.status_code != 307 or "location" not in step1.headers:
            raise WebHdfsError(
                f"CREATE {path}: expected 307, got {step1.status_code}: {step1.text[:200]}"
            )
        step2 = self._client.put(
            step1.headers["location"],
            content=data,
            headers={"Content-Type": "application/octet-stream"},
        )
        if step2.status_code != 201:
            raise WebHdfsError(
                f"write {path}: expected 201, got {step2.status_code}: {step2.text[:200]}"
            )
        return f"hdfs://{path}"

    def close(self) -> None:
        self._client.close()
