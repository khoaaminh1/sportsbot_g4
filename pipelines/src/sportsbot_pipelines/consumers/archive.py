"""Archive consumer: `articles.extracted` -> Bronze full-text files (HDFS in the stack).

This keeps the Big Data layer fed: Spark reads these files for Silver/Gold processing,
near-duplicate detection and the scaling experiments. Messages are buffered and written in
batches (one gzip JSON Lines file per batch), under source=extracted_<source_id>/...

Delivery note: Kafka offsets are committed per message, so messages buffered when the process
is killed (not stopped cleanly) are lost from the archive. They are still in PostgreSQL and
Qdrant, and `batch_size` keeps the window small. A clean stop (SIGTERM) flushes the buffer.
"""

from __future__ import annotations

import gzip
import json
import secrets
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from sportsbot_pipelines.ingestion.bronze import BronzeWriter, bronze_relpath


class ArchiveBuffer:
    def __init__(
        self,
        writer: BronzeWriter,
        *,
        batch_size: int = 100,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self.writer = writer
        self.batch_size = batch_size
        self.now = now
        self._buffer: list[dict[str, Any]] = []
        self.files_written: list[str] = []

    def handle(self, message: dict[str, Any]) -> None:
        self._buffer.append(message)
        if len(self._buffer) >= self.batch_size:
            self.flush()

    def flush(self) -> None:
        if not self._buffer:
            return
        ts = self.now()
        run_id = f"{ts:%Y%m%dT%H%M%SZ}-{secrets.token_hex(3)}"
        lines = "".join(json.dumps(m, ensure_ascii=False) + "\n" for m in self._buffer)
        data = gzip.compress(lines.encode("utf-8"), mtime=0)
        self.files_written.append(self.writer.write(bronze_relpath("extracted", ts, run_id), data))
        self._buffer.clear()
