"""Abstract ingestion source interface and run result model."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from typing import Any

import structlog
from pydantic import BaseModel, Field

from src.core.context_layer import SourceContext

logger = structlog.get_logger(__name__)


class IngestResult(BaseModel):
    """Summary of a single ingestion run."""

    source_id: str
    records_fetched: int = 0
    bytes_fetched: int = 0
    started_at: datetime
    completed_at: datetime | None = None
    schema_snapshot: dict[str, Any] = Field(default_factory=dict)
    errors: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class BaseSource(ABC):
    """Async context-managed source with timing and aggregate metrics."""

    def __init__(self, source_config: dict[str, Any], context: SourceContext) -> None:
        self._source_config = source_config
        self._context = context
        self._source_id = str(source_config.get("source_id") or context.source_id)
        self._log = logger.bind(component="ingest", source_id=self._source_id)
        self._started_at: datetime | None = None
        self._completed_at: datetime | None = None
        self._records_fetched: int = 0
        self._bytes_fetched: int = 0
        self._errors: list[str] = []
        self._metadata: dict[str, Any] = {}
        self._schema_snapshot: dict[str, Any] = {}

    def _mark_started(self) -> None:
        if self._started_at is None:
            self._started_at = datetime.now(timezone.utc)
            self._log.info("ingest.run_started", started_at=self._started_at.isoformat())

    def _mark_completed(self) -> None:
        self._completed_at = datetime.now(timezone.utc)
        self._log.info(
            "ingest.run_completed",
            completed_at=self._completed_at.isoformat(),
            records=self._records_fetched,
            bytes=self._bytes_fetched,
        )

    def _add_bytes(self, n: int) -> None:
        self._bytes_fetched += int(n)

    def _add_records(self, n: int) -> None:
        self._records_fetched += int(n)

    def _record_error(self, message: str) -> None:
        self._errors.append(message)
        self._log.warning("ingest.error_recorded", error=message)

    def _set_schema_snapshot(self, snapshot: dict[str, Any]) -> None:
        self._schema_snapshot = dict(snapshot)

    def _merge_metadata(self, extra: dict[str, Any]) -> None:
        self._metadata.update(extra)

    @abstractmethod
    async def connect(self) -> None:
        """Open connections or clients."""

    @abstractmethod
    async def fetch(self) -> AsyncIterator[dict[str, Any]]:
        """Yield individual records (async generator)."""
        if False:  # pragma: no cover
            yield {}

    @abstractmethod
    async def fetch_batch(self) -> list[dict[str, Any]]:
        """Materialize all records for this run."""

    @abstractmethod
    async def close(self) -> None:
        """Release resources."""

    async def __aenter__(self) -> BaseSource:
        await self.connect()
        return self

    async def __aexit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        await self.close()

    def get_result(self) -> IngestResult:
        """Build a snapshot of metrics for the current or last run."""
        started = self._started_at or datetime.now(timezone.utc)
        return IngestResult(
            source_id=self._source_id,
            records_fetched=self._records_fetched,
            bytes_fetched=self._bytes_fetched,
            started_at=started,
            completed_at=self._completed_at,
            schema_snapshot=dict(self._schema_snapshot),
            errors=list(self._errors),
            metadata=dict(self._metadata),
        )
