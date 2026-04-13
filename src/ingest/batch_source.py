"""Batch file or API downloads with JSON, CSV, and Parquet support."""

from __future__ import annotations

import asyncio
import csv
import json
import tempfile
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any, Callable

import httpx

from src.core.context_layer import SourceContext
from src.core.exceptions import IngestionError, SourceConfigurationError
from src.ingest.base import BaseSource


def _compute_batch_stats(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {"row_count": 0, "column_count": 0, "null_counts": {}}
    columns: set[str] = set()
    for r in rows:
        columns.update(r.keys())
    null_counts = {c: 0 for c in columns}
    for r in rows:
        for c in columns:
            if r.get(c) is None:
                null_counts[c] += 1
    return {
        "row_count": len(rows),
        "column_count": len(columns),
        "null_counts": null_counts,
    }


def _load_json_path(path: Path) -> list[dict[str, Any]]:
    with path.open(encoding="utf-8") as fh:
        data = json.load(fh)
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if isinstance(data, dict):
        for key in ("data", "results", "items", "records"):
            chunk = data.get(key)
            if isinstance(chunk, list):
                return [x for x in chunk if isinstance(x, dict)]
        return [data]
    raise IngestionError(f"Unsupported JSON structure in {path}")


def _load_csv_path(path: Path) -> list[dict[str, Any]]:
    with path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        return [dict(row) for row in reader]


def _load_parquet_path(path: Path) -> list[dict[str, Any]]:
    try:
        import pyarrow.parquet as pq  # type: ignore[import-not-found]
    except ImportError as exc:
        raise IngestionError(
            "Parquet batch ingestion requires the pyarrow package.",
        ) from exc
    table = pq.read_table(path)
    if hasattr(table, "to_pylist"):
        rows = table.to_pylist()
        if rows and not isinstance(rows[0], dict):
            raise IngestionError("Parquet table rows could not be converted to dict records")
        return [r for r in rows if isinstance(r, dict)]
    raise IngestionError("Installed pyarrow version does not support Table.to_pylist()")


class BatchSource(BaseSource):
    """One-shot dataset fetch from HTTP APIs or downloadable files."""

    def __init__(self, source_config: dict[str, Any], context: SourceContext) -> None:
        super().__init__(source_config, context)
        conn = source_config.get("connection") or {}
        self._base_url = str(conn.get("base_url", "")).rstrip("/")
        self._timeout = float(conn.get("timeout_seconds", 300))
        fmt = source_config.get("file_format") or source_config.get("format") or "json"
        self._file_format = str(fmt).lower()
        self._client: httpx.AsyncClient | None = None
        self._records: list[dict[str, Any]] | None = None
        self._temp_paths: list[Path] = []
        self._batch_stats: dict[str, Any] = {}

    async def connect(self) -> None:
        self._mark_started()
        self._client = httpx.AsyncClient(timeout=self._timeout)
        self._merge_metadata({"connector": "batch", "file_format": self._file_format})

    def _file_url(self) -> str | None:
        batch = self._source_config.get("batch") or {}
        for key in ("file_url", "url", "download_url"):
            v = self._source_config.get(key) or batch.get(key)
            if v:
                return str(v)
        return None

    async def _download_via_async_client(self, url: str, params: dict[str, Any] | None) -> Path:
        if self._client is None:
            raise IngestionError("Batch client not initialized")
        suffix = f".{self._file_format}" if self._file_format in ("json", "csv", "parquet") else ".bin"

        def _create_temp_path() -> Path:
            fh = tempfile.NamedTemporaryFile(
                prefix="hydra_batch_",
                suffix=suffix,
                delete=False,
            )
            fh.close()
            p = Path(fh.name)
            p.chmod(0o600)
            return p

        path = await asyncio.to_thread(_create_temp_path)
        self._temp_paths.append(path)
        nbytes = 0
        async with self._client.stream("GET", url, params=params) as resp:
            resp.raise_for_status()
            with path.open("wb") as fh:
                async for chunk in resp.aiter_bytes():
                    fh.write(chunk)
                    nbytes += len(chunk)
        self._add_bytes(nbytes)
        return path

    def _parse_local_file(self, path: Path) -> list[dict[str, Any]]:
        if self._file_format == "json":
            return _load_json_path(path)
        if self._file_format == "csv":
            return _load_csv_path(path)
        if self._file_format in ("parquet", "pq"):
            return _load_parquet_path(path)
        raise SourceConfigurationError(f"Unsupported file_format for batch: {self._file_format}")

    async def _load_from_file_url(self) -> list[dict[str, Any]]:
        url = self._file_url()
        if not url:
            raise SourceConfigurationError("File batch source requires file_url (or batch.file_url)")
        path = await self._download_via_async_client(url, None)
        return await asyncio.to_thread(self._parse_local_file, path)

    async def _load_from_datasets(self) -> list[dict[str, Any]]:
        if not self._base_url:
            raise SourceConfigurationError("API batch source requires connection.base_url")
        datasets = self._source_config.get("datasets") or []
        if not datasets:
            raise SourceConfigurationError("API batch source requires a non-empty datasets list")
        merged: list[dict[str, Any]] = []
        for ds in datasets:
            if not isinstance(ds, dict):
                raise SourceConfigurationError("Each datasets entry must be a mapping")
            subpath = str(ds.get("path", "")).strip("/")
            url = f"{self._base_url}/{subpath}"
            params: dict[str, Any] = {}
            vars_ = ds.get("default_variables") or ds.get("variables")
            if vars_:
                params["get"] = ",".join(str(v) for v in vars_)
            extra = ds.get("params") or {}
            params.update(extra)
            path = await self._download_via_async_client(url, params or None)
            fmt = str(ds.get("format") or self._file_format).lower()
            loader: Callable[[Path], list[dict[str, Any]]]
            if fmt == "json":
                loader = _load_json_path
            elif fmt == "csv":
                loader = _load_csv_path
            elif fmt in ("parquet", "pq"):
                loader = _load_parquet_path
            else:
                raise SourceConfigurationError(f"Unsupported dataset format: {fmt}")
            chunk = await asyncio.to_thread(loader, path)
            merged.extend(chunk)
        return merged

    async def _ensure_loaded(self) -> list[dict[str, Any]]:
        if self._records is not None:
            return self._records
        try:
            if self._file_url():
                self._records = await self._load_from_file_url()
            else:
                self._records = await self._load_from_datasets()
        except (httpx.HTTPError, OSError, ValueError) as exc:
            msg = f"Batch load failed: {exc}"
            self._record_error(msg)
            raise IngestionError(msg) from exc
        self._batch_stats = _compute_batch_stats(self._records)
        self._set_schema_snapshot({"batch_stats": self._batch_stats})
        self._merge_metadata({"batch_stats": self._batch_stats})
        return self._records

    async def fetch(self) -> AsyncIterator[dict[str, Any]]:
        rows = await self._ensure_loaded()
        for row in rows:
            self._add_records(1)
            yield row

    async def fetch_batch(self) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        async for row in self.fetch():
            out.append(row)
        return out

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None
        for p in self._temp_paths:
            try:
                p.unlink(missing_ok=True)
            except OSError as exc:
                self._log.warning("batch.temp_cleanup_failed", path=str(p), error=str(exc))
        self._temp_paths.clear()
        self._mark_completed()
