"""Streaming and SSE-style ingestion with optional simulated polling."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from typing import Any

import httpx

from src.core.context_layer import SourceContext
from src.core.exceptions import IngestionError, SourceConfigurationError
from src.ingest.base import BaseSource


def _parse_event_time(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return dt.timestamp()
        except ValueError:
            return None
    return None


def _record_hwm_keys(source_config: dict[str, Any]) -> tuple[str | None, str | None]:
    hwm = source_config.get("high_water_mark") or {}
    return (
        hwm.get("id_field") or source_config.get("hwm_id_field"),
        hwm.get("timestamp_field") or source_config.get("hwm_timestamp_field"),
    )


def _sse_event_to_record(block: str) -> dict[str, Any] | None:
    data_parts: list[str] = []
    event_id: str | None = None
    for raw_line in block.split("\n"):
        line = raw_line.rstrip("\r")
        if line.startswith("data:"):
            data_parts.append(line[5:].lstrip())
        elif line.startswith("id:"):
            event_id = line[3:].strip()
    if not data_parts:
        return None
    payload = "\n".join(data_parts)
    if payload == "[DONE]":
        return None
    try:
        parsed = json.loads(payload)
    except json.JSONDecodeError:
        rec: dict[str, Any] = {"raw": payload}
        if event_id:
            rec["_sse_id"] = event_id
        return rec
    if isinstance(parsed, dict):
        if event_id:
            parsed.setdefault("_sse_id", event_id)
        return parsed
    return {"value": parsed, "_sse_id": event_id} if event_id else {"value": parsed}


class StreamSource(BaseSource):
    """SSE reader or simulated rotating poll (e.g. NOAA-style stations)."""

    def __init__(
        self,
        source_config: dict[str, Any],
        context: SourceContext,
        *,
        max_records: int | None = None,
    ) -> None:
        super().__init__(source_config, context)
        conn = source_config.get("connection") or {}
        self._base_url = str(conn.get("base_url", "")).rstrip("/")
        self._timeout = float(conn.get("timeout_seconds", 120))
        stream_cfg = source_config.get("stream") or {}
        self._mode = str(
            stream_cfg.get("mode") or source_config.get("stream_mode") or "simulated",
        ).lower()
        self._sse_url = str(
            stream_cfg.get("sse_url") or source_config.get("sse_url") or "",
        )
        self._reconnect_interval = float(
            stream_cfg.get("reconnect_interval_seconds")
            or source_config.get("reconnect_interval_seconds", 5),
        )
        self._poll_interval = float(
            (source_config.get("simulated_stream") or {}).get("poll_interval_seconds", 30),
        )
        self._sim = source_config.get("simulated_stream") or {}
        stations = self._sim.get("stations") or ["KNYC"]
        self._stations = [str(s) for s in stations]
        self._station_idx = 0
        self._max_records = (
            max_records
            if max_records is not None
            else source_config.get("max_records")
        )
        if self._max_records is not None:
            self._max_records = int(self._max_records)
        self._client: httpx.AsyncClient | None = None
        self._hwm_id: Any = None
        self._hwm_ts: float | None = None
        self._id_field, self._ts_field = _record_hwm_keys(source_config)
        self._yielded = 0

    def _should_emit(self, record: dict[str, Any]) -> bool:
        if self._id_field and self._id_field in record:
            rid = record[self._id_field]
            if self._hwm_id is not None:
                try:
                    if float(rid) <= float(self._hwm_id):  # type: ignore[arg-type]
                        return False
                except (TypeError, ValueError):
                    if str(rid) <= str(self._hwm_id):
                        return False
            self._hwm_id = rid
        if self._ts_field and self._ts_field in record:
            ts = _parse_event_time(record[self._ts_field])
            if ts is not None:
                if self._hwm_ts is not None and ts <= self._hwm_ts:
                    return False
                self._hwm_ts = ts
        return True

    async def connect(self) -> None:
        self._mark_started()
        self._client = httpx.AsyncClient(timeout=self._timeout)
        self._merge_metadata({"connector": "stream", "mode": self._mode})
        if self._mode == "sse" and not self._sse_url:
            raise SourceConfigurationError("stream.mode=sse requires sse_url")

    async def _headers(self) -> dict[str, str]:
        conn = self._source_config.get("connection") or {}
        return {str(k): str(v) for k, v in (conn.get("headers") or {}).items()}

    async def _run_sse(self) -> AsyncIterator[dict[str, Any]]:
        if self._client is None:
            raise IngestionError("Stream client not initialized")
        headers = await self._headers()
        headers.setdefault("Accept", "text/event-stream")
        buffer = ""
        while True:
            try:
                async with self._client.stream("GET", self._sse_url, headers=headers) as resp:
                    resp.raise_for_status()
                    async for chunk in resp.aiter_text():
                        self._add_bytes(len(chunk.encode("utf-8")))
                        buffer += chunk
                        while "\n\n" in buffer:
                            raw_event, buffer = buffer.split("\n\n", 1)
                            rec = _sse_event_to_record(raw_event)
                            if rec is None:
                                continue
                            if self._should_emit(rec):
                                yield rec
            except (httpx.HTTPError, asyncio.CancelledError) as exc:
                if isinstance(exc, asyncio.CancelledError):
                    raise
                self._record_error(f"SSE connection error: {exc}")
                self._log.warning("stream.sse_reconnect", error=str(exc))
                await asyncio.sleep(self._reconnect_interval)

    async def _run_simulated(self) -> AsyncIterator[dict[str, Any]]:
        if self._client is None:
            raise IngestionError("Stream client not initialized")
        if not self._base_url:
            raise SourceConfigurationError("simulated stream requires connection.base_url")
        headers = await self._headers()
        while True:
            try:
                station = self._stations[self._station_idx % len(self._stations)]
                self._station_idx += 1
                path = self._sim.get("observation_path_template", "/stations/{station}/observations/latest")
                path_f = str(path).format(station=station)
                url = f"{self._base_url}/{path_f.lstrip('/')}"
                resp = await self._client.get(url, headers=headers)
                self._add_bytes(len(resp.content) if resp.content else 0)
                if resp.status_code >= 400:
                    self._record_error(f"Poll HTTP {resp.status_code} for {url}")
                    await asyncio.sleep(self._reconnect_interval)
                    continue
                try:
                    body = resp.json()
                except json.JSONDecodeError as exc:
                    self._record_error(f"Invalid JSON from {url}: {exc}")
                    await asyncio.sleep(self._poll_interval)
                    continue
                rec = body if isinstance(body, dict) else {"value": body, "station": station}
                rec.setdefault("station_id", station)
                if self._should_emit(rec):
                    yield rec
                await asyncio.sleep(self._poll_interval)
            except httpx.HTTPError as exc:
                self._record_error(f"Simulated stream transport error: {exc}")
                self._log.warning("stream.poll_reconnect", error=str(exc))
                await asyncio.sleep(self._reconnect_interval)

    async def fetch(self) -> AsyncIterator[dict[str, Any]]:
        if self._mode == "sse":
            gen = self._run_sse()
        else:
            gen = self._run_simulated()
        async for rec in gen:
            if self._max_records is not None and self._yielded >= self._max_records:
                break
            self._add_records(1)
            self._yielded += 1
            yield rec

    async def fetch_batch(self) -> list[dict[str, Any]]:
        if self._max_records is None:
            raise IngestionError(
                "fetch_batch on StreamSource requires max_records to avoid unbounded memory use",
            )
        out: list[dict[str, Any]] = []
        async for row in self.fetch():
            out.append(row)
        return out

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None
        self._merge_metadata({"stream_yielded": self._yielded})
        self._mark_completed()
