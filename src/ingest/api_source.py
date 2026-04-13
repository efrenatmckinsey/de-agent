"""REST API ingestion with pagination, auth, rate limiting, and retries."""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import AsyncIterator
from typing import Any

import httpx

from src.core.context_layer import SourceContext
from src.core.exceptions import IngestionError, SourceConfigurationError
from src.ingest.base import BaseSource


def _records_from_body(body: Any, source_config: dict[str, Any]) -> list[dict[str, Any]]:
    if isinstance(body, list):
        return [x for x in body if isinstance(x, dict)]
    if isinstance(body, dict):
        for key in ("data", "results", "items", "foods", "records", "rows"):
            chunk = body.get(key)
            if isinstance(chunk, list):
                return [x for x in chunk if isinstance(x, dict)]
        return [body]
    return []


def _get_by_path(obj: Any, path: str | None) -> Any:
    if not path or obj is None:
        return obj
    cur: Any = obj
    for part in path.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
    return cur


def _merge_pagination(
    source_config: dict[str, Any],
    context: SourceContext,
) -> dict[str, Any]:
    disc = source_config.get("discovery") or {}
    cfg = dict(disc.get("pagination") or {})
    pi = context.pagination_info
    if pi.style and "type" not in cfg:
        cfg.setdefault("type", pi.style)
    if pi.page_size is not None:
        cfg.setdefault("page_size", pi.page_size)
    if pi.next_url:
        cfg.setdefault("next_url_hint", pi.next_url)
    if pi.total_count is not None:
        cfg.setdefault("total_count_hint", pi.total_count)
    if pi.raw:
        cfg.setdefault("context_raw", pi.raw)
    return cfg


class _RateLimiter:
    """Serialize requests with a semaphore and sleep to cap requests per second."""

    def __init__(self, requests_per_second: float) -> None:
        self._rps = float(requests_per_second)
        self._sem = asyncio.Semaphore(1)
        self._min_interval = 1.0 / self._rps if self._rps > 0 else 0.0
        self._next_slot = 0.0

    async def acquire(self) -> None:
        if self._min_interval <= 0:
            return
        async with self._sem:
            now = time.monotonic()
            if now < self._next_slot:
                await asyncio.sleep(self._next_slot - now)
            self._next_slot = time.monotonic() + self._min_interval


class APISource(BaseSource):
    """Paginated REST ingestion using httpx."""

    def __init__(self, source_config: dict[str, Any], context: SourceContext) -> None:
        super().__init__(source_config, context)
        self._client: httpx.AsyncClient | None = None
        self._request_count = 0
        conn = source_config.get("connection") or {}
        self._base_url = str(
            source_config.get("base_url") or conn.get("base_url") or "",
        ).rstrip("/")
        self._timeout = float(conn.get("timeout_seconds", 60))
        self._rate_cfg = source_config.get("rate_limit") or {}
        rps = float(self._rate_cfg.get("requests_per_second") or 0)
        self._rate_limiter = _RateLimiter(rps)
        self._max_retries = 3

    def _build_headers_params(
        self,
        endpoint: dict[str, Any],
        extra_params: dict[str, Any] | None = None,
    ) -> tuple[dict[str, str], dict[str, Any]]:
        headers: dict[str, str] = {
            str(k): str(v) for k, v in (self._source_config.get("headers") or {}).items()
        }
        conn = self._source_config.get("connection") or {}
        headers.update({str(k): str(v) for k, v in (conn.get("headers") or {}).items()})
        params: dict[str, Any] = dict(endpoint.get("params") or {})
        if extra_params:
            params.update(extra_params)

        auth = self._source_config.get("auth") or {}
        auth_type = str(auth.get("type", "")).lower()
        if auth_type == "bearer":
            token = auth.get("token") or auth.get("access_token")
            if not token:
                raise SourceConfigurationError("auth.bearer requires token or access_token")
            headers["Authorization"] = f"Bearer {token}"
        elif auth_type in ("api_key", "apikey"):
            key = auth.get("api_key") or auth.get("key") or auth.get("value")
            if key is None:
                raise SourceConfigurationError("api_key auth requires api_key/key/value")
            loc = str(auth.get("location", "header")).lower()
            if loc == "query":
                pname = str(auth.get("param", "api_key"))
                params[pname] = key
            else:
                hname = str(auth.get("header", "X-API-Key"))
                headers[hname] = str(key)
        return headers, params

    async def connect(self) -> None:
        self._mark_started()
        if not self._base_url:
            raise SourceConfigurationError("base_url (or connection.base_url) is required for APISource")
        self._client = httpx.AsyncClient(timeout=self._timeout)
        self._merge_metadata({"connector": "api", "base_url": self._base_url})

    async def _request_with_retries(
        self,
        method: str,
        url: str,
        *,
        headers: dict[str, str],
        params: dict[str, Any] | None = None,
    ) -> httpx.Response:
        if self._client is None:
            raise IngestionError("Client not connected; call connect() first.")
        last_exc: Exception | None = None
        for attempt in range(1, self._max_retries + 1):
            await self._rate_limiter.acquire()
            try:
                resp = await self._client.request(method, url, headers=headers, params=params)
                self._request_count += 1
                self._add_bytes(len(resp.content) if resp.content else 0)
                if resp.status_code == 429 or resp.status_code >= 500:
                    if attempt >= self._max_retries:
                        resp.raise_for_status()
                    delay = min(2 ** (attempt - 1), 30.0)
                    if resp.status_code == 429:
                        ra = resp.headers.get("Retry-After")
                        if ra and ra.isdigit():
                            delay = max(delay, float(ra))
                    self._log.warning(
                        "api.retry",
                        url=url,
                        status=resp.status_code,
                        attempt=attempt,
                        sleep_s=delay,
                    )
                    await asyncio.sleep(delay)
                    continue
                resp.raise_for_status()
                return resp
            except httpx.HTTPError as exc:
                last_exc = exc
                if attempt >= self._max_retries:
                    msg = f"HTTP error after {self._max_retries} retries: {url}: {exc}"
                    self._record_error(msg)
                    raise IngestionError(msg) from exc
                delay = min(2 ** (attempt - 1), 30.0)
                self._log.warning("api.transport_retry", url=url, attempt=attempt, error=str(exc))
                await asyncio.sleep(delay)
        raise IngestionError(f"HTTP request failed: {url}") from last_exc

    async def _fetch_endpoint_paginated(
        self,
        endpoint: dict[str, Any],
    ) -> AsyncIterator[dict[str, Any]]:
        path = str(endpoint.get("path", "/")).lstrip("/")
        url = f"{self._base_url}/{path}" if path else self._base_url
        headers, base_params = self._build_headers_params(endpoint)
        pag = _merge_pagination(self._source_config, self._context)
        ptype = str(pag.get("type") or "offset").lower()

        if ptype in ("cursor", "cursor_based", "next_cursor"):
            cursor_param = str(pag.get("cursor_param", "cursor"))
            cursor_field = str(pag.get("cursor_response_field", "next_cursor"))
            next_cursor_val: str | None = None
            if pag.get("initial_cursor") is not None:
                next_cursor_val = str(pag.get("initial_cursor"))
            while True:
                params = dict(base_params)
                if next_cursor_val is not None:
                    params[cursor_param] = next_cursor_val
                resp = await self._request_with_retries("GET", url, headers=headers, params=params)
                try:
                    body = resp.json()
                except json.JSONDecodeError as exc:
                    msg = f"Invalid JSON from {url}"
                    self._record_error(msg)
                    raise IngestionError(msg) from exc
                records = _records_from_body(body, self._source_config)
                for row in records:
                    yield row
                raw_next: Any = None
                if isinstance(body, dict):
                    raw_next = _get_by_path(body, cursor_field)
                if raw_next is None or raw_next == "":
                    break
                next_cursor_val = str(raw_next)
                if not records:
                    break

        elif ptype in ("offset", "page", "pagenumber", "page_number"):
            page_param = str(pag.get("page_param", pag.get("page_number_param", "pageNumber")))
            size_param = str(pag.get("size_param", pag.get("page_size_param", "pageSize")))
            page_size = int(pag.get("page_size") or pag.get("limit") or 100)
            start_page = int(pag.get("start_page", pag.get("initial_page", 1)))
            page = start_page
            while True:
                params = dict(base_params)
                params[page_param] = page
                params[size_param] = page_size
                resp = await self._request_with_retries("GET", url, headers=headers, params=params)
                try:
                    body = resp.json()
                except json.JSONDecodeError as exc:
                    msg = f"Invalid JSON from {url}"
                    self._record_error(msg)
                    raise IngestionError(msg) from exc
                records = _records_from_body(body, self._source_config)
                if not records:
                    break
                for row in records:
                    yield row
                if len(records) < page_size:
                    break
                page += 1
        else:
            resp = await self._request_with_retries("GET", url, headers=headers, params=base_params)
            try:
                body = resp.json()
            except json.JSONDecodeError as exc:
                msg = f"Invalid JSON from {url}"
                self._record_error(msg)
                raise IngestionError(msg) from exc
            for row in _records_from_body(body, self._source_config):
                yield row

    async def fetch(self) -> AsyncIterator[dict[str, Any]]:
        endpoints = self._source_config.get("endpoints") or []
        if not endpoints:
            raise SourceConfigurationError("APISource requires at least one entry in endpoints")
        for ep in endpoints:
            async for row in self._fetch_endpoint_paginated(ep if isinstance(ep, dict) else {"path": ep}):
                self._add_records(1)
                yield row
        self._merge_metadata({"request_count": self._request_count})

    async def fetch_batch(self) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        async for row in self.fetch():
            out.append(row)
        return out

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None
        self._merge_metadata({"request_count": self._request_count, "total_bytes": self._bytes_fetched})
        self._mark_completed()
