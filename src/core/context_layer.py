"""HTTP source discovery, schema reflection, and cached :class:`SourceContext`."""

from __future__ import annotations

import asyncio
import email.utils
import json
import re
from datetime import datetime, timezone
from typing import Any, Optional

import httpx
import structlog
import yaml
from pydantic import BaseModel, ConfigDict, Field

from src.core.exceptions import ContextDiscoveryError, SourceConfigurationError

logger = structlog.get_logger(__name__)


class PaginationInfo(BaseModel):
    """Hints parsed from headers, body, or source YAML."""

    style: Optional[str] = None
    total_count: Optional[int] = None
    page_size: Optional[int] = None
    next_url: Optional[str] = None
    raw: dict[str, Any] = Field(default_factory=dict)


class SourceContext(BaseModel):
    """Cached discovery outcome for a source."""

    model_config = ConfigDict(populate_by_name=True)

    source_id: str
    # Wire JSON key "schema" without clashing with BaseModel schema helpers (Pydantic v2).
    inferred_schema: dict[str, Any] = Field(default_factory=dict, alias="schema")
    estimated_rows: int = 0
    content_type: Optional[str] = None
    last_modified: Optional[datetime] = None
    etag: Optional[str] = None
    discovered_at: datetime
    pagination_info: PaginationInfo = Field(default_factory=PaginationInfo)

    @property
    def schema(self) -> dict[str, Any]:
        """Alias for :attr:`inferred_schema` (matches public ``schema`` in configs and docs)."""
        return self.inferred_schema


def _parse_http_date(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    parsed = email.utils.parsedate_to_datetime(value)
    if parsed is None:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _infer_type(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int) and not isinstance(value, bool):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    return type(value).__name__


def _merge_schema_field(
    columns: dict[str, dict[str, Any]],
    key: str,
    value: Any,
) -> None:
    inferred = _infer_type(value)
    if key not in columns:
        columns[key] = {"types": {inferred}, "nullable": value is None}
        return
    col = columns[key]
    col["types"].add(inferred)
    if value is None:
        col["nullable"] = True


def _records_from_body(body: Any, source_config: dict[str, Any]) -> list[dict[str, Any]]:
    if isinstance(body, list):
        return [x for x in body if isinstance(x, dict)]
    if isinstance(body, dict):
        for key in ("data", "results", "items", "foods", "records"):
            chunk = body.get(key)
            if isinstance(chunk, list):
                return [x for x in chunk if isinstance(x, dict)]
        return [body]
    return []


class ContextStore:
    """In-memory TTL cache of :class:`SourceContext` by ``source_id``."""

    def __init__(self, ttl_seconds: float) -> None:
        self._ttl = ttl_seconds
        self._data: dict[str, tuple[SourceContext, datetime]] = {}
        self._lock = asyncio.Lock()

    async def get(self, source_id: str) -> SourceContext | None:
        async with self._lock:
            entry = self._data.get(source_id)
            if entry is None:
                return None
            ctx, stored_at = entry
            age = (datetime.now(timezone.utc) - stored_at).total_seconds()
            if age > self._ttl:
                del self._data[source_id]
                return None
            return ctx

    async def set(self, context: SourceContext) -> None:
        async with self._lock:
            self._data[context.source_id] = (context, datetime.now(timezone.utc))


class ContextLayer:
    """Probes endpoints, infers schema from samples, estimates volume, checks freshness."""

    def __init__(self, platform_config: dict[str, Any]) -> None:
        cl = platform_config.get("context_layer") or {}
        disc = cl.get("discovery") or {}
        refl = cl.get("reflection") or {}
        self._timeout = float(disc.get("timeout_seconds", 45))
        self._max_retries = int(disc.get("max_retries", 3))
        self._sample_size = int(refl.get("sample_size", 500))
        ttl = float(disc.get("cache_ttl_seconds", 3600))
        self._store = ContextStore(ttl)
        self._log = logger.bind(component="context_layer")

    async def get_or_discover(self, source_id: str, source_config: dict[str, Any]) -> SourceContext:
        cached = await self._store.get(source_id)
        if cached is not None:
            self._log.info("context.cache_hit", source_id=source_id)
            return cached
        ctx = await self._discover(source_id, source_config)
        await self._store.set(ctx)
        return ctx

    async def _discover(self, source_id: str, source_config: dict[str, Any]) -> SourceContext:
        self._log.info("context.discover_start", source_id=source_id)
        probe = await self.endpoint_probe(source_config)
        schema = await self.schema_discovery(source_config)
        volume = await self.volume_estimation(source_config, probe, schema)
        freshness = await self.freshness_check(source_config, probe)
        raw_pagination = probe.get("pagination")
        pagination = (
            raw_pagination
            if isinstance(raw_pagination, PaginationInfo)
            else PaginationInfo.model_validate(raw_pagination or {})
        )
        ctx = SourceContext(
            source_id=source_id,
            inferred_schema=schema,
            estimated_rows=volume,
            content_type=probe.get("content_type"),
            last_modified=freshness.get("last_modified"),
            etag=freshness.get("etag"),
            discovered_at=datetime.now(timezone.utc),
            pagination_info=pagination,
        )
        self._log.info(
            "context.discover_done",
            source_id=source_id,
            estimated_rows=ctx.estimated_rows,
            content_type=ctx.content_type,
        )
        return ctx

    def _build_url(self, source_config: dict[str, Any], path: str | None = None) -> str:
        conn = source_config.get("connection") or {}
        base = conn.get("base_url")
        if not base:
            raise SourceConfigurationError("connection.base_url is required for HTTP discovery.")
        if path:
            return str(base).rstrip("/") + "/" + str(path).lstrip("/")
        endpoints = source_config.get("endpoints") or []
        if not endpoints:
            return str(base).rstrip("/")
        first = endpoints[0]
        ep_path = first.get("path", "/")
        return str(base).rstrip("/") + "/" + str(ep_path).lstrip("/")

    async def _client_request(
        self,
        method: str,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        params: dict[str, Any] | None = None,
        follow_redirects: bool = True,
    ) -> httpx.Response:
        last_exc: Exception | None = None
        for attempt in range(1, self._max_retries + 1):
            try:
                async with httpx.AsyncClient(timeout=self._timeout) as client:
                    self._log.debug("http.request", method=method, url=url, attempt=attempt)
                    resp = await client.request(
                        method,
                        url,
                        headers=headers,
                        params=params,
                        follow_redirects=follow_redirects,
                    )
                    return resp
            except httpx.HTTPError as exc:
                last_exc = exc
                self._log.warning("http.retry", url=url, attempt=attempt, error=str(exc))
                await asyncio.sleep(min(2 ** (attempt - 1), 8))
        raise ContextDiscoveryError(f"HTTP request failed after retries: {url}") from last_exc

    async def endpoint_probe(self, source_config: dict[str, Any]) -> dict[str, Any]:
        url = self._build_url(source_config)
        self._log.info("discovery.endpoint_probe", url=url)
        try:
            head = await self._client_request("HEAD", url)
        except ContextDiscoveryError:
            head = None
        headers: dict[str, str] = {}
        if head is not None:
            headers = {k.lower(): v for k, v in head.headers.items()}
        content_type = headers.get("content-type")
        link = headers.get("link")
        pagination = PaginationInfo(raw={"link": link} if link else {})
        m = re.search(r"rel=\"?next\"?", link or "", re.IGNORECASE)
        if m and link:
            part = link.split(";")[0].strip()
            if part.startswith("<") and part.endswith(">"):
                pagination.next_url = part[1:-1]
                pagination.style = "link_header"
        x_total = headers.get("x-total-count")
        if x_total and x_total.isdigit():
            pagination.total_count = int(x_total)
        content_range = headers.get("content-range")
        if content_range and "/" in content_range:
            total_part = content_range.split("/")[-1]
            if total_part.isdigit():
                pagination.total_count = int(total_part)
        disc = source_config.get("discovery") or {}
        pag_cfg = disc.get("pagination") or {}
        if pag_cfg.get("total_field"):
            pagination.style = pagination.style or str(pag_cfg.get("type") or "configured")

        if head is None or head.status_code >= 400 or not content_type:
            get_resp = await self._client_request("GET", url)
            gh = {k.lower(): v for k, v in get_resp.headers.items()}
            content_type = content_type or gh.get("content-type")
            for hk, hv in gh.items():
                headers.setdefault(hk, hv)

        return {
            "url": url,
            "headers": headers,
            "content_type": content_type,
            "pagination": pagination,
        }

    async def schema_discovery(self, source_config: dict[str, Any]) -> dict[str, Any]:
        url = self._build_url(source_config)
        params: dict[str, Any] = {}
        endpoints = source_config.get("endpoints") or []
        if endpoints:
            ep0 = endpoints[0]
            names = [p if isinstance(p, str) else str(p) for p in (ep0.get("params") or [])]
            by_lower = {n.lower(): n for n in names}
            if "pagesize" in by_lower:
                params[by_lower["pagesize"]] = min(self._sample_size, 500)
            elif "limit" in by_lower:
                params[by_lower["limit"]] = min(self._sample_size, 500)
            if "query" in by_lower:
                params[by_lower["query"]] = "*"
            elif "q" in by_lower:
                params[by_lower["q"]] = "*"
            elif "search" in by_lower:
                params[by_lower["search"]] = "*"
        self._log.info("discovery.schema_discovery", url=url, sample_target=self._sample_size)
        resp = await self._client_request("GET", url, params=params or None)
        try:
            body = resp.json()
        except json.JSONDecodeError as exc:
            raise ContextDiscoveryError(f"Could not parse JSON for schema discovery: {url}") from exc
        records = _records_from_body(body, source_config)
        sample = records[: self._sample_size]
        columns: dict[str, dict[str, Any]] = {}
        for row in sample:
            for k, v in row.items():
                _merge_schema_field(columns, k, v)
        schema_columns: dict[str, Any] = {}
        for name, meta in columns.items():
            types_set = meta.get("types") or set()
            types_list = sorted(str(t) for t in types_set)
            schema_columns[name] = {
                "types": types_list,
                "nullable": bool(meta.get("nullable")),
            }
        result = {
            "columns": schema_columns,
            "sample_size": len(sample),
            "inferred_from": url,
        }
        self._log.info("discovery.schema_inferred", columns=len(schema_columns), sample_size=len(sample))
        return result

    async def volume_estimation(
        self,
        source_config: dict[str, Any],
        probe: dict[str, Any],
        schema: dict[str, Any],
    ) -> int:
        pagination = probe.get("pagination")
        if isinstance(pagination, PaginationInfo) and pagination.total_count is not None:
            self._log.info("volume.from_pagination_header", total=pagination.total_count)
            return int(pagination.total_count)
        if isinstance(pagination, dict):
            tc = pagination.get("total_count")
            if isinstance(tc, int):
                return tc
        sample_size = int(schema.get("sample_size") or 0)
        body_hint: int | None = None
        url = self._build_url(source_config)
        resp = await self._client_request("GET", url)
        try:
            body = resp.json()
        except json.JSONDecodeError:
            body = None
        disc = source_config.get("discovery") or {}
        pag = disc.get("pagination") or {}
        total_field = pag.get("total_field")
        if isinstance(body, dict) and total_field:
            raw = body.get(total_field)
            if isinstance(raw, int):
                body_hint = raw
            elif isinstance(raw, str) and raw.isdigit():
                body_hint = int(raw)
        if body_hint is not None:
            self._log.info("volume.from_body_total_field", total=body_hint)
            return body_hint
        if sample_size > 0:
            extrapolated = sample_size * 50
            self._log.info("volume.extrapolated_from_sample", estimate=extrapolated)
            return extrapolated
        self._log.info("volume.unknown_default", estimate=0)
        return 0

    async def freshness_check(
        self,
        source_config: dict[str, Any],
        probe: dict[str, Any],
    ) -> dict[str, Any]:
        headers = probe.get("headers") or {}
        lm_raw = headers.get("last-modified")
        etag = headers.get("etag")
        last_modified = _parse_http_date(lm_raw) if lm_raw else None
        if last_modified is None and etag is None:
            url = self._build_url(source_config)
            resp = await self._client_request("HEAD", url)
            rh = {k.lower(): v for k, v in resp.headers.items()}
            lm_raw = rh.get("last-modified")
            etag = etag or rh.get("etag")
            last_modified = _parse_http_date(lm_raw) if lm_raw else None
        self._log.info(
            "discovery.freshness",
            has_last_modified=last_modified is not None,
            has_etag=bool(etag),
        )
        return {"last_modified": last_modified, "etag": etag}


def load_source_config(path: str) -> dict[str, Any]:
    """Load a single YAML source file (helper for orchestrator)."""
    with open(path, encoding="utf-8") as fh:
        data = yaml.safe_load(fh)
    if not isinstance(data, dict):
        raise SourceConfigurationError(f"Invalid YAML in {path}")
    return data
