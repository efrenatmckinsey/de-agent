"""End-to-end orchestration: commands, discovery, ingest, transform, lineage, quality, metrics."""

from __future__ import annotations

import asyncio
import json
import time
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import structlog
import yaml
from pydantic import BaseModel, Field

from src.core.autoscaler import (
    AutoScaler,
    ComponentMetrics,
    IdleTracker,
    LocalAutoScaler,
    create_autoscaler,
)
from src.core.context_layer import ContextLayer, SourceContext
from src.core.exceptions import (
    ConfigurationError,
    ContextDiscoveryError,
    OrchestratorError,
    SourceConfigurationError,
)
from src.core.topic_router import IngestionAction, IngestionCommand, TopicRouter, create_router

logger = structlog.get_logger(__name__)


class LineageEvent(BaseModel):
    """Minimal OpenLineage-style record emitted after major steps."""

    event_type: str
    source_id: str
    job_name: str
    inputs: list[str] = Field(default_factory=list)
    outputs: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class TransformConfig(BaseModel):
    """Subset of transform YAML used for engine selection."""

    transform_id: str
    source: str
    engine_hint: str = "auto"
    volume_threshold: int = 100_000
    raw: dict[str, Any] = Field(default_factory=dict)


class StorageWriter(ABC):
    """Persists raw ingestion output (S3, local, Iceberg — implementation-specific)."""

    @abstractmethod
    async def write_raw(self, *, source_id: str, records: list[dict[str, Any]], context: SourceContext) -> str:
        """Return a URI or path identifying stored artifacts."""


class LocalJsonlStorageWriter(StorageWriter):
    """Development writer: append JSON lines under ``.hydra/raw``."""

    def __init__(self, base_dir: Path | None = None) -> None:
        self._base = base_dir or Path(".hydra/raw")

    async def write_raw(self, *, source_id: str, records: list[dict[str, Any]], context: SourceContext) -> str:
        self._base.mkdir(parents=True, exist_ok=True)
        path = self._base / f"{source_id}_{int(time.time())}.jsonl"

        def _write() -> None:
            with path.open("w", encoding="utf-8") as fh:
                for row in records:
                    fh.write(json.dumps(row, default=str) + "\n")

        await asyncio.to_thread(_write)
        logger.info("storage.raw_written", path=str(path), rows=len(records), source_id=source_id)
        return str(path.resolve())


class BaseIngestor(ABC):
    """Loads data from a configured source profile."""

    @abstractmethod
    async def ingest(self, *, source_config: dict[str, Any], context: SourceContext) -> list[dict[str, Any]]:
        """Return tabular or semi-structured records as dict rows."""


class APIIngestor(BaseIngestor):
    """REST-style pull using connection + endpoints (sample slice)."""

    async def ingest(self, *, source_config: dict[str, Any], context: SourceContext) -> list[dict[str, Any]]:
        import httpx

        conn = source_config.get("connection") or {}
        base = str(conn.get("base_url", "")).rstrip("/")
        endpoints = source_config.get("endpoints") or []
        if not base or not endpoints:
            logger.warning("api_ingest.skipped", reason="missing_base_or_endpoints")
            return []
        ep = endpoints[0]
        path = str(ep.get("path", "/"))
        url = f"{base}/{path.lstrip('/')}"
        timeout = float(conn.get("timeout_seconds", 60))
        logger.info("api_ingest.fetch", url=url)
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            body = resp.json()
        if isinstance(body, list):
            rows = [x for x in body if isinstance(x, dict)]
        elif isinstance(body, dict):
            rows = []
            for key in ("data", "results", "items", "foods"):
                chunk = body.get(key)
                if isinstance(chunk, list):
                    rows = [x for x in chunk if isinstance(x, dict)]
                    break
            if not rows:
                rows = [body]
        else:
            rows = []
        logger.info("api_ingest.done", rows=len(rows), source_id=source_config.get("source_id"))
        return rows


class StreamIngestor(BaseIngestor):
    """Poll-based stand-in when a live SSE client is not configured."""

    async def ingest(self, *, source_config: dict[str, Any], context: SourceContext) -> list[dict[str, Any]]:
        sim = source_config.get("simulated_stream") or {}
        if not sim.get("enabled", False):
            logger.info("stream_ingest.placeholder", source_id=source_config.get("source_id"))
            return [{"event": "heartbeat", "source_id": source_config.get("source_id")}]
        import httpx

        conn = source_config.get("connection") or {}
        base = str(conn.get("base_url", "")).rstrip("/")
        stations = sim.get("stations") or ["KNYC"]
        station_id = str(stations[0])
        path = f"/stations/{station_id}/observations/latest"
        url = f"{base}{path}"
        logger.info("stream_ingest.poll", url=url)
        async with httpx.AsyncClient(timeout=float(conn.get("timeout_seconds", 60))) as client:
            resp = await client.get(url, headers=dict(conn.get("headers") or {}))
            if resp.status_code >= 400:
                logger.warning("stream_ingest.http_error", status=resp.status_code)
                return []
            body = resp.json()
        return [body] if isinstance(body, dict) else []


class BatchIngestor(BaseIngestor):
    """Batch Census-style pulls over dataset paths declared in YAML."""

    async def ingest(self, *, source_config: dict[str, Any], context: SourceContext) -> list[dict[str, Any]]:
        import httpx

        conn = source_config.get("connection") or {}
        base = str(conn.get("base_url", "")).rstrip("/")
        datasets = source_config.get("datasets") or []
        if not base or not datasets:
            logger.warning("batch_ingest.skipped", reason="missing_base_or_datasets")
            return []
        ds = datasets[0]
        subpath = str(ds.get("path", "")).strip("/")
        url = f"{base}/{subpath}"
        params: dict[str, Any] = {}
        vars_ = ds.get("default_variables")
        if vars_:
            params["get"] = ",".join(str(v) for v in vars_)
        logger.info("batch_ingest.fetch", url=url)
        async with httpx.AsyncClient(timeout=float(conn.get("timeout_seconds", 120))) as client:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            body = resp.json()
        if isinstance(body, list):
            return [{"row": i, "value": v} for i, v in enumerate(body[:500])]
        if isinstance(body, dict):
            return [body]
        return []


def select_ingestor(source_type: str) -> BaseIngestor:
    st = str(source_type).lower()
    if st == "api":
        return APIIngestor()
    if st == "stream":
        return StreamIngestor()
    if st == "batch":
        return BatchIngestor()
    raise SourceConfigurationError(f"Unsupported source type: {source_type!r}")


class TransformRunner:
    """Selects Spark vs Python from ``engine_hint`` and estimated volume."""

    def __init__(self, platform_config: dict[str, Any]) -> None:
        self._platform_config = platform_config

    def choose_engine(self, spec: TransformConfig, estimated_rows: int) -> str:
        hint = str(spec.engine_hint).lower()
        if hint == "spark":
            return "spark"
        if hint == "python":
            return "python"
        threshold = int(spec.volume_threshold)
        return "spark" if estimated_rows >= threshold else "python"

    async def run(self, *, engine: str, spec: TransformConfig, raw_uri: str) -> str:
        logger.info("transform.run", engine=engine, transform_id=spec.transform_id, raw_uri=raw_uri)
        if engine == "spark":
            await asyncio.sleep(0)
            return f"spark://{spec.transform_id}"
        await asyncio.sleep(0)
        return f"python://{spec.transform_id}"


class LineageRecorder:
    """Emits structured lineage events (sink integration can replace logging)."""

    def __init__(self) -> None:
        self._events: list[LineageEvent] = []

    async def record(self, event: LineageEvent) -> None:
        self._events.append(event)
        logger.info(
            "lineage.event",
            event_type=event.event_type,
            source_id=event.source_id,
            job=event.job_name,
            inputs=event.inputs,
            outputs=event.outputs,
        )


class QualityChecker:
    """Executes declarative checks from source YAML (minimal evaluator)."""

    async def run(self, *, source_id: str, records: list[dict[str, Any]], checks: list[dict[str, Any]]) -> None:
        failures: list[str] = []
        for chk in checks:
            ctype = str(chk.get("type", "")).lower()
            if ctype == "non_null":
                fields = chk.get("fields") or []
                for row in records[:1000]:
                    for f in fields:
                        if row.get(f) is None:
                            failures.append(f"non_null:{f}")
            elif ctype == "valid_range":
                field = chk.get("field")
                mn = chk.get("min")
                mx = chk.get("max")
                if not field:
                    continue
                for row in records[:1000]:
                    val = row.get(field)
                    if val is None:
                        continue
                    try:
                        num = float(val)
                    except (TypeError, ValueError):
                        continue
                    if mn is not None and num < float(mn):
                        failures.append(f"range_low:{field}")
                    if mx is not None and num > float(mx):
                        failures.append(f"range_high:{field}")
        if failures:
            logger.warning("quality.check_warnings", source_id=source_id, count=len(failures))
        else:
            logger.info("quality.checks_passed", source_id=source_id, checks=len(checks))


class MetricsEmitter:
    """Hook for Prometheus / OTLP — structured log for now."""

    async def emit(self, **kwargs: Any) -> None:
        logger.info("metrics.emit", **kwargs)


class Orchestrator:
    """Loads configs, routes commands, and runs the ingestion DAG."""

    def __init__(self, platform_config_path: str | Path) -> None:
        self._platform_path = Path(platform_config_path).resolve()
        if not self._platform_path.is_file():
            raise ConfigurationError(f"Platform config not found: {self._platform_path}")
        self._config_dir = self._platform_path.parent
        with self._platform_path.open(encoding="utf-8") as fh:
            loaded = yaml.safe_load(fh)
        if not isinstance(loaded, dict):
            raise ConfigurationError("Platform config must be a YAML mapping.")
        self._config: dict[str, Any] = loaded

        self._sources: dict[str, dict[str, Any]] = {}
        self._transforms: list[TransformConfig] = []
        self._router: TopicRouter = create_router(self._config)
        self._context_layer = ContextLayer(self._config)
        self._autoscaler: AutoScaler = create_autoscaler(self._config)
        self._idle_tracker = IdleTracker()
        self._storage: StorageWriter = LocalJsonlStorageWriter()
        self._transform_runner = TransformRunner(self._config)
        self._lineage = LineageRecorder()
        self._quality = QualityChecker()
        self._metrics = MetricsEmitter()
        self._stop = asyncio.Event()
        self._scaling_interval = float((self._config.get("autoscaler") or {}).get("cooldown_seconds", 120))
        self._component_id = "hydra-orchestrator"
        self._queue_depth = 0
        self._tasks: list[asyncio.Task[Any]] = []

    def load_source_configs(self) -> None:
        """Populate ``self._sources`` from ``config/sources/*.yaml``."""
        pattern = self._config_dir / "sources" / "*.yaml"
        paths = sorted(self._config_dir.glob("sources/*.yaml"))
        if not paths:
            logger.warning("orchestrator.sources_empty", glob=str(pattern))
        for path in paths:
            with path.open(encoding="utf-8") as fh:
                data = yaml.safe_load(fh)
            if not isinstance(data, dict):
                raise SourceConfigurationError(f"Invalid source file: {path}")
            sid = str(data.get("source_id") or path.stem)
            self._sources[sid] = data
            logger.debug("source.loaded", source_id=sid, path=str(path))
        logger.info("sources.loaded", count=len(self._sources))

    def load_transform_configs(self) -> None:
        """Load transform definitions from ``config/transforms/*.yaml``."""
        paths = sorted(self._config_dir.glob("transforms/*.yaml"))
        self._transforms.clear()
        for path in paths:
            with path.open(encoding="utf-8") as fh:
                data = yaml.safe_load(fh)
            if not isinstance(data, dict):
                raise ConfigurationError(f"Invalid transform file: {path}")
            spec = TransformConfig(
                transform_id=str(data.get("transform_id", path.stem)),
                source=str(data.get("source", "")),
                engine_hint=str(data.get("engine_hint", "auto")),
                volume_threshold=int(data.get("volume_threshold", 100_000)),
                raw=data,
            )
            self._transforms.append(spec)
        logger.info("transforms.loaded", count=len(self._transforms))

    def _get_source_config(self, source_id: str) -> dict[str, Any]:
        cfg = self._sources.get(source_id)
        if cfg is None:
            raise SourceConfigurationError(f"Unknown source_id: {source_id!r}")
        return cfg

    def _transforms_for_source(self, source_id: str) -> list[TransformConfig]:
        return [t for t in self._transforms if t.source == source_id]

    async def _discover_context(self, source_id: str, source_config: dict[str, Any]) -> SourceContext:
        stype = str(source_config.get("type", "api")).lower()
        if stype in {"batch", "stream", "api"} and (source_config.get("connection") or {}).get("base_url"):
            try:
                return await self._context_layer.get_or_discover(source_id, source_config)
            except ContextDiscoveryError as exc:
                logger.warning("context.discovery_failed", source_id=source_id, error=str(exc))
        now = datetime.now(timezone.utc)
        return SourceContext(
            source_id=source_id,
            inferred_schema={},
            estimated_rows=0,
            content_type=None,
            last_modified=None,
            etag=None,
            discovered_at=now,
        )

    async def _process_command(self, cmd: IngestionCommand) -> None:
        await self._idle_tracker.touch(self._component_id)
        self._queue_depth = max(0, self._queue_depth - 1)
        source_id = cmd.source_id
        logger.info("orchestrator.command", source_id=source_id, action=cmd.action.value, priority=cmd.priority)
        if cmd.action in (IngestionAction.INGEST, IngestionAction.BACKFILL):
            if cmd.action == IngestionAction.BACKFILL:
                logger.info("orchestrator.backfill", source_id=source_id, payload=cmd.payload)
            source_config = self._get_source_config(source_id)
            context = await self._discover_context(source_id, source_config)
            ingestor = select_ingestor(str(source_config.get("type", "api")))
            records = await ingestor.ingest(source_config=source_config, context=context)
            raw_uri = await self._storage.write_raw(source_id=source_id, records=records, context=context)
            await self._lineage.record(
                LineageEvent(
                    event_type="INGEST",
                    source_id=source_id,
                    job_name=f"ingest:{source_id}",
                    inputs=[str(context.schema.get("inferred_from", ""))],
                    outputs=[raw_uri],
                    metadata={"rows": len(records), "action": cmd.action.value},
                )
            )
            for spec in self._transforms_for_source(source_id):
                engine = self._transform_runner.choose_engine(spec, context.estimated_rows)
                await self._transform_runner.run(engine=engine, spec=spec, raw_uri=raw_uri)
                await self._lineage.record(
                    LineageEvent(
                        event_type="TRANSFORM",
                        source_id=source_id,
                        job_name=spec.transform_id,
                        inputs=[raw_uri],
                        outputs=[f"{engine}://{spec.transform_id}"],
                    )
                )
            checks = list(source_config.get("quality_checks") or [])
            await self._quality.run(source_id=source_id, records=records, checks=checks)
            await self._metrics.emit(
                stage="ingest_complete",
                source_id=source_id,
                rows=len(records),
                estimated_rows=context.estimated_rows,
            )
            return

        if cmd.action == IngestionAction.TRANSFORM:
            source_config = self._get_source_config(source_id)
            context = await self._discover_context(source_id, source_config)
            raw_uri = str(cmd.payload.get("raw_uri", ""))
            for spec in self._transforms_for_source(source_id):
                engine = self._transform_runner.choose_engine(spec, context.estimated_rows)
                out = await self._transform_runner.run(engine=engine, spec=spec, raw_uri=raw_uri or "memory://")
                await self._lineage.record(
                    LineageEvent(
                        event_type="TRANSFORM",
                        source_id=source_id,
                        job_name=spec.transform_id,
                        inputs=[raw_uri or "memory://"],
                        outputs=[out],
                    )
                )
            await self._metrics.emit(stage="transform_only", source_id=source_id)
            return

        logger.warning("orchestrator.unhandled_action", action=cmd.action.value, source_id=source_id)

    async def _scaling_loop(self) -> None:
        acfg = self._config.get("autoscaler") or {}
        idle_mins = float(acfg.get("idle_shutdown_minutes") or 0)
        while not self._stop.is_set():
            try:
                last = await self._idle_tracker.last_active(self._component_id)
                last_ts = last or datetime.now(timezone.utc)
                if isinstance(self._autoscaler, LocalAutoScaler):
                    metrics = LocalAutoScaler.host_metrics(
                        self._component_id,
                        self._queue_depth,
                        last_ts,
                    )
                else:
                    metrics = ComponentMetrics(
                        component_id=self._component_id,
                        queue_depth=self._queue_depth,
                        last_active=last_ts,
                    )
                idle_decision = await self._idle_tracker.recommend(self._component_id, idle_mins)
                decision = idle_decision or self._autoscaler.evaluate(metrics, self._config)
                await self._autoscaler.apply_decision(decision, component_id=self._component_id)
            except Exception as exc:
                logger.warning("autoscaler.loop_error", error=str(exc))
            try:
                await asyncio.sleep(self._scaling_interval)
            except asyncio.CancelledError:
                break

    async def run(self) -> None:
        """Start consumers, process commands, and periodically evaluate scaling."""
        self._stop.clear()
        self.load_source_configs()
        self.load_transform_configs()
        await self._router.start()
        self._tasks.append(asyncio.create_task(self._scaling_loop(), name="scaling-loop"))
        logger.info("orchestrator.run_started")
        try:
            async for cmd in self._router.consume():
                if self._stop.is_set():
                    break
                self._queue_depth += 1
                try:
                    await self._process_command(cmd)
                except (SourceConfigurationError, OrchestratorError) as exc:
                    logger.error("orchestrator.command_failed", error=str(exc), source_id=cmd.source_id)
                except Exception as exc:
                    logger.exception("orchestrator.unexpected_error", source_id=cmd.source_id, error=str(exc))
        finally:
            await self.stop()

    async def stop(self) -> None:
        """Graceful shutdown: cancel background tasks and close routers."""
        if self._stop.is_set():
            return
        self._stop.set()
        for task in self._tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._tasks.clear()
        await self._router.stop()
        logger.info("orchestrator.stopped")
