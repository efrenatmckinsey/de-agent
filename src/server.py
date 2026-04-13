"""FastAPI backend that powers the live dashboard.

Launch with:
    python -m src.server
    # Dashboard: http://localhost:8550
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import queue
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
import structlog
import yaml
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from pydantic import BaseModel
from sse_starlette.sse import EventSourceResponse

# ---------------------------------------------------------------------------
# Shared log buffer — structlog writes here, SSE reads from it
# ---------------------------------------------------------------------------
LOG_BUFFER: queue.Queue[dict[str, Any]] = queue.Queue(maxsize=5000)


class DashboardLogHandler(logging.Handler):
    """Stdlib handler that pushes formatted records into the SSE buffer."""

    def emit(self, record: logging.LogRecord) -> None:
        try:
            entry = {
                "ts": datetime.now(timezone.utc).isoformat(),
                "level": record.levelname,
                "msg": self.format(record),
            }
            try:
                LOG_BUFFER.put_nowait(entry)
            except queue.Full:
                try:
                    LOG_BUFFER.get_nowait()
                except queue.Empty:
                    pass
                LOG_BUFFER.put_nowait(entry)
        except Exception:
            pass


def _configure_logging() -> None:
    handler = DashboardLogHandler()
    handler.setFormatter(logging.Formatter("%(message)s"))
    root = logging.getLogger()
    root.addHandler(handler)
    root.setLevel(logging.INFO)

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.dev.ConsoleRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
    )


_configure_logging()
log = structlog.get_logger("dashboard")

# ---------------------------------------------------------------------------
# Configuration helpers
# ---------------------------------------------------------------------------
CONFIG_DIR = Path(__file__).resolve().parent.parent / "config"
PLATFORM_PATH = CONFIG_DIR / "platform.yaml"


def _load_platform() -> dict[str, Any]:
    with PLATFORM_PATH.open(encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _load_sources() -> dict[str, dict[str, Any]]:
    sources: dict[str, dict[str, Any]] = {}
    for p in sorted((CONFIG_DIR / "sources").glob("*.yaml")):
        with p.open(encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        sid = data.get("source_id", p.stem)
        data["_file"] = p.name
        sources[sid] = data
    return sources


def _load_transforms() -> dict[str, dict[str, Any]]:
    transforms: dict[str, dict[str, Any]] = {}
    for p in sorted((CONFIG_DIR / "transforms").glob("*.yaml")):
        with p.open(encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        tid = data.get("transform_id", p.stem)
        data["_file"] = p.name
        transforms[tid] = data
    return transforms


# ---------------------------------------------------------------------------
# Pipeline state (in-memory)
# ---------------------------------------------------------------------------
class PipelineState:
    def __init__(self) -> None:
        self.runs: list[dict[str, Any]] = []
        self.lock = asyncio.Lock()

    async def add_run(self, run: dict[str, Any]) -> None:
        async with self.lock:
            self.runs.append(run)
            if len(self.runs) > 200:
                self.runs = self.runs[-200:]

    async def get_runs(self) -> list[dict[str, Any]]:
        async with self.lock:
            return list(self.runs)


STATE = PipelineState()

# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------
app = FastAPI(title="hydra-ingest Dashboard API", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

DASHBOARD_PATH = Path(__file__).resolve().parent.parent / "docs" / "dashboard.html"


@app.get("/", response_class=HTMLResponse)
async def root():
    if DASHBOARD_PATH.is_file():
        return FileResponse(DASHBOARD_PATH, media_type="text/html")
    return HTMLResponse("<h1>hydra-ingest API</h1><p>Dashboard not found.</p>")


# ---- Sources & Transforms ----

@app.get("/api/sources")
async def list_sources():
    return _load_sources()


@app.get("/api/transforms")
async def list_transforms():
    return _load_transforms()


@app.get("/api/platform")
async def get_platform():
    return _load_platform()


# ---- Discovery ----

class DiscoveryResult(BaseModel):
    source_id: str
    content_type: str | None = None
    estimated_rows: int = 0
    columns: dict[str, Any] = {}
    sample_size: int = 0
    last_modified: str | None = None
    etag: str | None = None
    duration_ms: int = 0


@app.post("/api/discover/{source_id}")
async def discover_source(source_id: str):
    sources = _load_sources()
    if source_id not in sources:
        raise HTTPException(404, f"Source {source_id!r} not found")

    source_config = sources[source_id]
    platform_config = _load_platform()
    log.info("discovery.start", source_id=source_id)
    start = time.time()

    try:
        from src.core.context_layer import ContextLayer
        layer = ContextLayer(platform_config)
        ctx = await layer.get_or_discover(source_id, source_config)
        elapsed = int((time.time() - start) * 1000)

        result = DiscoveryResult(
            source_id=source_id,
            content_type=ctx.content_type,
            estimated_rows=ctx.estimated_rows,
            columns=ctx.schema.get("columns", {}),
            sample_size=ctx.schema.get("sample_size", 0),
            last_modified=ctx.last_modified.isoformat() if ctx.last_modified else None,
            etag=ctx.etag,
            duration_ms=elapsed,
        )
        log.info("discovery.complete", source_id=source_id, estimated_rows=result.estimated_rows, duration_ms=elapsed)
        await STATE.add_run({"type": "discovery", "source_id": source_id, "result": result.model_dump(), "ts": _now()})
        return result
    except Exception as exc:
        log.error("discovery.failed", source_id=source_id, error=str(exc))
        raise HTTPException(500, str(exc))


# ---- Ingestion ----

@app.post("/api/ingest/{source_id}")
async def ingest_source(source_id: str):
    sources = _load_sources()
    if source_id not in sources:
        raise HTTPException(404, f"Source {source_id!r} not found")

    source_config = sources[source_id]
    platform_config = _load_platform()
    log.info("ingest.start", source_id=source_id, source_type=source_config.get("type"))
    start = time.time()

    try:
        from src.core.context_layer import ContextLayer
        layer = ContextLayer(platform_config)
        ctx = await layer.get_or_discover(source_id, source_config)

        from src.core.orchestrator import select_ingestor
        ingestor = select_ingestor(source_config.get("type", "api"))
        records = await ingestor.ingest(source_config=source_config, context=ctx)

        elapsed = int((time.time() - start) * 1000)
        sample = records[:5] if records else []

        # Run PII classification
        pii_result = None
        try:
            from src.governance.classifier import DataClassifier
            classifier = DataClassifier()
            gov_config = source_config.get("governance", {})
            pii_result = classifier.classify_records(source_id, records, gov_config)
            pii_out = {
                "classification": pii_result.classification.value,
                "pii_fields": {k: v.value for k, v in pii_result.pii_fields.items()},
                "scanned_rows": pii_result.scanned_rows,
            }
            log.info("classify.complete", source_id=source_id, classification=pii_out["classification"], pii_fields=len(pii_out["pii_fields"]))
        except Exception as exc:
            pii_out = {"error": str(exc)}
            log.warning("classify.failed", error=str(exc))

        # Run quality checks
        quality_out = None
        try:
            from src.governance.quality import QualityEngine
            qe = QualityEngine()
            checks_cfg = source_config.get("quality_checks", [])
            gate_cfg = source_config.get("governance", {}).get("quality_gate", {"fail_on_critical": True, "warn_threshold": 0.95})
            if not gate_cfg:
                gate_cfg = platform_config.get("governance", {}).get("quality_gate", {"fail_on_critical": True, "warn_threshold": 0.95})
            qr = qe.run_checks(source_id, records, checks_cfg, gate_cfg)
            quality_out = {
                "checks_run": qr.checks_run,
                "checks_passed": qr.checks_passed,
                "checks_failed": qr.checks_failed,
                "pass_rate": qr.pass_rate,
                "passed_gate": qr.passed_gate,
                "violations": [v.model_dump() for v in qr.violations[:10]],
            }
            log.info("quality.complete", source_id=source_id, passed=qr.passed_gate, pass_rate=qr.pass_rate)
        except Exception as exc:
            quality_out = {"error": str(exc)}
            log.warning("quality.failed", error=str(exc))

        # Lineage
        lineage_out = None
        try:
            from src.governance.lineage import DatasetRef, LineageTracker
            tracker = LineageTracker()
            run_id = tracker.start_run(
                f"ingest:{source_id}",
                inputs=[DatasetRef(namespace="api", name=source_id)],
                outputs=[DatasetRef(namespace="local", name=f"raw/{source_id}")],
            )
            tracker.complete_run(run_id, [DatasetRef(namespace="local", name=f"raw/{source_id}")])
            lineage_out = {
                "run_id": run_id,
                "events": len(tracker.events),
                "graph": tracker.get_lineage_graph(source_id),
            }
            log.info("lineage.recorded", source_id=source_id, run_id=run_id)
        except Exception as exc:
            lineage_out = {"error": str(exc)}

        result = {
            "source_id": source_id,
            "source_type": source_config.get("type"),
            "records_fetched": len(records),
            "sample_records": sample,
            "schema_columns": list(records[0].keys()) if records else [],
            "duration_ms": elapsed,
            "governance": {
                "classification": pii_out,
                "quality": quality_out,
                "lineage": lineage_out,
            },
        }

        log.info("ingest.complete", source_id=source_id, records=len(records), duration_ms=elapsed)
        await STATE.add_run({"type": "ingest", "source_id": source_id, "records": len(records), "ts": _now(), "quality": quality_out})
        return result

    except Exception as exc:
        log.error("ingest.failed", source_id=source_id, error=str(exc))
        raise HTTPException(500, f"Ingestion failed: {exc}")


# ---- Transform ----

@app.post("/api/transform/{transform_id}")
async def run_transform(transform_id: str):
    transforms = _load_transforms()
    if transform_id not in transforms:
        raise HTTPException(404, f"Transform {transform_id!r} not found")

    spec_raw = transforms[transform_id]
    source_id = spec_raw.get("source", "")
    log.info("transform.start", transform_id=transform_id, source=source_id)
    start = time.time()

    try:
        from src.transform.dsl_parser import DSLParser
        spec = DSLParser.parse_dict(spec_raw)
        engine_hint = spec.engine_hint
        log.info("transform.parsed", steps=len(spec.steps), engine_hint=engine_hint)

        # Fetch some sample data from the source to transform
        sources = _load_sources()
        sample_records: list[dict] = []
        if source_id in sources:
            source_config = sources[source_id]
            platform_config = _load_platform()
            from src.core.context_layer import ContextLayer
            layer = ContextLayer(platform_config)
            ctx = await layer.get_or_discover(source_id, source_config)
            from src.core.orchestrator import select_ingestor
            ingestor = select_ingestor(source_config.get("type", "api"))
            sample_records = await ingestor.ingest(source_config=source_config, context=ctx)
            log.info("transform.source_fetched", records=len(sample_records))

        # Pick engine
        estimated = len(sample_records)
        chosen_engine = DSLParser.recommend_engine(spec, estimated)
        log.info("transform.engine_selected", engine=chosen_engine, estimated_rows=estimated, threshold=spec.volume_threshold)

        # Run Python engine (always available, Spark requires cluster)
        from src.transform.python_engine import PythonTransformEngine
        engine = PythonTransformEngine()
        result = engine.execute(spec, sample_records)

        elapsed = int((time.time() - start) * 1000)
        out = {
            "transform_id": transform_id,
            "recommended_engine": chosen_engine,
            "actual_engine": "python",
            "rows_in": result.rows_in,
            "rows_out": result.rows_out,
            "quality_violations": result.quality_violations,
            "steps_executed": len(spec.steps),
            "step_types": [s.type if hasattr(s, "type") else "unknown" for s in spec.steps],
            "duration_ms": elapsed,
        }
        log.info("transform.complete", transform_id=transform_id, rows_in=result.rows_in, rows_out=result.rows_out, duration_ms=elapsed)
        await STATE.add_run({"type": "transform", "transform_id": transform_id, "rows_in": result.rows_in, "rows_out": result.rows_out, "ts": _now()})
        return out

    except Exception as exc:
        log.error("transform.failed", transform_id=transform_id, error=str(exc))
        raise HTTPException(500, f"Transform failed: {exc}")


# ---- Validate Transform ----

@app.post("/api/validate/{transform_id}")
async def validate_transform(transform_id: str):
    transforms = _load_transforms()
    if transform_id not in transforms:
        raise HTTPException(404, f"Transform {transform_id!r} not found")

    from src.transform.dsl_parser import DSLParser
    spec = DSLParser.parse_dict(transforms[transform_id])
    errors = DSLParser.validate_steps(list(spec.steps))
    return {
        "transform_id": transform_id,
        "valid": len(errors) == 0,
        "steps": len(spec.steps),
        "engine_hint": spec.engine_hint,
        "output_table": spec.output.table,
        "warnings": errors,
    }


# ---- Pipeline Status ----

@app.get("/api/status")
async def get_status():
    runs = await STATE.get_runs()
    return {
        "total_runs": len(runs),
        "recent_runs": runs[-20:],
        "sources_configured": len(_load_sources()),
        "transforms_configured": len(_load_transforms()),
    }


# ---- SSE Log Stream ----

@app.get("/api/stream/logs")
async def stream_logs():
    async def event_generator():
        while True:
            try:
                entry = LOG_BUFFER.get_nowait()
                yield {"event": "log", "data": json.dumps(entry, default=str)}
            except queue.Empty:
                yield {"event": "ping", "data": ""}
                await asyncio.sleep(0.3)

    return EventSourceResponse(event_generator())


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------
def main() -> None:
    import uvicorn
    port = int(os.environ.get("HYDRA_DASHBOARD_PORT", "8550"))
    print(f"\n  hydra-ingest dashboard → http://localhost:{port}\n")
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="warning")


if __name__ == "__main__":
    main()
