"""Prometheus metrics for hydra-ingest."""

from __future__ import annotations

import structlog
from prometheus_client import Counter, Gauge, Histogram, start_http_server

from src.governance.quality import QualityResult
from src.ingest.base import IngestResult
from src.storage.iceberg_writer import WriteResult
from src.transform.models import TransformResult

logger = structlog.get_logger(__name__)

# Default histogram buckets suitable for ingestion / transform latency (seconds)
_DURATION_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0)

ingestion_records_total = Counter(
    "ingestion_records_total",
    "Records ingested per source",
    labelnames=("source_id", "source_type"),
)
ingestion_bytes_total = Counter(
    "ingestion_bytes_total",
    "Bytes ingested per source",
    labelnames=("source_id",),
)
ingestion_duration_seconds = Histogram(
    "ingestion_duration_seconds",
    "Wall time for ingestion runs",
    labelnames=("source_id", "source_type"),
    buckets=_DURATION_BUCKETS,
)
ingestion_errors_total = Counter(
    "ingestion_errors_total",
    "Ingestion errors",
    labelnames=("source_id", "error_type"),
)
transform_records_total = Counter(
    "transform_records_total",
    "Records processed by transforms",
    labelnames=("transform_id", "engine"),
)
transform_duration_seconds = Histogram(
    "transform_duration_seconds",
    "Transform execution duration",
    labelnames=("transform_id", "engine"),
    buckets=_DURATION_BUCKETS,
)
quality_checks_total = Counter(
    "quality_checks_total",
    "Quality check outcomes",
    labelnames=("source_id", "result"),
)
quality_pass_rate = Gauge(
    "quality_pass_rate",
    "Latest quality pass rate per source",
    labelnames=("source_id",),
)
active_components = Gauge(
    "active_components",
    "Active platform components",
    labelnames=("component_type",),
)
storage_writes_total = Counter(
    "storage_writes_total",
    "Storage write operations",
    labelnames=("zone",),
)
storage_bytes_total = Counter(
    "storage_bytes_total",
    "Bytes written to lake zones",
    labelnames=("zone",),
)


class MetricsCollector:
    """Updates platform-wide Prometheus metrics (shared registry)."""

    def __init__(self) -> None:
        self._http_started = False
        logger.debug("metrics.collector_ready")

    def record_ingestion(self, result: IngestResult) -> None:
        source_type = str(result.metadata.get("source_type", "unknown"))
        ingestion_records_total.labels(
            source_id=result.source_id,
            source_type=source_type,
        ).inc(max(result.records_fetched, 0))
        ingestion_bytes_total.labels(source_id=result.source_id).inc(max(result.bytes_fetched, 0))
        if result.completed_at and result.started_at:
            delta = (result.completed_at - result.started_at).total_seconds()
            if delta >= 0:
                ingestion_duration_seconds.labels(
                    source_id=result.source_id,
                    source_type=source_type,
                ).observe(delta)
        for err in result.errors:
            err_type = _classify_error(err)
            ingestion_errors_total.labels(source_id=result.source_id, error_type=err_type).inc()

    def record_transform(self, result: TransformResult) -> None:
        engine = result.engine_used
        transform_records_total.labels(
            transform_id=result.transform_id,
            engine=engine,
        ).inc(max(result.rows_out, 0))
        delta = (result.completed_at - result.started_at).total_seconds()
        if delta >= 0:
            transform_duration_seconds.labels(
                transform_id=result.transform_id,
                engine=engine,
            ).observe(delta)

    def record_quality(self, result: QualityResult) -> None:
        if result.checks_passed:
            quality_checks_total.labels(source_id=result.source_id, result="passed").inc(result.checks_passed)
        if result.checks_failed:
            quality_checks_total.labels(source_id=result.source_id, result="failed").inc(result.checks_failed)
        quality_pass_rate.labels(source_id=result.source_id).set(result.pass_rate)

    def record_storage_write(self, result: WriteResult, zone: str) -> None:
        z = _normalize_zone(zone)
        storage_writes_total.labels(zone=z).inc(1)
        storage_bytes_total.labels(zone=z).inc(max(result.bytes_written, 0))

    def set_active_components(self, component_type: str, value: float) -> None:
        active_components.labels(component_type=component_type).set(value)

    def inc_active_components(self, component_type: str, delta: float = 1.0) -> None:
        active_components.labels(component_type=component_type).inc(delta)

    def dec_active_components(self, component_type: str, delta: float = 1.0) -> None:
        active_components.labels(component_type=component_type).dec(delta)

    def start_server(self, port: int) -> None:
        """Start the Prometheus metrics HTTP endpoint (idempotent per collector instance)."""
        if self._http_started:
            logger.warning("metrics.http_already_started", port=port)
            return
        try:
            start_http_server(port)
            self._http_started = True
            logger.info("metrics.prometheus_http_started", port=port)
        except OSError as exc:
            logger.exception("metrics.prometheus_http_failed", port=port, error=str(exc))
            raise


def _classify_error(message: str) -> str:
    m = (message or "").lower().strip()
    if not m:
        return "unknown"
    if "timeout" in m:
        return "timeout"
    if "http" in m or "status" in m:
        return "http"
    if "auth" in m or "403" in m or "401" in m:
        return "auth"
    if "config" in m:
        return "configuration"
    return "generic"


def _normalize_zone(zone: str) -> str:
    z = str(zone).lower().strip()
    if z in ("raw", "curated", "governed"):
        return z
    logger.warning("metrics.unknown_storage_zone", zone=zone)
    return z or "unknown"
