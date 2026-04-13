"""OpenTelemetry tracing for hydra-ingest."""

from __future__ import annotations

import time
from contextlib import contextmanager
from typing import Any, Generator

import structlog
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider as SDKTracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter, SpanExporter
from opentelemetry.trace import Span, Tracer

logger = structlog.get_logger(__name__)


class TracingProvider:
    """Configure OTLP or console export and expose tracer helpers."""

    def __init__(self, service_name: str = "hydra-ingest", otlp_endpoint: str | None = None) -> None:
        self._service_name = service_name
        self._otlp_endpoint = (otlp_endpoint or "").strip() or None
        self._provider: SDKTracerProvider | None = None
        self._setup()

    def _setup(self) -> None:
        resource = Resource.create(
            {
                "service.name": self._service_name,
                "service.namespace": "hydra-ingest",
            }
        )
        provider = SDKTracerProvider(resource=resource)
        exporter = self._build_exporter()
        if isinstance(exporter, SpanExporter):
            provider.add_span_processor(BatchSpanProcessor(exporter))
        trace.set_tracer_provider(provider)
        self._provider = provider
        mode = "otlp" if self._otlp_endpoint else "console"
        logger.info("tracing.provider_configured", mode=mode, service_name=self._service_name)

    def _build_exporter(self) -> SpanExporter:
        if self._otlp_endpoint:
            try:
                from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

                endpoint = self._otlp_endpoint
                if not endpoint.endswith("/v1/traces"):
                    endpoint = endpoint.rstrip("/") + "/v1/traces"
                return OTLPSpanExporter(endpoint=endpoint)
            except Exception as exc:
                logger.exception(
                    "tracing.otlp_exporter_failed",
                    error=str(exc),
                    fallback="console",
                )
        return ConsoleSpanExporter()

    def get_tracer(self, component: str) -> Tracer:
        return trace.get_tracer(component, tracer_provider=self._provider)

    @contextmanager
    def trace_ingestion(self, source_id: str) -> Generator[Span, None, None]:
        tracer = self.get_tracer("hydra.ingestion")
        with tracer.start_as_current_span("ingestion") as span:
            span.set_attribute("source_id", source_id)
            span.set_attribute("hydra.source_id", source_id)
            start = time.perf_counter()
            try:
                yield span
            finally:
                duration = time.perf_counter() - start
                span.set_attribute("duration", duration)
                span.set_attribute("hydra.duration_seconds", duration)

    @contextmanager
    def trace_transform(self, transform_id: str) -> Generator[Span, None, None]:
        tracer = self.get_tracer("hydra.transform")
        with tracer.start_as_current_span("transform") as span:
            span.set_attribute("transform_id", transform_id)
            span.set_attribute("hydra.transform_id", transform_id)
            start = time.perf_counter()
            try:
                yield span
            finally:
                duration = time.perf_counter() - start
                span.set_attribute("duration", duration)
                span.set_attribute("hydra.duration_seconds", duration)

    @contextmanager
    def trace_quality_check(self, source_id: str) -> Generator[Span, None, None]:
        tracer = self.get_tracer("hydra.quality")
        with tracer.start_as_current_span("quality_check") as span:
            span.set_attribute("source_id", source_id)
            span.set_attribute("hydra.source_id", source_id)
            start = time.perf_counter()
            try:
                yield span
            finally:
                duration = time.perf_counter() - start
                span.set_attribute("duration", duration)
                span.set_attribute("hydra.duration_seconds", duration)

    def shutdown(self) -> None:
        if self._provider is not None:
            try:
                self._provider.shutdown()
                logger.info("tracing.provider_shutdown", service_name=self._service_name)
            except Exception as exc:
                logger.warning("tracing.shutdown_error", error=str(exc))


def span_set_ingestion_stats(span: Span, *, records_count: int | None = None, errors: list[Any] | None = None) -> None:
    """Attach common ingestion attributes to the active span."""
    if records_count is not None:
        span.set_attribute("records_count", int(records_count))
        span.set_attribute("hydra.records_count", int(records_count))
    if errors is not None:
        span.set_attribute("error_count", len(errors))
        span.set_attribute("hydra.error_count", len(errors))
        if errors:
            span.set_attribute("hydra.last_error", str(errors[-1])[:1024])
