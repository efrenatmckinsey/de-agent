"""OpenLineage-compatible lineage tracking for hydra-ingest."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any

import structlog
from pydantic import BaseModel, Field

logger = structlog.get_logger(__name__)

try:
    import httpx
except ImportError:  # pragma: no cover
    httpx = None  # type: ignore[misc, assignment]


class LineageEventType(str, Enum):
    START = "START"
    COMPLETE = "COMPLETE"
    FAIL = "FAIL"


class DatasetRef(BaseModel):
    namespace: str
    name: str
    facets: dict[str, Any] = Field(default_factory=dict)


class LineageEvent(BaseModel):
    event_type: LineageEventType
    run_id: uuid.UUID
    job_name: str
    job_namespace: str = "hydra-ingest"
    event_time: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    inputs: list[DatasetRef] = Field(default_factory=list)
    outputs: list[DatasetRef] = Field(default_factory=list)
    facets: dict[str, Any] = Field(default_factory=dict)


def schema_facet(columns: list[str], types: dict[str, str] | None = None) -> dict[str, Any]:
    """OpenLineage-style schema facet (simplified JSON structure)."""
    fields = []
    for c in columns:
        fields.append({"name": c, "type": (types or {}).get(c, "unknown")})
    return {"_schemaURL": "https://openlineage.io/spec/facets/1-0-0/SchemaDatasetFacet.json", "fields": fields}


def data_quality_facet(
    row_count: int,
    null_counts: dict[str, int],
    unique_counts: dict[str, int] | None = None,
) -> dict[str, Any]:
    return {
        "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/DataQualityMetricsInputDatasetFacet.json",
        "rowCount": row_count,
        "nullCounts": null_counts,
        "uniqueCounts": unique_counts or {},
    }


def transform_facet(steps: list[str]) -> dict[str, Any]:
    return {
        "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/DocumentationJobFacet.json",
        "steps": steps,
        "engine": "hydra-ingest",
    }


class LineageTracker:
    """Emit OpenLineage run events to an HTTP backend or keep them in memory."""

    JOB_NAMESPACE_DEFAULT = "hydra-ingest"

    def __init__(self, openlineage_url: str | None = None) -> None:
        self._openlineage_url = (openlineage_url or "").rstrip("/") or None
        self.events: list[LineageEvent] = []
        self._run_states: dict[uuid.UUID, dict[str, Any]] = {}

    def start_run(
        self,
        job_name: str,
        inputs: list[DatasetRef],
        outputs: list[DatasetRef],
        *,
        job_namespace: str | None = None,
        facets: dict[str, Any] | None = None,
    ) -> str:
        run_id = uuid.uuid4()
        ns = job_namespace or self.JOB_NAMESPACE_DEFAULT
        event = LineageEvent(
            event_type=LineageEventType.START,
            run_id=run_id,
            job_name=job_name,
            job_namespace=ns,
            inputs=list(inputs),
            outputs=list(outputs),
            facets=dict(facets or {}),
        )
        self._run_states[run_id] = {"job_name": job_name, "job_namespace": ns, "inputs": inputs}
        self._emit(event)
        return str(run_id)

    def complete_run(
        self,
        run_id: str,
        outputs_with_facets: list[DatasetRef],
        *,
        facets: dict[str, Any] | None = None,
    ) -> LineageEvent:
        try:
            rid = uuid.UUID(run_id)
        except ValueError as e:
            logger.error("lineage.invalid_run_id", run_id=run_id)
            raise ValueError(f"Invalid run_id: {run_id!r}") from e

        state = self._run_states.get(rid, {})
        job_name = str(state.get("job_name", "unknown"))
        job_namespace = str(state.get("job_namespace", self.JOB_NAMESPACE_DEFAULT))
        inputs = list(state.get("inputs", []))

        merged_facets = dict(facets or {})
        event = LineageEvent(
            event_type=LineageEventType.COMPLETE,
            run_id=rid,
            job_name=job_name,
            job_namespace=job_namespace,
            inputs=inputs,
            outputs=list(outputs_with_facets),
            facets=merged_facets,
        )
        self._emit(event)
        self._run_states.pop(rid, None)
        return event

    def fail_run(self, run_id: str, error_message: str) -> LineageEvent:
        try:
            rid = uuid.UUID(run_id)
        except ValueError as e:
            logger.error("lineage.invalid_run_id", run_id=run_id)
            raise ValueError(f"Invalid run_id: {run_id!r}") from e

        state = self._run_states.get(rid, {})
        job_name = str(state.get("job_name", "unknown"))
        job_namespace = str(state.get("job_namespace", self.JOB_NAMESPACE_DEFAULT))
        inputs = list(state.get("inputs", []))
        facets = {"errorMessage": error_message}
        event = LineageEvent(
            event_type=LineageEventType.FAIL,
            run_id=rid,
            job_name=job_name,
            job_namespace=job_namespace,
            inputs=inputs,
            outputs=[],
            facets=facets,
        )
        self._emit(event)
        self._run_states.pop(rid, None)
        return event

    def get_lineage_graph(self, source_id: str) -> dict[str, Any]:
        """Build a simple upstream/downstream view keyed by dataset identity."""
        upstream: set[str] = set()
        downstream: set[str] = set()
        target_keys = self._dataset_keys_for_source(source_id)

        for ev in self.events:
            if ev.event_type == LineageEventType.START:
                continue
            in_keys = {self._dataset_key(d) for d in ev.inputs}
            out_keys = {self._dataset_key(d) for d in ev.outputs}
            touches = target_keys & (in_keys | out_keys)
            if not touches:
                continue
            if target_keys & in_keys:
                downstream |= out_keys - target_keys
            if target_keys & out_keys:
                upstream |= in_keys - target_keys

        return {
            "source_id": source_id,
            "upstream": sorted(upstream),
            "downstream": sorted(downstream),
            "events_considered": len(self.events),
        }

    @staticmethod
    def _dataset_key(d: DatasetRef) -> str:
        return f"{d.namespace}:{d.name}"

    def _dataset_keys_for_source(self, source_id: str) -> set[str]:
        keys: set[str] = set()
        for ev in self.events:
            for d in (*ev.inputs, *ev.outputs):
                if source_id in d.name or source_id in d.namespace:
                    keys.add(self._dataset_key(d))
                src_facet = (d.facets.get("source") or {}) if isinstance(d.facets, dict) else {}
                if isinstance(src_facet, dict) and src_facet.get("source_id") == source_id:
                    keys.add(self._dataset_key(d))
        if not keys:
            keys.add(f"{self.JOB_NAMESPACE_DEFAULT}:{source_id}")
        return keys

    def _emit(self, event: LineageEvent) -> None:
        self.events.append(event)
        if not self._openlineage_url:
            logger.debug("lineage.stored_in_memory", event_type=event.event_type.value, run_id=str(event.run_id))
            return

        if httpx is None:
            logger.warning("lineage.emit_skipped_no_httpx", run_id=str(event.run_id))
            return

        body = self._to_openlineage_json(event)
        url = f"{self._openlineage_url}/api/v1/lineage"
        try:
            with httpx.Client(timeout=30.0) as client:
                resp = client.post(url, json=body, headers={"Content-Type": "application/json"})
                resp.raise_for_status()
            logger.info("lineage.emitted", url=url, event_type=event.event_type.value, run_id=str(event.run_id))
        except Exception as exc:
            logger.exception(
                "lineage.emit_failed",
                url=url,
                run_id=str(event.run_id),
                error=str(exc),
            )

    @staticmethod
    def _to_openlineage_json(event: LineageEvent) -> dict[str, Any]:
        """Map our LineageEvent to a minimal OpenLineage RunEvent JSON shape."""
        return {
            "eventType": event.event_type.value,
            "eventTime": event.event_time.isoformat().replace("+00:00", "Z"),
            "run": {"runId": str(event.run_id)},
            "job": {"namespace": event.job_namespace, "name": event.job_name, "facets": event.facets},
            "inputs": [
                {"namespace": d.namespace, "name": d.name, "facets": d.facets} for d in event.inputs
            ],
            "outputs": [
                {"namespace": d.namespace, "name": d.name, "facets": d.facets} for d in event.outputs
            ],
            "producer": "https://github.com/hydra-ingest",
        }


def build_dataset_ref_with_facets(
    namespace: str,
    name: str,
    *,
    column_names: list[str],
    column_types: dict[str, str] | None = None,
    row_count: int = 0,
    null_counts: dict[str, int] | None = None,
    unique_counts: dict[str, int] | None = None,
    transform_steps: list[str] | None = None,
    source_id: str | None = None,
) -> DatasetRef:
    """Convenience: attach schema, optional data quality, and transform facets."""
    facets: dict[str, Any] = {
        "schema": schema_facet(column_names, column_types),
    }
    if null_counts is not None:
        facets["dataQuality"] = data_quality_facet(row_count, null_counts, unique_counts)
    if transform_steps:
        facets["transform"] = transform_facet(transform_steps)
    if source_id is not None:
        facets["source"] = {"source_id": source_id}
    return DatasetRef(namespace=namespace, name=name, facets=facets)
