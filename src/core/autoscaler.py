"""Resource-aware scaling decisions and optional process management (local / AWS placeholder)."""

from __future__ import annotations

import asyncio
import multiprocessing
import os
import time
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable

import psutil
import structlog
from pydantic import BaseModel, Field

from src.core.exceptions import ScalingError

logger = structlog.get_logger(__name__)


class ScalingDecision(str, Enum):
    """Outcome of an autoscaler evaluation cycle."""

    SCALE_UP = "scale_up"
    SCALE_DOWN = "scale_down"
    MAINTAIN = "maintain"
    SHUTDOWN = "shutdown"


class ComponentMetrics(BaseModel):
    """Point-in-time signals for one scalable component."""

    component_id: str
    cpu_percent: float = 0.0
    memory_mb: float = 0.0
    queue_depth: int = 0
    last_active: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


def _autoscaler_cfg(config: dict[str, Any]) -> dict[str, Any]:
    return config.get("autoscaler") or {}


def _memory_utilization_percent(used_mb: float) -> float:
    vm = psutil.virtual_memory()
    total_mb = float(vm.total) / (1024.0 * 1024.0)
    if total_mb <= 0:
        return 0.0
    return (used_mb / total_mb) * 100.0


def evaluate_scaling(metrics: ComponentMetrics, config: dict[str, Any]) -> ScalingDecision:
    """
    Pure threshold evaluation shared by autoscaler implementations.

    ``memory_mb`` on :class:`ComponentMetrics` is interpreted as **used RAM in MiB**;
    it is converted to a utilization percent using host totals from ``psutil`` for
    comparison with ``memory_percent_threshold`` in config.
    """
    ac = _autoscaler_cfg(config)
    idle_shutdown = float(ac.get("idle_shutdown_minutes") or 0)
    if idle_shutdown > 0:
        idle_minutes = (datetime.now(timezone.utc) - metrics.last_active).total_seconds() / 60.0
        if idle_minutes >= idle_shutdown:
            logger.info(
                "autoscaler.decision",
                decision=ScalingDecision.SHUTDOWN.value,
                reason="idle_timeout",
                component_id=metrics.component_id,
                idle_minutes=round(idle_minutes, 2),
            )
            return ScalingDecision.SHUTDOWN

    up = ac.get("scale_up") or {}
    down = ac.get("scale_down") or {}

    cpu_up = float(up.get("cpu_percent_threshold", 75))
    mem_up = float(up.get("memory_percent_threshold", 80))
    q_up = int(up.get("queue_depth_threshold", 500))

    cpu_down = float(down.get("cpu_percent_threshold", 35))
    mem_down = float(down.get("memory_percent_threshold", 45))
    q_down = int(down.get("queue_depth_threshold", 50))

    mem_pct = _memory_utilization_percent(metrics.memory_mb)

    if metrics.cpu_percent >= cpu_up or mem_pct >= mem_up or metrics.queue_depth >= q_up:
        logger.info(
            "autoscaler.decision",
            decision=ScalingDecision.SCALE_UP.value,
            component_id=metrics.component_id,
            cpu=metrics.cpu_percent,
            memory_mb=metrics.memory_mb,
            memory_percent=round(mem_pct, 2),
            queue_depth=metrics.queue_depth,
        )
        return ScalingDecision.SCALE_UP

    if metrics.cpu_percent <= cpu_down and mem_pct <= mem_down and metrics.queue_depth <= q_down:
        logger.info(
            "autoscaler.decision",
            decision=ScalingDecision.SCALE_DOWN.value,
            component_id=metrics.component_id,
            cpu=metrics.cpu_percent,
            memory_mb=metrics.memory_mb,
            memory_percent=round(mem_pct, 2),
            queue_depth=metrics.queue_depth,
        )
        return ScalingDecision.SCALE_DOWN

    logger.debug("autoscaler.decision", decision=ScalingDecision.MAINTAIN.value, component_id=metrics.component_id)
    return ScalingDecision.MAINTAIN


class IdleTracker:
    """Tracks per-component activity to support idle-based shutdown recommendations."""

    def __init__(self) -> None:
        self._last: dict[str, datetime] = {}
        self._lock = asyncio.Lock()

    async def touch(self, component_id: str) -> None:
        async with self._lock:
            self._last[component_id] = datetime.now(timezone.utc)

    async def last_active(self, component_id: str) -> datetime | None:
        async with self._lock:
            return self._last.get(component_id)

    async def recommend(self, component_id: str, idle_shutdown_minutes: float) -> ScalingDecision | None:
        if idle_shutdown_minutes <= 0:
            return None
        async with self._lock:
            ts = self._last.get(component_id)
        if ts is None:
            return None
        idle_minutes = (datetime.now(timezone.utc) - ts).total_seconds() / 60.0
        if idle_minutes >= idle_shutdown_minutes:
            logger.info(
                "idle_tracker.shutdown_recommended",
                component_id=component_id,
                idle_minutes=round(idle_minutes, 2),
            )
            return ScalingDecision.SHUTDOWN
        return None


def _worker_loop(_: object) -> None:
    """CPU-light placeholder worker process."""
    time.sleep(3600.0)


class AutoScaler(ABC):
    """Abstract autoscaler with shared ``evaluate`` semantics."""

    @abstractmethod
    async def apply_decision(self, decision: ScalingDecision, *, component_id: str) -> None:
        """Apply scaling decision to the underlying runtime (processes, ECS, etc.)."""

    def evaluate(self, metrics: ComponentMetrics, config: dict[str, Any]) -> ScalingDecision:
        """Return a :class:`ScalingDecision` from thresholds in ``config``."""
        return evaluate_scaling(metrics, config)


class LocalAutoScaler(AutoScaler):
    """Manages local worker processes via ``multiprocessing``."""

    def __init__(self, config: dict[str, Any]) -> None:
        self._config = config
        ac = _autoscaler_cfg(config)
        self._min_replicas = int(ac.get("min_replicas", 1))
        self._max_replicas = int(ac.get("max_replicas", 20))
        self._target: Callable[[object], None] = _worker_loop
        self._workers: list[multiprocessing.Process] = []
        self._log = logger.bind(autoscaler="local")

    @property
    def worker_count(self) -> int:
        return len([p for p in self._workers if p.is_alive()])

    async def apply_decision(self, decision: ScalingDecision, *, component_id: str) -> None:
        if decision == ScalingDecision.SCALE_UP:
            if self.worker_count >= self._max_replicas:
                self._log.warning("scale_up.skipped_at_max", component_id=component_id)
                return
            proc = multiprocessing.Process(target=self._target, args=(None,), daemon=True)
            proc.start()
            self._workers.append(proc)
            self._log.info("worker.spawned", pid=proc.pid, workers=self.worker_count)
            return
        if decision == ScalingDecision.SCALE_DOWN:
            if self.worker_count <= self._min_replicas:
                self._log.warning("scale_down.skipped_at_min", component_id=component_id)
                return
            proc = next((p for p in reversed(self._workers) if p.is_alive()), None)
            if proc is None:
                return
            proc.terminate()
            proc.join(timeout=5)
            self._log.info("worker.terminated", pid=proc.pid, workers=self.worker_count)
            return
        if decision == ScalingDecision.SHUTDOWN:
            for p in list(self._workers):
                if p.is_alive():
                    p.terminate()
                    p.join(timeout=5)
            self._workers.clear()
            self._log.info("workers.shutdown_all", component_id=component_id)
            return
        self._log.debug("maintain", component_id=component_id)

    @staticmethod
    def host_metrics(component_id: str, queue_depth: int, last_active: datetime) -> ComponentMetrics:
        """Build :class:`ComponentMetrics` using host-wide CPU/RAM (local dev helper)."""
        cpu = float(psutil.cpu_percent(interval=None))
        mem = psutil.virtual_memory()
        memory_mb = float(mem.used) / (1024.0 * 1024.0)
        return ComponentMetrics(
            component_id=component_id,
            cpu_percent=cpu,
            memory_mb=memory_mb,
            queue_depth=queue_depth,
            last_active=last_active,
        )


class AWSAutoScaler(AutoScaler):
    """Placeholder: would call ECS/EKS APIs to change desired count / HPA."""

    def __init__(self, config: dict[str, Any]) -> None:
        self._config = config
        self._log = logger.bind(autoscaler="aws")

    async def apply_decision(self, decision: ScalingDecision, *, component_id: str) -> None:
        ac = _autoscaler_cfg(self._config)
        cluster = os.environ.get("HYDRA_ECS_CLUSTER", "hydra-ingest-cluster")
        service = os.environ.get("HYDRA_ECS_SERVICE", "hydra-ingest-service")
        self._log.info(
            "aws_autoscaler.would_apply",
            decision=decision.value,
            component_id=component_id,
            ecs_cluster=cluster,
            ecs_service=service,
            min_replicas=ac.get("min_replicas"),
            max_replicas=ac.get("max_replicas"),
            note="ECS UpdateService / EKS kubectl scale would run here",
        )


def create_autoscaler(config: dict[str, Any]) -> AutoScaler:
    """Return a local process autoscaler or AWS placeholder based on config."""
    mode = (config.get("autoscaler") or {}).get("mode")
    if mode is None:
        env = (config.get("platform") or {}).get("environment", "dev")
        mode = "local" if str(env).lower() in {"dev", "local", "test"} else "aws"
    if mode == "aws":
        logger.info("autoscaler.created", implementation="aws")
        return AWSAutoScaler(config)
    logger.info("autoscaler.created", implementation="local")
    return LocalAutoScaler(config)
