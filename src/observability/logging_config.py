"""Structured logging setup (structlog + stdlib integration)."""

from __future__ import annotations

import logging
import os
from typing import Any

import structlog
from structlog.types import Processor


class HydraLogProcessor:
    """Inject platform context (environment, component_id, run_id) into every event."""

    def __init__(
        self,
        environment: str | None = None,
        component_id: str | None = None,
        run_id: str | None = None,
    ) -> None:
        self._default_env = environment
        self._default_component = component_id
        self._default_run = run_id

    def __call__(self, logger: Any, method_name: str, event_dict: dict[str, Any]) -> dict[str, Any]:
        env = self._default_env or os.environ.get("HYDRA_ENV") or os.environ.get("ENVIRONMENT") or "development"
        event_dict.setdefault("environment", env)
        cid = structlog.contextvars.get_contextvars().get("component_id") or self._default_component
        if cid is not None:
            event_dict.setdefault("component_id", cid)
        rid = structlog.contextvars.get_contextvars().get("run_id") or self._default_run
        if rid is not None:
            event_dict.setdefault("run_id", rid)
        return event_dict


def bind_platform_context(*, component_id: str | None = None, run_id: str | None = None) -> None:
    """Bind identifiers for HydraLogProcessor via structlog contextvars."""
    if component_id is not None:
        structlog.contextvars.bind_contextvars(component_id=component_id)
    if run_id is not None:
        structlog.contextvars.bind_contextvars(run_id=run_id)


def clear_platform_context() -> None:
    structlog.contextvars.clear_contextvars()


def _foreign_pre_chain(hydra_processor: HydraLogProcessor) -> list[Processor]:
    return [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        hydra_processor,
    ]


def configure_logging(level: str = "INFO", format: str = "json") -> None:
    """
    Configure structlog with JSON (production) or colored console (development) rendering.

    Integrates with the standard library ``logging`` package so third-party loggers
    are formatted consistently.
    """
    hydra_processor = HydraLogProcessor()
    log_level = getattr(logging, level.upper(), logging.INFO)

    if format.lower() == "json":
        renderer: Processor = structlog.processors.JSONRenderer()
    else:
        renderer = structlog.dev.ConsoleRenderer(colors=True)

    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=_foreign_pre_chain(hydra_processor),
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            structlog.processors.CallsiteParameterAdder(
                {
                    structlog.processors.CallsiteParameter.FILENAME,
                    structlog.processors.CallsiteParameter.FUNC_NAME,
                    structlog.processors.CallsiteParameter.LINENO,
                }
            ),
            renderer,
        ],
    )

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(log_level)

    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.contextvars.merge_contextvars,
            hydra_processor,
            structlog.processors.CallsiteParameterAdder(
                {
                    structlog.processors.CallsiteParameter.FILENAME,
                    structlog.processors.CallsiteParameter.FUNC_NAME,
                    structlog.processors.CallsiteParameter.LINENO,
                }
            ),
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )


def get_logger(name: str) -> structlog.BoundLogger:
    return structlog.get_logger(name)
