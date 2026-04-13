"""Shared Pydantic models for transform execution results."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class TransformResult(BaseModel):
    """Outcome of running a transform specification on an engine."""

    transform_id: str
    engine_used: str
    rows_in: int
    rows_out: int
    started_at: datetime
    completed_at: datetime
    quality_violations: list[dict[str, Any]] = Field(default_factory=list)
    output_path: str | None = None
