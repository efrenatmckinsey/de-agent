"""Parse and validate the hydra-ingest transform YAML DSL."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated, Any, Literal, Union

import structlog
import yaml
from pydantic import AliasChoices, BaseModel, ConfigDict, Field, field_validator

from src.core.exceptions import TransformError

logger = structlog.get_logger(__name__)

_VALID_AGG_FUNCS = frozenset({"sum", "avg", "mean", "min", "max", "count"})
_VALID_JOIN_HOW = frozenset({"inner", "left", "right", "outer", "left_outer", "right_outer", "full", "full_outer"})


class StepBase(BaseModel):
    """Optional step identifier from YAML ``id``."""

    model_config = ConfigDict(populate_by_name=True)

    step_id: str | None = Field(default=None, alias="id")


class FilterStep(StepBase):
    type: Literal["filter"] = "filter"
    condition: str = Field(..., validation_alias=AliasChoices("condition", "where"))


class FlattenStep(StepBase):
    type: Literal["flatten"] = "flatten"
    source_column: str = Field(..., validation_alias=AliasChoices("source_column", "column"))
    output_columns: list[str] = Field(..., validation_alias=AliasChoices("output_columns", "into"))


class AggregateMeasure(BaseModel):
    column: str
    function: str
    alias: str

    @field_validator("function")
    @classmethod
    def normalize_function(cls, v: str) -> str:
        fn = v.strip().lower()
        if fn == "mean":
            return "avg"
        return fn


class AggregateStep(StepBase):
    type: Literal["aggregate"] = "aggregate"
    group_by: list[str]
    measures: list[AggregateMeasure]


class DeriveStep(StepBase):
    type: Literal["derive"] = "derive"
    new_column: str
    expression: str


class RenameStep(StepBase):
    type: Literal["rename"] = "rename"
    mappings: dict[str, str]


class CastStep(StepBase):
    type: Literal["cast"] = "cast"
    type_mappings: dict[str, str] = Field(
        ...,
        validation_alias=AliasChoices("type_mappings", "columns"),
    )


class QualityGateStep(StepBase):
    type: Literal["quality_gate"] = "quality_gate"
    checks: list[dict[str, Any]]


class JoinStep(StepBase):
    type: Literal["join"] = "join"
    right_source: str
    on: list[str]
    how: str = "inner"


TransformStep = Annotated[
    Union[
        FilterStep,
        FlattenStep,
        AggregateStep,
        DeriveStep,
        RenameStep,
        CastStep,
        QualityGateStep,
        JoinStep,
    ],
    Field(discriminator="type"),
]


class OutputSpec(BaseModel):
    table: str
    format: str
    partition_by: list[str] = Field(default_factory=list)


class TransformSpec(BaseModel):
    transform_id: str
    engine_hint: Literal["auto", "spark", "python"] = "auto"
    volume_threshold: int = Field(default=100_000, ge=0)
    source: str
    steps: list[TransformStep]
    output: OutputSpec


class DSLParser:
    """Load transform YAML / dicts into ``TransformSpec`` and validate."""

    @staticmethod
    def parse_file(path: str | Path) -> TransformSpec:
        p = Path(path)
        log = logger.bind(path=str(p))
        if not p.is_file():
            msg = f"Transform spec file not found: {p}"
            log.error("dsl.parse_file.missing", error=msg)
            raise TransformError(msg)
        try:
            text = p.read_text(encoding="utf-8")
            data = yaml.safe_load(text)
        except yaml.YAMLError as e:
            log.exception("dsl.parse_file.yaml_error")
            raise TransformError(f"Invalid YAML in {p}: {e}") from e
        except OSError as e:
            log.exception("dsl.parse_file.read_error")
            raise TransformError(f"Cannot read transform file {p}: {e}") from e
        if not isinstance(data, dict):
            raise TransformError(f"Transform spec root must be a mapping, got {type(data).__name__}")
        return DSLParser.parse_dict(data)

    @staticmethod
    def parse_dict(d: dict[str, Any]) -> TransformSpec:
        log = logger.bind(transform_id=d.get("transform_id"))
        try:
            spec = TransformSpec.model_validate(d)
        except Exception as e:
            log.warning("dsl.parse_dict.validation_failed", error=str(e))
            raise TransformError(f"Invalid transform specification: {e}") from e
        errors = DSLParser.validate_steps(list(spec.steps))
        if errors:
            log.warning("dsl.parse_dict.step_warnings", errors=errors)
        return spec

    @staticmethod
    def validate_steps(steps: list[Any]) -> list[str]:
        """Return human-readable validation messages (non-fatal for parse_dict)."""
        errors: list[str] = []
        for i, step in enumerate(steps):
            prefix = f"steps[{i}]"
            if isinstance(step, JoinStep):
                how = step.how.lower().replace(" ", "_")
                if how not in _VALID_JOIN_HOW:
                    errors.append(f"{prefix}: join.how '{step.how}' is not a supported join type")
                if not step.on:
                    errors.append(f"{prefix}: join.on must be non-empty")
            elif isinstance(step, AggregateStep):
                for j, m in enumerate(step.measures):
                    if m.function not in _VALID_AGG_FUNCS:
                        errors.append(
                            f"{prefix}.measures[{j}]: unknown aggregate function '{m.function}' "
                            f"(expected one of {sorted(_VALID_AGG_FUNCS)})"
                        )
            elif isinstance(step, FlattenStep):
                if not step.output_columns:
                    errors.append(f"{prefix}: flatten.output_columns must be non-empty")
            elif isinstance(step, QualityGateStep):
                for j, chk in enumerate(step.checks):
                    ctype = chk.get("type")
                    if ctype not in ("not_null", "valid_range", "unique_key", "row_count", None):
                        errors.append(f"{prefix}.checks[{j}]: unknown check type {ctype!r}")
                    if ctype == "not_null" and not chk.get("columns"):
                        errors.append(f"{prefix}.checks[{j}]: not_null requires 'columns'")
                    if ctype == "valid_range" and not chk.get("column"):
                        errors.append(f"{prefix}.checks[{j}]: valid_range requires 'column'")
                    if ctype == "unique_key" and not chk.get("columns"):
                        errors.append(f"{prefix}.checks[{j}]: unique_key requires 'columns'")
        return errors

    @staticmethod
    def recommend_engine(spec: TransformSpec, estimated_rows: int) -> str:
        """Return ``spark`` or ``python`` based on hint and volume."""
        if spec.engine_hint == "spark":
            return "spark"
        if spec.engine_hint == "python":
            return "python"
        return "spark" if estimated_rows >= spec.volume_threshold else "python"
