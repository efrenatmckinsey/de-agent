"""Tests for the transform DSL parser."""

from __future__ import annotations

import pytest

from src.transform.dsl_parser import (
    AggregateStep,
    CastStep,
    DSLParser,
    DeriveStep,
    FilterStep,
    FlattenStep,
    QualityGateStep,
    RenameStep,
    TransformSpec,
)


MINIMAL_SPEC = {
    "transform_id": "test_transform",
    "engine_hint": "python",
    "source": "test_source",
    "steps": [
        {"type": "filter", "condition": "value IS NOT NULL"},
    ],
    "output": {"table": "curated.test", "format": "parquet"},
}


def test_parse_minimal_spec():
    spec = DSLParser.parse_dict(MINIMAL_SPEC)
    assert spec.transform_id == "test_transform"
    assert spec.engine_hint == "python"
    assert spec.source == "test_source"
    assert len(spec.steps) == 1
    assert isinstance(spec.steps[0], FilterStep)
    assert spec.steps[0].condition == "value IS NOT NULL"


def test_parse_all_step_types():
    raw = {
        "transform_id": "full_pipeline",
        "source": "src",
        "steps": [
            {"type": "filter", "where": "col > 0"},
            {"type": "flatten", "column": "items", "into": ["name", "price"]},
            {"type": "rename", "mappings": {"old_name": "new_name"}},
            {"type": "cast", "columns": {"amount": "float", "count": "int"}},
            {"type": "derive", "new_column": "total", "expression": "amount * count"},
            {
                "type": "aggregate",
                "group_by": ["category"],
                "measures": [
                    {"column": "amount", "function": "sum", "alias": "total_amount"},
                    {"column": "amount", "function": "avg", "alias": "avg_amount"},
                ],
            },
            {
                "type": "quality_gate",
                "checks": [
                    {"type": "not_null", "columns": ["category", "total_amount"]},
                    {"type": "valid_range", "column": "total_amount", "min": 0},
                ],
            },
        ],
        "output": {"table": "curated.output", "format": "iceberg", "partition_by": ["category"]},
    }
    spec = DSLParser.parse_dict(raw)
    assert len(spec.steps) == 7
    assert isinstance(spec.steps[0], FilterStep)
    assert spec.steps[0].condition == "col > 0"
    assert isinstance(spec.steps[1], FlattenStep)
    assert spec.steps[1].source_column == "items"
    assert isinstance(spec.steps[2], RenameStep)
    assert isinstance(spec.steps[3], CastStep)
    assert spec.steps[3].type_mappings == {"amount": "float", "count": "int"}
    assert isinstance(spec.steps[4], DeriveStep)
    assert isinstance(spec.steps[5], AggregateStep)
    assert len(spec.steps[5].measures) == 2
    assert isinstance(spec.steps[6], QualityGateStep)
    assert spec.output.partition_by == ["category"]


def test_validate_steps_catches_bad_agg_function():
    raw = {
        "transform_id": "bad_agg",
        "source": "s",
        "steps": [
            {
                "type": "aggregate",
                "group_by": ["x"],
                "measures": [{"column": "y", "function": "median", "alias": "m"}],
            },
        ],
        "output": {"table": "t", "format": "parquet"},
    }
    spec = DSLParser.parse_dict(raw)
    errors = DSLParser.validate_steps(list(spec.steps))
    assert any("median" in e for e in errors)


def test_recommend_engine_auto():
    spec = TransformSpec(
        transform_id="t",
        engine_hint="auto",
        volume_threshold=1000,
        source="s",
        steps=[],
        output={"table": "t", "format": "p"},
    )
    assert DSLParser.recommend_engine(spec, 500) == "python"
    assert DSLParser.recommend_engine(spec, 1000) == "spark"
    assert DSLParser.recommend_engine(spec, 2000) == "spark"


def test_recommend_engine_forced():
    spec = TransformSpec(
        transform_id="t",
        engine_hint="spark",
        source="s",
        steps=[],
        output={"table": "t", "format": "p"},
    )
    assert DSLParser.recommend_engine(spec, 1) == "spark"

    spec2 = TransformSpec(
        transform_id="t",
        engine_hint="python",
        source="s",
        steps=[],
        output={"table": "t", "format": "p"},
    )
    assert DSLParser.recommend_engine(spec2, 10_000_000) == "python"


def test_parse_file_not_found():
    with pytest.raises(Exception, match="not found"):
        DSLParser.parse_file("/nonexistent/transform.yaml")


def test_mean_normalizes_to_avg():
    raw = {
        "transform_id": "t",
        "source": "s",
        "steps": [
            {
                "type": "aggregate",
                "group_by": ["g"],
                "measures": [{"column": "c", "function": "mean", "alias": "a"}],
            }
        ],
        "output": {"table": "t", "format": "p"},
    }
    spec = DSLParser.parse_dict(raw)
    assert isinstance(spec.steps[0], AggregateStep)
    assert spec.steps[0].measures[0].function == "avg"
