"""Tests for the Python transform engine."""

from __future__ import annotations

import pytest

from src.transform.dsl_parser import DSLParser
from src.transform.python_engine import ExpressionEvaluator, PythonTransformEngine


SAMPLE_RECORDS = [
    {"name": "Widget A", "category": "tools", "price": 10.0, "qty": 100},
    {"name": "Widget B", "category": "tools", "price": 25.0, "qty": 50},
    {"name": "Gadget C", "category": "electronics", "price": 99.0, "qty": 200},
    {"name": "Gadget D", "category": "electronics", "price": 150.0, "qty": 75},
    {"name": "Gizmo E", "category": "tools", "price": 5.0, "qty": 300},
]


class TestExpressionEvaluator:
    def test_simple_comparison(self):
        ev = ExpressionEvaluator()
        row = {"age": 30, "name": "Alice"}
        assert ev.eval_filter("age > 25", row) is True
        assert ev.eval_filter("age < 25", row) is False

    def test_is_not_null(self):
        ev = ExpressionEvaluator()
        assert ev.eval_filter("name IS NOT NULL", {"name": "Alice"}) is True
        assert ev.eval_filter("name IS NOT NULL", {"name": None}) is False

    def test_is_null(self):
        ev = ExpressionEvaluator()
        assert ev.eval_filter("value IS NULL", {"value": None}) is True
        assert ev.eval_filter("value IS NULL", {"value": 0}) is False

    def test_equality(self):
        ev = ExpressionEvaluator()
        assert ev.eval_filter("status = 'active'", {"status": "active"}) is True
        assert ev.eval_filter("status != 'active'", {"status": "inactive"}) is True


class TestPythonTransformEngine:
    def test_filter(self):
        spec_dict = {
            "transform_id": "filter_test",
            "source": "s",
            "steps": [{"type": "filter", "condition": "price > 20"}],
            "output": {"table": "t", "format": "p"},
        }
        spec = DSLParser.parse_dict(spec_dict)
        engine = PythonTransformEngine()
        result = engine.execute(spec, SAMPLE_RECORDS)
        assert result.rows_out < result.rows_in
        assert result.rows_out == 3

    def test_rename(self):
        spec_dict = {
            "transform_id": "rename_test",
            "source": "s",
            "steps": [{"type": "rename", "mappings": {"name": "product_name", "qty": "quantity"}}],
            "output": {"table": "t", "format": "p"},
        }
        spec = DSLParser.parse_dict(spec_dict)
        engine = PythonTransformEngine()
        result = engine.execute(spec, SAMPLE_RECORDS)
        assert result.rows_out == 5

    def test_cast(self):
        records = [
            {"count": "100", "amount": "3.14", "active": "true"},
            {"count": "200", "amount": "2.71", "active": "false"},
        ]
        spec_dict = {
            "transform_id": "cast_test",
            "source": "s",
            "steps": [{"type": "cast", "columns": {"count": "int", "amount": "float", "active": "bool"}}],
            "output": {"table": "t", "format": "p"},
        }
        spec = DSLParser.parse_dict(spec_dict)
        engine = PythonTransformEngine()
        result = engine.execute(spec, records)
        assert result.rows_out == 2

    def test_aggregate(self):
        spec_dict = {
            "transform_id": "agg_test",
            "source": "s",
            "steps": [
                {
                    "type": "aggregate",
                    "group_by": ["category"],
                    "measures": [
                        {"column": "price", "function": "sum", "alias": "total_price"},
                        {"column": "price", "function": "avg", "alias": "avg_price"},
                        {"column": "name", "function": "count", "alias": "item_count"},
                    ],
                }
            ],
            "output": {"table": "t", "format": "p"},
        }
        spec = DSLParser.parse_dict(spec_dict)
        engine = PythonTransformEngine()
        result = engine.execute(spec, SAMPLE_RECORDS)
        assert result.rows_out == 2

    def test_quality_gate_pass(self):
        spec_dict = {
            "transform_id": "qg_test",
            "source": "s",
            "steps": [
                {
                    "type": "quality_gate",
                    "checks": [
                        {"type": "not_null", "columns": ["name", "category"]},
                        {"type": "row_count", "min": 1},
                    ],
                }
            ],
            "output": {"table": "t", "format": "p"},
        }
        spec = DSLParser.parse_dict(spec_dict)
        engine = PythonTransformEngine()
        result = engine.execute(spec, SAMPLE_RECORDS)
        assert len(result.quality_violations) == 0

    def test_quality_gate_fail(self):
        records_with_nulls = SAMPLE_RECORDS + [{"name": None, "category": "tools", "price": 0, "qty": 0}]
        spec_dict = {
            "transform_id": "qg_fail_test",
            "source": "s",
            "steps": [
                {
                    "type": "quality_gate",
                    "checks": [{"type": "not_null", "columns": ["name"]}],
                }
            ],
            "output": {"table": "t", "format": "p"},
        }
        spec = DSLParser.parse_dict(spec_dict)
        engine = PythonTransformEngine()
        result = engine.execute(spec, records_with_nulls)
        assert len(result.quality_violations) > 0

    def test_multi_step_pipeline(self):
        spec_dict = {
            "transform_id": "multi_step",
            "source": "s",
            "steps": [
                {"type": "filter", "condition": "category = 'tools'"},
                {"type": "rename", "mappings": {"qty": "quantity"}},
                {
                    "type": "aggregate",
                    "group_by": ["category"],
                    "measures": [
                        {"column": "price", "function": "sum", "alias": "total"},
                        {"column": "price", "function": "count", "alias": "num_items"},
                    ],
                },
            ],
            "output": {"table": "curated.tools_summary", "format": "iceberg"},
        }
        spec = DSLParser.parse_dict(spec_dict)
        engine = PythonTransformEngine()
        result = engine.execute(spec, SAMPLE_RECORDS)
        assert result.transform_id == "multi_step"
        assert result.engine_used == "python"
        assert result.rows_in == 5
        assert result.rows_out == 1
