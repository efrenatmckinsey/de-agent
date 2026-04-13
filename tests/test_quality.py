"""Tests for the governance quality engine."""

from __future__ import annotations

from datetime import datetime, timezone

from src.governance.quality import QualityCheckType, QualityEngine


def _gate(fail_on_critical: bool = True, warn_threshold: float = 0.95) -> dict:
    return {"fail_on_critical": fail_on_critical, "warn_threshold": warn_threshold}


SAMPLE_RECORDS = [
    {"name": "Alice", "age": 30, "state": "NY", "score": 85.5},
    {"name": "Bob", "age": 25, "state": "CA", "score": 92.0},
    {"name": None, "age": 40, "state": "TX", "score": None},
    {"name": "Diana", "age": 35, "state": "NY", "score": 78.0},
    {"name": "Eve", "age": 28, "state": "CA", "score": 95.5},
]


def test_not_null_passes():
    engine = QualityEngine()
    result = engine.run_checks(
        "test",
        SAMPLE_RECORDS,
        [{"type": "not_null", "columns": ["age", "state"]}],
        _gate(),
    )
    assert result.checks_passed == 1
    assert result.passed_gate is True


def test_not_null_fails():
    engine = QualityEngine()
    result = engine.run_checks(
        "test",
        SAMPLE_RECORDS,
        [{"type": "not_null", "columns": ["name", "score"]}],
        _gate(),
    )
    assert result.checks_failed == 1
    violations = [v for v in result.violations if v.check_type == QualityCheckType.NOT_NULL]
    assert len(violations) >= 1
    assert result.passed_gate is False


def test_valid_range():
    engine = QualityEngine()
    result = engine.run_checks(
        "test",
        SAMPLE_RECORDS,
        [{"type": "valid_range", "column": "age", "min": 20, "max": 50}],
        _gate(),
    )
    assert result.checks_passed == 1

    result_fail = engine.run_checks(
        "test",
        SAMPLE_RECORDS,
        [{"type": "valid_range", "column": "age", "min": 30, "max": 35}],
        _gate(),
    )
    assert result_fail.checks_failed == 1


def test_unique_key_pass():
    records = [
        {"state": "NY", "year": 2020},
        {"state": "CA", "year": 2020},
        {"state": "NY", "year": 2021},
    ]
    engine = QualityEngine()
    result = engine.run_checks(
        "test",
        records,
        [{"type": "unique_key", "columns": ["state", "year"]}],
        _gate(),
    )
    assert result.checks_passed == 1


def test_unique_key_fail():
    records = [
        {"state": "NY", "year": 2020},
        {"state": "NY", "year": 2020},
        {"state": "CA", "year": 2020},
    ]
    engine = QualityEngine()
    result = engine.run_checks(
        "test",
        records,
        [{"type": "unique_key", "columns": ["state", "year"]}],
        _gate(),
    )
    assert result.checks_failed == 1


def test_row_count():
    engine = QualityEngine()
    result = engine.run_checks(
        "test",
        SAMPLE_RECORDS,
        [{"type": "row_count", "min_rows": 3, "max_rows": 10}],
        _gate(),
    )
    assert result.checks_passed == 1

    result_fail = engine.run_checks(
        "test",
        SAMPLE_RECORDS,
        [{"type": "row_count", "min_rows": 10}],
        _gate(),
    )
    assert result_fail.checks_failed == 1


def test_regex_match():
    records = [
        {"email": "alice@example.com"},
        {"email": "bob@test.org"},
        {"email": "not-an-email"},
    ]
    engine = QualityEngine()
    result = engine.run_checks(
        "test",
        records,
        [{"type": "regex_match", "column": "email", "pattern": r"^[\w.+-]+@[\w-]+\.[\w.]+$"}],
        _gate(),
    )
    assert result.checks_failed == 1


def test_freshness():
    now = datetime.now(timezone.utc)
    records = [
        {"ts": now.isoformat()},
        {"ts": "2020-01-01T00:00:00+00:00"},
    ]
    engine = QualityEngine()
    result = engine.run_checks(
        "test",
        records,
        [{"type": "freshness", "column": "ts", "max_age_hours": 24}],
        _gate(),
    )
    assert result.checks_failed == 1


def test_gate_warn_threshold():
    engine = QualityEngine()
    result = engine.run_checks(
        "test",
        SAMPLE_RECORDS,
        [
            {"type": "not_null", "columns": ["age"]},
            {"type": "not_null", "columns": ["name"]},
            {"type": "not_null", "columns": ["score"]},
        ],
        {"fail_on_critical": False, "warn_threshold": 0.9},
    )
    assert result.pass_rate < 0.9
    assert result.passed_gate is False


def test_non_null_alias_maps_to_not_null():
    """Source YAML uses 'non_null' which should map to NOT_NULL."""
    engine = QualityEngine()
    result = engine.run_checks(
        "test",
        SAMPLE_RECORDS,
        [{"type": "non_null", "fields": ["state"]}],
        _gate(),
    )
    assert result.checks_run == 1
    assert result.checks_passed == 1


def test_multiple_checks_combined():
    engine = QualityEngine()
    result = engine.run_checks(
        "test",
        SAMPLE_RECORDS,
        [
            {"type": "not_null", "columns": ["state"]},
            {"type": "valid_range", "column": "score", "min": 0, "max": 100},
            {"type": "row_count", "min_rows": 1},
            {"type": "unique_key", "columns": ["name"]},
        ],
        _gate(),
    )
    assert result.checks_run == 4
    assert result.checks_passed + result.checks_failed == 4
