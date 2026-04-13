"""Declarative static data quality checks."""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any

import structlog
from pydantic import BaseModel, Field

logger = structlog.get_logger(__name__)


class QualityCheckType(str, Enum):
    NOT_NULL = "not_null"
    VALID_RANGE = "valid_range"
    UNIQUE_KEY = "unique_key"
    ROW_COUNT = "row_count"
    REGEX_MATCH = "regex_match"
    REFERENTIAL = "referential"
    FRESHNESS = "freshness"
    CUSTOM_SQL = "custom_sql"


class QualityCheck(BaseModel):
    check_type: QualityCheckType
    columns: list[str] | None = None
    params: dict[str, Any] = Field(default_factory=dict)


class QualityViolation(BaseModel):
    check_type: QualityCheckType
    column: str | None = None
    message: str
    severity: str  # "critical" | "warning"
    failed_count: int = 0
    sample_values: list[Any] = Field(default_factory=list)


class QualityResult(BaseModel):
    source_id: str
    checks_run: int = 0
    checks_passed: int = 0
    checks_failed: int = 0
    violations: list[QualityViolation] = Field(default_factory=list)
    pass_rate: float = 1.0
    passed_gate: bool = True
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class QualityEngine:
    """Evaluate row-oriented checks against in-memory record batches."""

    @staticmethod
    def parse_checks_from_config(quality_checks_config: list[dict[str, Any]]) -> list[QualityCheck]:
        out: list[QualityCheck] = []
        for raw in quality_checks_config:
            try:
                ctype_raw = str(raw.get("type") or raw.get("check_type") or "").lower()
                # allow YAML style "non_null" alias
                if ctype_raw == "non_null":
                    ctype_raw = QualityCheckType.NOT_NULL.value
                check_type = QualityCheckType(ctype_raw)
            except ValueError:
                logger.warning("quality.unknown_check_type", config=raw)
                continue
            cols = raw.get("columns") or raw.get("fields")
            if isinstance(cols, str):
                columns = [cols]
            elif isinstance(cols, list):
                columns = [str(c) for c in cols]
            else:
                column = raw.get("column") or raw.get("field")
                columns = [str(column)] if column else None
            params = {k: v for k, v in raw.items() if k not in ("type", "check_type", "columns", "fields", "column", "field")}
            out.append(QualityCheck(check_type=check_type, columns=columns, params=params))
        return out

    def run_checks(
        self,
        source_id: str,
        records: list[dict[str, Any]],
        checks_config: list[dict[str, Any]],
        gate_config: dict[str, Any],
    ) -> QualityResult:
        checks = self.parse_checks_from_config(checks_config)
        violations: list[QualityViolation] = []
        checks_run = len(checks)
        checks_failed = 0

        for chk in checks:
            vlist: list[QualityViolation] = []
            try:
                vlist = self._dispatch_check(records, chk)
            except Exception as exc:
                logger.exception("quality.check_error", check_type=chk.check_type.value, error=str(exc))
                sev = str(chk.params.get("severity", "critical")).lower()
                vlist = [
                    QualityViolation(
                        check_type=chk.check_type,
                        message=f"Check execution error: {exc}",
                        severity="critical" if sev == "critical" else "warning",
                        failed_count=len(records),
                    )
                ]
            if vlist:
                checks_failed += 1
                violations.extend(vlist)
            else:
                pass  # check passed

        checks_passed = checks_run - checks_failed
        pass_rate = (checks_passed / checks_run) if checks_run > 0 else 1.0

        fail_on_critical = bool(gate_config.get("fail_on_critical", True))
        warn_threshold = float(gate_config.get("warn_threshold", 0.95))
        has_critical = any(v.severity == "critical" for v in violations)
        passed_gate = True
        if fail_on_critical and has_critical:
            passed_gate = False
        if pass_rate < warn_threshold:
            passed_gate = False

        return QualityResult(
            source_id=source_id,
            checks_run=checks_run,
            checks_passed=checks_passed,
            checks_failed=checks_failed,
            violations=violations,
            pass_rate=round(pass_rate, 6),
            passed_gate=passed_gate,
        )

    def _dispatch_check(self, records: list[dict[str, Any]], chk: QualityCheck) -> list[QualityViolation]:
        ct = chk.check_type
        cols = chk.columns or []
        p = chk.params

        if ct == QualityCheckType.NOT_NULL:
            return self._check_not_null(records, cols or list(p.get("fields") or []))
        if ct == QualityCheckType.VALID_RANGE:
            col = (cols[0] if cols else None) or p.get("field") or p.get("column")
            if not col:
                return []
            return self._check_valid_range(
                records,
                str(col),
                p.get("min"),
                p.get("max"),
                str(p.get("severity", "warning")),
            )
        if ct == QualityCheckType.UNIQUE_KEY:
            key_cols = cols or list(p.get("columns") or p.get("fields") or [])
            return self._check_unique_key(records, [str(c) for c in key_cols], str(p.get("severity", "critical")))
        if ct == QualityCheckType.ROW_COUNT:
            return self._check_row_count(
                records,
                int(p.get("min_rows", 0)),
                p.get("max_rows"),
                str(p.get("severity", "critical")),
            )
        if ct == QualityCheckType.REGEX_MATCH:
            col = (cols[0] if cols else None) or p.get("field") or p.get("column")
            pattern = p.get("pattern") or p.get("regex")
            if not col or not pattern:
                logger.warning("quality.regex_missing_params", check=chk.model_dump())
                return []
            return self._check_regex_match(records, str(col), str(pattern), str(p.get("severity", "warning")))
        if ct == QualityCheckType.FRESHNESS:
            col = (cols[0] if cols else None) or p.get("field") or p.get("column")
            max_age = p.get("max_age_hours")
            if not col or max_age is None:
                logger.warning("quality.freshness_missing_params", check=chk.model_dump())
                return []
            return self._check_freshness(
                records,
                str(col),
                float(max_age),
                str(p.get("severity", "warning")),
            )
        if ct == QualityCheckType.REFERENTIAL:
            return self._check_referential(records, p, str(p.get("severity", "critical")))
        if ct == QualityCheckType.CUSTOM_SQL:
            logger.warning(
                "quality.custom_sql_not_supported",
                message="CUSTOM_SQL checks require an external SQL engine; skipping.",
            )
            return []
        return []

    def _check_not_null(self, records: list[dict[str, Any]], columns: list[str]) -> list[QualityViolation]:
        if not columns:
            return []
        violations: list[QualityViolation] = []
        for col in columns:
            failed = sum(1 for row in records if row.get(col) is None or (isinstance(row.get(col), str) and not str(row.get(col)).strip()))
            if failed:
                samples = [row.get(col) for row in records if row.get(col) is None][:5]
                violations.append(
                    QualityViolation(
                        check_type=QualityCheckType.NOT_NULL,
                        column=col,
                        message=f"NOT_NULL violated for {col!r}",
                        severity="critical",
                        failed_count=failed,
                        sample_values=samples,
                    )
                )
        return violations

    def _check_valid_range(
        self,
        records: list[dict[str, Any]],
        column: str,
        min_val: Any,
        max_val: Any,
        severity: str,
    ) -> list[QualityViolation]:
        failed = 0
        samples: list[Any] = []
        for row in records:
            val = row.get(column)
            if val is None:
                continue
            try:
                num = float(val)
            except (TypeError, ValueError):
                failed += 1
                if len(samples) < 5:
                    samples.append(val)
                continue
            if min_val is not None and num < float(min_val):
                failed += 1
                if len(samples) < 5:
                    samples.append(val)
            if max_val is not None and num > float(max_val):
                failed += 1
                if len(samples) < 5:
                    samples.append(val)
        if not failed:
            return []
        return [
            QualityViolation(
                check_type=QualityCheckType.VALID_RANGE,
                column=column,
                message=f"VALID_RANGE violated for {column!r}",
                severity=severity if severity in ("critical", "warning") else "warning",
                failed_count=failed,
                sample_values=samples,
            )
        ]

    def _check_unique_key(self, records: list[dict[str, Any]], columns: list[str], severity: str) -> list[QualityViolation]:
        if not columns:
            return []
        seen: dict[tuple[Any, ...], int] = {}
        dup_count = 0
        sample_dup: list[Any] = []
        for row in records:
            key = tuple(row.get(c) for c in columns)
            if None in key and any(v is None for v in key):
                continue
            seen[key] = seen.get(key, 0) + 1
        for key, cnt in seen.items():
            if cnt > 1:
                dup_count += cnt - 1
                if len(sample_dup) < 5:
                    sample_dup.append(dict(zip(columns, key, strict=True)))
        if dup_count == 0:
            return []
        return [
            QualityViolation(
                check_type=QualityCheckType.UNIQUE_KEY,
                column=",".join(columns),
                message=f"UNIQUE_KEY violated on {columns!r}",
                severity=severity if severity in ("critical", "warning") else "critical",
                failed_count=dup_count,
                sample_values=sample_dup,
            )
        ]

    def _check_row_count(
        self,
        records: list[dict[str, Any]],
        min_rows: int,
        max_rows: Any,
        severity: str,
    ) -> list[QualityViolation]:
        n = len(records)
        ok = n >= min_rows
        if max_rows is not None:
            ok = ok and n <= int(max_rows)
        if ok:
            return []
        msg = f"ROW_COUNT {n} outside bounds min_rows={min_rows}"
        if max_rows is not None:
            msg += f", max_rows={max_rows}"
        return [
            QualityViolation(
                check_type=QualityCheckType.ROW_COUNT,
                column=None,
                message=msg,
                severity=severity if severity in ("critical", "warning") else "critical",
                failed_count=1,
                sample_values=[n],
            )
        ]

    def _check_regex_match(
        self,
        records: list[dict[str, Any]],
        column: str,
        pattern: str,
        severity: str,
    ) -> list[QualityViolation]:
        try:
            rx = re.compile(pattern)
        except re.error as e:
            logger.error("quality.invalid_regex", pattern=pattern, error=str(e))
            return [
                QualityViolation(
                    check_type=QualityCheckType.REGEX_MATCH,
                    column=column,
                    message=f"Invalid regex: {e}",
                    severity="critical",
                    failed_count=0,
                )
            ]
        failed = 0
        samples: list[Any] = []
        for row in records:
            val = row.get(column)
            if val is None:
                continue
            s = str(val)
            if not rx.search(s):
                failed += 1
                if len(samples) < 5:
                    samples.append(val)
        if not failed:
            return []
        return [
            QualityViolation(
                check_type=QualityCheckType.REGEX_MATCH,
                column=column,
                message=f"REGEX_MATCH violated for {column!r}",
                severity=severity if severity in ("critical", "warning") else "warning",
                failed_count=failed,
                sample_values=samples,
            )
        ]

    def _check_freshness(
        self,
        records: list[dict[str, Any]],
        column: str,
        max_age_hours: float,
        severity: str,
    ) -> list[QualityViolation]:
        now = datetime.now(timezone.utc)
        threshold = now - timedelta(hours=max_age_hours)
        failed = 0
        samples: list[Any] = []

        def _parse_dt(val: Any) -> datetime | None:
            if isinstance(val, datetime):
                return val if val.tzinfo else val.replace(tzinfo=timezone.utc)
            if isinstance(val, (int, float)):
                return datetime.fromtimestamp(float(val), tz=timezone.utc)
            if isinstance(val, str):
                s = val.strip()
                if not s:
                    return None
                try:
                    if s.endswith("Z"):
                        s = s[:-1] + "+00:00"
                    return datetime.fromisoformat(s)
                except ValueError:
                    return None
            return None

        for row in records:
            val = row.get(column)
            if val is None:
                continue
            dt = _parse_dt(val)
            if dt is None:
                failed += 1
                if len(samples) < 5:
                    samples.append(val)
                continue
            if dt < threshold:
                failed += 1
                if len(samples) < 5:
                    samples.append(val)
        if not failed:
            return []
        return [
            QualityViolation(
                check_type=QualityCheckType.FRESHNESS,
                column=column,
                message=f"FRESHNESS: values older than {max_age_hours}h in {column!r}",
                severity=severity if severity in ("critical", "warning") else "warning",
                failed_count=failed,
                sample_values=samples,
            )
        ]

    def _check_referential(self, records: list[dict[str, Any]], params: dict[str, Any], severity: str) -> list[QualityViolation]:
        child_col = params.get("child_column") or params.get("fk_column")
        parent_col = params.get("parent_column") or params.get("pk_column")
        if not child_col or not parent_col:
            logger.warning("quality.referential_missing_columns", params=params)
            return []
        parent_values = {row.get(parent_col) for row in records if row.get(parent_col) is not None}
        failed = 0
        samples: list[Any] = []
        for row in records:
            c = row.get(child_col)
            if c is None:
                continue
            if c not in parent_values:
                failed += 1
                if len(samples) < 5:
                    samples.append(c)
        if not failed:
            return []
        return [
            QualityViolation(
                check_type=QualityCheckType.REFERENTIAL,
                column=str(child_col),
                message=f"REFERENTIAL: {child_col!r} not in {parent_col!r} domain",
                severity=severity if severity in ("critical", "warning") else "critical",
                failed_count=failed,
                sample_values=samples,
            )
        ]
