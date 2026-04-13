"""Lightweight transform execution over in-memory row dicts."""

from __future__ import annotations

import ast
import json
import operator
import re
from collections import defaultdict
from collections.abc import Callable
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import structlog

from src.core.exceptions import TransformError
from src.transform.dsl_parser import (
    AggregateStep,
    CastStep,
    DeriveStep,
    FilterStep,
    FlattenStep,
    JoinStep,
    QualityGateStep,
    RenameStep,
    TransformSpec,
    TransformStep as TransformStepUnion,
)
from src.transform.models import TransformResult

logger = structlog.get_logger(__name__)

_CASE_HEADER = re.compile(r"^\s*CASE\s+", re.IGNORECASE)
_CASE_END = re.compile(r"\s+END\s*$", re.IGNORECASE)


class ExpressionEvaluator:
    """Basic SQL-like expressions for filter conditions and derive (incl. CASE WHEN)."""

    _CMP_OPS: dict[type, Callable[[Any, Any], bool]] = {
        ast.Eq: lambda a, b: a == b,
        ast.NotEq: lambda a, b: a != b,
        ast.Lt: lambda a, b: a < b,
        ast.LtE: lambda a, b: a <= b,
        ast.Gt: lambda a, b: a > b,
        ast.GtE: lambda a, b: a >= b,
    }

    def eval_filter(self, expr: str, row: dict[str, Any]) -> bool:
        return self._eval_bool_expr(expr.strip(), row)

    def eval_derive(self, expr: str, row: dict[str, Any]) -> Any:
        s = expr.strip()
        if _CASE_HEADER.search(s) and _CASE_END.search(s):
            return self._eval_case_when(s, row)
        return self._eval_arithmetic(s, row)

    def _eval_bool_expr(self, expr: str, row: dict[str, Any]) -> bool:
        """Parse OR of AND clauses without full SQL support."""
        if not expr:
            return True
        parts = self._split_top_level(expr, " OR ")
        return any(self._eval_and_block(p.strip(), row) for p in parts if p.strip())

    def _eval_and_block(self, expr: str, row: dict[str, Any]) -> bool:
        parts = self._split_top_level(expr, " AND ")
        return all(self._eval_atom(p.strip(), row) for p in parts if p.strip())

    def _split_top_level(self, expr: str, sep: str) -> list[str]:
        out: list[str] = []
        buf: list[str] = []
        depth = 0
        i = 0
        while i < len(expr):
            if expr[i] == "(":
                depth += 1
                buf.append(expr[i])
                i += 1
                continue
            if expr[i] == ")":
                depth = max(0, depth - 1)
                buf.append(expr[i])
                i += 1
                continue
            if depth == 0 and expr[i : i + len(sep)].upper() == sep.upper():
                out.append("".join(buf))
                buf = []
                i += len(sep)
                continue
            buf.append(expr[i])
            i += 1
        out.append("".join(buf))
        return out

    def _eval_atom(self, atom: str, row: dict[str, Any]) -> bool:
        s = atom.strip()
        if s.startswith("(") and s.endswith(")"):
            return self._eval_bool_expr(s[1:-1].strip(), row)
        up = s.upper()
        if " IS NOT NULL" in up:
            col = s[: up.index(" IS NOT NULL")].strip()
            return self._row_get(row, col) is not None
        if " IS NULL" in up:
            col = s[: up.index(" IS NULL")].strip()
            return self._row_get(row, col) is None
        m = re.match(
            r"^(.+?)\s*(=|!=|<>|>=|<=|>|<)\s*(.+)$",
            s,
        )
        if not m:
            raise TransformError(f"Unsupported filter atom: {s!r}")
        left_s, op, right_s = m.group(1).strip(), m.group(2), m.group(3).strip()
        left = self._parse_value(left_s, row)
        right = self._parse_value(right_s, row)
        if op == "=":
            return left == right
        if op in ("!=", "<>"):
            return left != right
        if op == ">":
            return left > right
        if op == "<":
            return left < right
        if op == ">=":
            return left >= right
        if op == "<=":
            return left <= right
        raise TransformError(f"Unsupported operator {op}")

    def _row_get(self, row: dict[str, Any], key: str) -> Any:
        if key.startswith('"') and key.endswith('"'):
            key = key[1:-1]
        elif key.startswith("'") and key.endswith("'"):
            key = key[1:-1]
        return row.get(key)

    def _parse_value(self, token: str, row: dict[str, Any]) -> Any:
        t = token.strip()
        if (t.startswith("'") and t.endswith("'")) or (t.startswith('"') and t.endswith('"')):
            return t[1:-1]
        low = t.lower()
        if low == "null":
            return None
        if low == "true":
            return True
        if low == "false":
            return False
        if re.fullmatch(r"-?\d+", t):
            return int(t)
        if re.fullmatch(r"-?\d+\.\d+([eE][+-]?\d+)?", t):
            return float(t)
        return self._row_get(row, t)

    def _eval_case_when(self, expr: str, row: dict[str, Any]) -> Any:
        body = _CASE_HEADER.sub("", expr)
        body = _CASE_END.sub("", body).strip()
        else_val: Any = None
        if re.search(r"\bELSE\b", body, re.IGNORECASE):
            parts = re.split(r"\bELSE\b", body, maxsplit=1, flags=re.IGNORECASE)
            main, else_part = parts[0], parts[1]
            else_val = self.eval_derive(else_part.strip(), row)
            body = main.strip()
        when_pattern = re.compile(
            r"WHEN\s+(.+?)\s+THEN\s+(.+?)(?=\s+WHEN\s+|\s*$)",
            re.IGNORECASE | re.DOTALL,
        )
        for m in when_pattern.finditer(body):
            cond, then_expr = m.group(1).strip(), m.group(2).strip()
            if self._eval_bool_expr(cond, row):
                return self.eval_derive(then_expr, row)
        return else_val

    def _eval_arithmetic(self, expr: str, row: dict[str, Any]) -> Any:
        node = ast.parse(expr, mode="eval")

        def _eval(n: ast.AST) -> Any:
            if isinstance(n, ast.Constant):
                return n.value
            if isinstance(n, ast.Name):
                if n.id in row:
                    return row[n.id]
                raise TransformError(f"Unknown column in expression: {n.id}")
            if isinstance(n, ast.BinOp) and type(n.op) in self._binops_map():
                return self._binops_map()[type(n.op)](_eval(n.left), _eval(n.right))
            if isinstance(n, ast.UnaryOp) and isinstance(n.op, ast.USub):
                return -_eval(n.operand)
            if isinstance(n, ast.UnaryOp) and isinstance(n.op, ast.UAdd):
                return +_eval(n.operand)
            raise TransformError(f"Unsupported derive expression fragment: {ast.dump(n)}")

        return _eval(node.body)

    @staticmethod
    def _binops_map() -> dict[type, Callable[[Any, Any], Any]]:
        return {
            ast.Add: operator.add,
            ast.Sub: operator.sub,
            ast.Mult: operator.mul,
            ast.Div: operator.truediv,
            ast.FloorDiv: operator.floordiv,
            ast.Mod: operator.mod,
            ast.Pow: operator.pow,
        }


def _run_quality_gate_python(rows: list[dict[str, Any]], checks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    violations: list[dict[str, Any]] = []
    n = len(rows)
    for chk in checks:
        ctype = chk.get("type")
        if ctype == "not_null":
            cols = chk.get("columns") or []
            bad = 0
            for r in rows:
                if any(r.get(c) is None for c in cols):
                    bad += 1
            if bad:
                violations.append({"check": "not_null", "columns": cols, "violating_rows": bad})
        elif ctype == "valid_range":
            col = chk.get("column")
            mn, mx = chk.get("min"), chk.get("max")
            bad = 0
            for r in rows:
                v = r.get(col)
                if v is None:
                    continue
                try:
                    fv = float(v)
                except (TypeError, ValueError):
                    bad += 1
                    continue
                if mn is not None and fv < float(mn):
                    bad += 1
                elif mx is not None and fv > float(mx):
                    bad += 1
            if bad:
                violations.append({"check": "valid_range", "column": col, "violating_rows": bad})
        elif ctype == "unique_key":
            cols = chk.get("columns") or []
            seen: dict[tuple[Any, ...], int] = defaultdict(int)
            for r in rows:
                key = tuple(r.get(c) for c in cols)
                seen[key] += 1
            dups = sum(1 for _key, cnt in seen.items() if cnt > 1)
            if dups:
                violations.append({"check": "unique_key", "columns": cols, "duplicate_key_groups": dups})
        elif ctype == "row_count":
            mn = chk.get("min", 0)
            mx = chk.get("max")
            if n < int(mn):
                violations.append({"check": "row_count", "detail": f"row_count {n} < min {mn}"})
            if mx is not None and n > int(mx):
                violations.append({"check": "row_count", "detail": f"row_count {n} > max {mx}"})
    return violations


def _cast_value(val: Any, target: str) -> Any:
    t = target.strip().lower()
    if val is None:
        return None
    if t in ("str", "string", "varchar"):
        return str(val)
    if t in ("int", "integer", "long"):
        return int(val)
    if t in ("float", "double", "real"):
        return float(val)
    if t in ("bool", "boolean"):
        if isinstance(val, bool):
            return val
        if isinstance(val, str):
            return val.lower() in ("1", "true", "yes", "y")
        return bool(val)
    raise TransformError(f"Unsupported cast target type: {target!r}")


def _apply_filter_python(rows: list[dict[str, Any]], step: FilterStep) -> list[dict[str, Any]]:
    ev = ExpressionEvaluator()
    return [r for r in rows if ev.eval_filter(step.condition, r)]


def _apply_flatten_python(rows: list[dict[str, Any]], step: FlattenStep) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in rows:
        nested = row.get(step.source_column)
        base = {k: v for k, v in row.items() if k != step.source_column}
        if not isinstance(nested, list):
            continue
        for elem in nested:
            new_row = dict(base)
            if isinstance(elem, dict):
                for c in step.output_columns:
                    new_row[c] = elem.get(c)
            elif isinstance(elem, (list, tuple)):
                for i, c in enumerate(step.output_columns):
                    new_row[c] = elem[i] if i < len(elem) else None
            else:
                if len(step.output_columns) == 1:
                    new_row[step.output_columns[0]] = elem
                else:
                    raise TransformError("flatten output_columns length must be 1 for scalar elements")
            out.append(new_row)
    return out


def _apply_aggregate_python(rows: list[dict[str, Any]], step: AggregateStep) -> list[dict[str, Any]]:
    groups: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for r in rows:
        key = tuple(r.get(c) for c in step.group_by)
        groups[key].append(r)

    out: list[dict[str, Any]] = []
    for key, grp in groups.items():
        row: dict[str, Any] = {g: key[i] for i, g in enumerate(step.group_by)}
        for m in step.measures:
            col = m.column
            fn = m.function
            vals = [x.get(col) for x in grp if x.get(col) is not None]
            if fn == "count":
                if not col or col == "*":
                    row[m.alias] = len(grp)
                else:
                    row[m.alias] = sum(1 for x in grp if x.get(col) is not None)
            elif not vals:
                row[m.alias] = None
            elif fn == "sum":
                row[m.alias] = sum(float(v) for v in vals)
            elif fn == "avg":
                row[m.alias] = sum(float(v) for v in vals) / len(vals)
            elif fn == "min":
                row[m.alias] = min(vals)
            elif fn == "max":
                row[m.alias] = max(vals)
            else:
                raise TransformError(f"Unsupported aggregate {fn}")
        out.append(row)
    return out


def _apply_derive_python(rows: list[dict[str, Any]], step: DeriveStep) -> list[dict[str, Any]]:
    ev = ExpressionEvaluator()
    out: list[dict[str, Any]] = []
    for r in rows:
        nr = dict(r)
        nr[step.new_column] = ev.eval_derive(step.expression, r)
        out.append(nr)
    return out


def _apply_rename_python(rows: list[dict[str, Any]], step: RenameStep) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for r in rows:
        nr = dict(r)
        for old, new in step.mappings.items():
            if old in nr:
                nr[new] = nr.pop(old)
        out.append(nr)
    return out


def _apply_cast_python(rows: list[dict[str, Any]], step: CastStep) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for r in rows:
        nr = dict(r)
        for col, typ in step.type_mappings.items():
            if col in nr:
                nr[col] = _cast_value(nr[col], typ)
        out.append(nr)
    return out


def _load_join_side(path_str: str) -> list[dict[str, Any]]:
    p = Path(path_str)
    if not p.is_file():
        raise TransformError(f"join right_source path not found: {path_str}")
    text = p.read_text(encoding="utf-8")
    if p.suffix.lower() == ".json":
        data = json.loads(text)
        if not isinstance(data, list):
            raise TransformError("join JSON right_source must be a JSON array of objects")
        return [dict(x) for x in data]
    if p.suffix.lower() in (".jsonl", ".ndjson"):
        lines = [ln for ln in text.splitlines() if ln.strip()]
        return [dict(json.loads(ln)) for ln in lines]
    raise TransformError(f"Unsupported join right_source format: {p.suffix}")


def _apply_join_python(
    left: list[dict[str, Any]],
    step: JoinStep,
) -> list[dict[str, Any]]:
    right = _load_join_side(step.right_source)
    how = step.how.lower().replace(" ", "_")
    keys = step.on
    out: list[dict[str, Any]] = []

    def key_row(r: dict[str, Any]) -> tuple[Any, ...]:
        return tuple(r.get(k) for k in keys)

    if how in ("inner", ""):
        r_index: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
        for r in right:
            r_index[key_row(r)].append(r)
        for l in left:
            k = key_row(l)
            for rr in r_index.get(k, []):
                merged = {**l, **rr}
                out.append(merged)
        return out
    if how == "left":
        r_index = defaultdict(list)
        for r in right:
            r_index[key_row(r)].append(r)
        for l in left:
            k = key_row(l)
            matches = r_index.get(k, [])
            if not matches:
                out.append(dict(l))
            else:
                for rr in matches:
                    out.append({**l, **rr})
        return out
    raise TransformError(f"Python engine join how={step.how!r} is not implemented (use inner or left)")


class PythonTransformEngine:
    """Execute ``TransformSpec`` steps on ``list[dict]`` rows."""

    def execute(self, spec: TransformSpec, input_data: list[dict[str, Any]]) -> TransformResult:
        log = logger.bind(transform_id=spec.transform_id, engine="python")
        started = datetime.now(timezone.utc)
        rows_in = len(input_data)
        rows: list[dict[str, Any]] = [deepcopy(r) for r in input_data]
        all_violations: list[dict[str, Any]] = []

        try:
            for step in spec.steps:
                rows = self._apply_step(rows, step, all_violations, log)
        except TransformError:
            raise
        except Exception as e:
            log.exception("python_engine.step_failed")
            raise TransformError(f"Python transform failed: {e}") from e

        completed = datetime.now(timezone.utc)
        log.info(
            "python_engine.completed",
            rows_in=rows_in,
            rows_out=len(rows),
            violations=len(all_violations),
        )
        return TransformResult(
            transform_id=spec.transform_id,
            engine_used="python",
            rows_in=rows_in,
            rows_out=len(rows),
            started_at=started,
            completed_at=completed,
            quality_violations=all_violations,
        )

    def _apply_step(
        self,
        rows: list[dict[str, Any]],
        step: TransformStepUnion,
        violations_acc: list[dict[str, Any]],
        log: Any,
    ) -> list[dict[str, Any]]:
        if isinstance(step, FilterStep):
            return _apply_filter_python(rows, step)
        if isinstance(step, FlattenStep):
            return _apply_flatten_python(rows, step)
        if isinstance(step, AggregateStep):
            return _apply_aggregate_python(rows, step)
        if isinstance(step, DeriveStep):
            return _apply_derive_python(rows, step)
        if isinstance(step, RenameStep):
            return _apply_rename_python(rows, step)
        if isinstance(step, CastStep):
            return _apply_cast_python(rows, step)
        if isinstance(step, QualityGateStep):
            v = _run_quality_gate_python(rows, step.checks)
            if v:
                violations_acc.extend(v)
                log.warning("python_engine.quality_gate", violations=v)
            return rows
        if isinstance(step, JoinStep):
            return _apply_join_python(rows, step)
        raise TransformError(f"Unsupported step type: {type(step).__name__}")
