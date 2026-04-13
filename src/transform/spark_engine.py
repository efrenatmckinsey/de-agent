"""Apache Spark execution backend for transform specifications (Iceberg-ready)."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

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

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

logger = structlog.get_logger(__name__)


def _spark_type_for_name(name: str):
    from pyspark.sql import types as T

    n = name.strip().lower()
    mapping: dict[str, Any] = {
        "string": T.StringType(),
        "str": T.StringType(),
        "varchar": T.StringType(),
        "int": T.IntegerType(),
        "integer": T.IntegerType(),
        "long": T.LongType(),
        "bigint": T.LongType(),
        "float": T.FloatType(),
        "double": T.DoubleType(),
        "real": T.DoubleType(),
        "boolean": T.BooleanType(),
        "bool": T.BooleanType(),
        "date": T.DateType(),
        "timestamp": T.TimestampType(),
    }
    if n not in mapping:
        raise TransformError(f"Unsupported Spark cast type: {name!r}")
    return mapping[n]


def _read_path_df(spark: SparkSession, path: str) -> DataFrame:
    lower = path.lower()
    if lower.endswith(".csv"):
        return spark.read.option("header", True).csv(path)
    if lower.endswith(".json") or lower.endswith(".jsonl"):
        return spark.read.json(path)
    return spark.read.parquet(path)


def _normalize_join_how(how: str) -> str:
    h = how.lower().replace(" ", "_")
    aliases = {
        "left_outer": "left",
        "right_outer": "right",
        "full": "outer",
        "full_outer": "outer",
        "outer": "outer",
    }
    return aliases.get(h, h)


def _quality_gate_spark(df: DataFrame, checks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    from pyspark.sql import functions as F

    violations: list[dict[str, Any]] = []
    total = df.count()
    for chk in checks:
        ctype = chk.get("type")
        if ctype == "not_null":
            cols = chk.get("columns") or []
            cond = None
            for c in cols:
                piece = F.col(c).isNotNull()
                cond = piece if cond is None else cond & piece
            if cond is not None:
                bad = df.filter(~cond).count()
                if bad:
                    violations.append({"check": "not_null", "columns": cols, "violating_rows": bad})
        elif ctype == "valid_range":
            col = chk.get("column")
            mn, mx = chk.get("min"), chk.get("max")
            c = F.col(col)
            cond = c.isNotNull()
            if mn is not None:
                cond = cond & (c >= float(mn))
            if mx is not None:
                cond = cond & (c <= float(mx))
            bad = df.filter(c.isNotNull() & ~cond).count()
            if bad:
                violations.append({"check": "valid_range", "column": col, "violating_rows": bad})
        elif ctype == "unique_key":
            cols = chk.get("columns") or []
            if not cols:
                continue
            dup = df.groupBy(*cols).count().filter(F.col("count") > 1).count()
            if dup:
                violations.append({"check": "unique_key", "columns": cols, "duplicate_key_groups": dup})
        elif ctype == "row_count":
            mn = int(chk.get("min", 0))
            mx = chk.get("max")
            if total < mn:
                violations.append({"check": "row_count", "detail": f"row_count {total} < min {mn}"})
            if mx is not None and total > int(mx):
                violations.append({"check": "row_count", "detail": f"row_count {total} > max {mx}"})
    return violations


class SparkTransformEngine:
    """Run ``TransformSpec`` on Spark with lazy session creation."""

    def __init__(self, app_name: str = "hydra-transform") -> None:
        self._app_name = app_name
        self._spark: SparkSession | None = None
        self._log = logger.bind(component="spark_transform", app_name=app_name)

    def get_or_create_session(self) -> SparkSession:
        if self._spark is None:
            try:
                from pyspark.sql import SparkSession
            except ImportError as e:
                raise TransformError("PySpark is not installed or not on PYTHONPATH") from e

            builder = (
                SparkSession.builder.appName(self._app_name)
                .config(
                    "spark.sql.extensions",
                    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                )
                .config("spark.sql.session.timeZone", "UTC")
            )
            catalog_name = os.environ.get("HYDRA_ICEBERG_CATALOG_NAME", "iceberg")
            warehouse = os.environ.get("HYDRA_ICEBERG_WAREHOUSE", "").strip()
            catalog_class = os.environ.get(
                "HYDRA_ICEBERG_CATALOG_CLASS",
                "org.apache.iceberg.spark.SparkCatalog",
            )
            if warehouse:
                builder = builder.config(f"spark.sql.catalog.{catalog_name}", catalog_class).config(
                    f"spark.sql.catalog.{catalog_name}.warehouse",
                    warehouse,
                )
                cat_impl = os.environ.get("HYDRA_ICEBERG_CATALOG_IMPL", "").strip()
                if cat_impl:
                    builder = builder.config(f"spark.sql.catalog.{catalog_name}.catalog-impl", cat_impl)
                builder = builder.config("spark.sql.defaultCatalog", catalog_name)
            try:
                self._spark = builder.getOrCreate()
            except Exception as e:
                self._log.exception("spark.session_init_failed")
                raise TransformError(f"Failed to initialize SparkSession: {e}") from e
            self._log.info("spark.session_ready", master=self._spark.sparkContext.master)
        return self._spark

    def execute(self, spec: TransformSpec, input_data: list[dict] | str) -> TransformResult:
        from pyspark.sql import Row
        from pyspark.sql.types import StructType

        spark = self.get_or_create_session()
        started = datetime.now(timezone.utc)
        violations: list[dict[str, Any]] = []
        output_path: str | None = None

        try:
            if isinstance(input_data, list):
                if not input_data:
                    df = spark.createDataFrame([], schema=StructType([]))
                    rows_in = 0
                else:
                    rows_in = len(input_data)
                    df = spark.createDataFrame([Row(**r) for r in input_data])
            elif isinstance(input_data, str):
                df = _read_path_df(spark, input_data)
                rows_in = df.count()
            else:
                raise TransformError("input_data must be list[dict] or path str")

            for step in spec.steps:
                df = self._apply_step(spark, df, step, violations)

            rows_out = df.count()
        except TransformError:
            raise
        except Exception as e:
            self._log.exception("spark.execute.failed", transform_id=spec.transform_id)
            raise TransformError(f"Spark transform failed: {e}") from e

        completed = datetime.now(timezone.utc)
        self._log.info(
            "spark.execute.completed",
            transform_id=spec.transform_id,
            rows_in=rows_in,
            rows_out=rows_out,
        )
        return TransformResult(
            transform_id=spec.transform_id,
            engine_used="spark",
            rows_in=rows_in,
            rows_out=rows_out,
            started_at=started,
            completed_at=completed,
            quality_violations=violations,
            output_path=output_path,
        )

    def _apply_step(
        self,
        spark: SparkSession,
        df: DataFrame,
        step: TransformStepUnion,
        violations_acc: list[dict[str, Any]],
    ) -> DataFrame:
        from pyspark.sql import functions as F

        if isinstance(step, FilterStep):
            return df.filter(F.expr(step.condition))
        if isinstance(step, FlattenStep):
            tmp = df.withColumn("__expl", F.explode(F.col(step.source_column))).drop(step.source_column)
            expl = F.col("__expl")
            if len(step.output_columns) == 1:
                return tmp.withColumn(step.output_columns[0], expl).drop("__expl")
            out = tmp
            for c in step.output_columns:
                out = out.withColumn(c, expl.getField(c))
            return out.drop("__expl")
        if isinstance(step, AggregateStep):
            group_cols = [F.col(c) for c in step.group_by]
            agg_exprs = []
            for m in step.measures:
                c = F.col(m.column)
                fn = m.function
                if fn == "sum":
                    agg_exprs.append(F.sum(c).alias(m.alias))
                elif fn == "avg":
                    agg_exprs.append(F.avg(c).alias(m.alias))
                elif fn == "min":
                    agg_exprs.append(F.min(c).alias(m.alias))
                elif fn == "max":
                    agg_exprs.append(F.max(c).alias(m.alias))
                elif fn == "count":
                    agg_exprs.append(F.count(c).alias(m.alias))
                else:
                    raise TransformError(f"Unsupported Spark aggregate {fn}")
            if step.group_by:
                return df.groupBy(*group_cols).agg(*agg_exprs)
            return df.agg(*agg_exprs)
        if isinstance(step, DeriveStep):
            return df.withColumn(step.new_column, F.expr(step.expression))
        if isinstance(step, RenameStep):
            out = df
            for old, new in step.mappings.items():
                out = out.withColumnRenamed(old, new)
            return out
        if isinstance(step, CastStep):
            out = df
            for col_name, typ in step.type_mappings.items():
                st = _spark_type_for_name(typ)
                out = out.withColumn(col_name, F.col(col_name).cast(st))
            return out
        if isinstance(step, QualityGateStep):
            v = _quality_gate_spark(df, step.checks)
            if v:
                violations_acc.extend(v)
                self._log.warning("spark.quality_gate", violations=v)
            return df
        if isinstance(step, JoinStep):
            right = _read_path_df(spark, step.right_source)
            how = _normalize_join_how(step.how)
            return df.join(right, on=step.on, how=how)
        raise TransformError(f"Unsupported Spark step: {type(step).__name__}")

    def stop(self) -> None:
        if self._spark is not None:
            try:
                self._spark.stop()
            except Exception as e:
                self._log.warning("spark.stop_failed", error=str(e))
            finally:
                self._spark = None
