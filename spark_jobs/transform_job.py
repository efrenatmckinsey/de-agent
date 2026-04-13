"""Spark submit entrypoint for running hydra-ingest transform specs.

Usage:
    spark-submit spark_jobs/transform_job.py \
        --transform-spec config/transforms/food_enrichment.yaml \
        --input-path s3://hydra-data-lake/raw/usda_food_data/2024-01-01/ \
        --output-path s3://hydra-data-lake/curated/food_nutrients/
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

from pyspark.sql import SparkSession


def _build_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.hydra", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.hydra.type", "hadoop")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )


def _apply_steps(spark: SparkSession, df, steps: list[dict]) -> tuple:
    """Apply DSL steps sequentially and collect quality violations."""
    from pyspark.sql import functions as F

    violations: list[dict] = []
    rows_in = df.count()

    for step in steps:
        stype = step.get("type", "")

        if stype == "filter":
            condition = step.get("condition") or step.get("where", "")
            if condition:
                df = df.filter(condition)

        elif stype == "flatten":
            src_col = step.get("source_column") or step.get("column")
            out_cols = step.get("output_columns") or step.get("into", [])
            if src_col:
                df = df.withColumn(src_col, F.explode_outer(F.col(src_col)))
                for oc in out_cols:
                    if oc != src_col:
                        df = df.withColumn(oc, F.col(f"{src_col}.{oc}"))

        elif stype == "aggregate":
            group_by = step.get("group_by", [])
            measures = step.get("measures", [])
            agg_exprs = []
            for m in measures:
                col_name = m["column"]
                func = m["function"].lower()
                alias = m["alias"]
                fn_map = {"sum": F.sum, "avg": F.avg, "min": F.min, "max": F.max, "count": F.count}
                if func in fn_map:
                    agg_exprs.append(fn_map[func](F.col(col_name)).alias(alias))
            if group_by and agg_exprs:
                df = df.groupBy(*group_by).agg(*agg_exprs)

        elif stype == "derive":
            new_col = step.get("new_column", "derived")
            expr = step.get("expression", "NULL")
            df = df.withColumn(new_col, F.expr(expr))

        elif stype == "rename":
            for old_name, new_name in (step.get("mappings") or {}).items():
                df = df.withColumnRenamed(old_name, new_name)

        elif stype == "cast":
            for col_name, dtype in (step.get("type_mappings") or step.get("columns") or {}).items():
                df = df.withColumn(col_name, F.col(col_name).cast(dtype))

        elif stype == "quality_gate":
            for chk in step.get("checks", []):
                ct = chk.get("type", "")
                if ct == "not_null":
                    for c in chk.get("columns", []):
                        null_count = df.filter(F.col(c).isNull()).count()
                        if null_count > 0:
                            violations.append({"type": "not_null", "column": c, "failed": null_count})
                elif ct == "valid_range":
                    c = chk.get("column", "")
                    mn, mx = chk.get("min"), chk.get("max")
                    if c:
                        cond_parts = []
                        if mn is not None:
                            cond_parts.append(F.col(c) < mn)
                        if mx is not None:
                            cond_parts.append(F.col(c) > mx)
                        if cond_parts:
                            from functools import reduce
                            combined = reduce(lambda a, b: a | b, cond_parts)
                            fail_count = df.filter(combined).count()
                            if fail_count > 0:
                                violations.append({"type": "valid_range", "column": c, "failed": fail_count})

        elif stype == "join":
            right_path = step.get("right_source", "")
            on_cols = step.get("on", [])
            how = step.get("how", "inner")
            if right_path and on_cols:
                right_df = spark.read.parquet(right_path)
                df = df.join(right_df, on=on_cols, how=how)

    rows_out = df.count()
    return df, rows_in, rows_out, violations


def main() -> None:
    parser = argparse.ArgumentParser(description="hydra-ingest Spark transform job")
    parser.add_argument("--transform-spec", required=True, help="Path to transform YAML DSL file")
    parser.add_argument("--input-path", required=True, help="Input data path (Parquet, JSON, or JSONL)")
    parser.add_argument("--output-path", help="Output path override (otherwise uses spec output config)")
    parser.add_argument("--input-format", default="parquet", choices=["parquet", "json", "jsonl", "csv"])
    parser.add_argument("--output-format", default="parquet", choices=["parquet", "iceberg"])
    args = parser.parse_args()

    import yaml

    with open(args.transform_spec, encoding="utf-8") as f:
        spec = yaml.safe_load(f)

    transform_id = spec.get("transform_id", "unknown")
    steps = spec.get("steps", [])
    output_cfg = spec.get("output", {})
    output_path = args.output_path or output_cfg.get("path") or f"/tmp/hydra/{transform_id}"

    spark = _build_spark(f"hydra-transform-{transform_id}")
    started = time.time()

    try:
        fmt = args.input_format
        if fmt == "jsonl":
            fmt = "json"
        df = spark.read.format(fmt).load(args.input_path)

        df, rows_in, rows_out, violations = _apply_steps(spark, df, steps)

        partition_cols = output_cfg.get("partition_by", [])
        writer = df.write.mode("overwrite")
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)

        if args.output_format == "iceberg":
            table_name = output_cfg.get("table", f"hydra.{transform_id}")
            writer.format("iceberg").saveAsTable(table_name)
        else:
            writer.parquet(output_path)

        elapsed = time.time() - started
        result = {
            "transform_id": transform_id,
            "engine": "spark",
            "rows_in": rows_in,
            "rows_out": rows_out,
            "quality_violations": violations,
            "output_path": output_path,
            "elapsed_seconds": round(elapsed, 2),
        }
        print(json.dumps(result, indent=2))

        if violations:
            print(f"\nWARNING: {len(violations)} quality violation(s) detected", file=sys.stderr)
            for v in violations:
                print(f"  - {v['type']} on {v.get('column', 'N/A')}: {v['failed']} rows failed", file=sys.stderr)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
