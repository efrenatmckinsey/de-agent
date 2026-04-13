"""Raw and curated lake writers: Parquet landing and Iceberg curated tables."""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import structlog
from pydantic import BaseModel, Field

from src.core.exceptions import StorageError

logger = structlog.get_logger(__name__)


class StorageConfig(BaseModel):
    """Normalized storage settings for S3 zones and the Iceberg catalog."""

    bucket: str = ""
    raw_path: str = "raw"
    curated_path: str = "curated"
    governed_path: str = "governed"
    catalog_type: str = "glue"
    warehouse_uri: str = ""
    region: str = ""
    catalog_name: str = "hydra"
    local_base_path: str = Field(default="./data/lake", description="Root for LocalStorageWriter")
    pyiceberg_properties: dict[str, str] = Field(default_factory=dict)


class WriteResult(BaseModel):
    destination: str
    records_written: int
    bytes_written: int = 0
    partitions: list[str] = Field(default_factory=list)
    timestamp: datetime
    format: str


def _split_s3_uri(uri: str) -> tuple[str, str]:
    """Return (bucket, key_prefix) from ``s3://bucket/prefix``."""
    rest = uri[5:].strip("/")
    if "/" in rest:
        b, _, prefix = rest.partition("/")
        return b, prefix.strip("/")
    return rest, ""


def _zone_key(
    bucket: str,
    uri_val: str,
    default_key: str,
) -> tuple[str, str]:
    """Return (possibly updated bucket, path-under-bucket) for a zone entry."""
    u = (uri_val or "").strip()
    if u.startswith("s3://"):
        b2, prefix = _split_s3_uri(u)
        b_out = bucket or b2
        key = prefix if (not b_out or b2 == b_out) else f"{b2}/{prefix}".strip("/")
        return b_out, (key or default_key)
    if u:
        return bucket, u.strip("/")
    return bucket, default_key


def _parse_nested_config(config: dict[str, Any]) -> dict[str, Any]:
    """Accept either flat dicts or ``platform.yaml``-style ``storage`` subtree."""
    if "storage" in config and isinstance(config["storage"], dict):
        cfg = dict(config["storage"])
        s3 = cfg.get("s3") or {}
        iceberg = cfg.get("iceberg") or {}
        catalog = iceberg.get("catalog") or {}
        base_paths = s3.get("base_paths") or {}
        bucket = str(s3.get("bucket", "") or "")
        bucket, raw_p = _zone_key(bucket, str(base_paths.get("raw", "")), "raw")
        bucket, cur_p = _zone_key(bucket, str(base_paths.get("curated", "")), "curated")
        bucket, gov_p = _zone_key(bucket, str(base_paths.get("governed", "")), "governed")
        return {
            "bucket": bucket or config.get("bucket", ""),
            "raw_path": raw_p or config.get("raw_path", "raw"),
            "curated_path": cur_p or config.get("curated_path", "curated"),
            "governed_path": gov_p or config.get("governed_path", "governed"),
            "catalog_type": catalog.get("type", config.get("catalog_type", "glue")),
            "warehouse_uri": catalog.get("warehouse", config.get("warehouse_uri", "")),
            "region": s3.get("region", config.get("region", "")),
            "catalog_name": config.get("catalog_name", "hydra"),
            "local_base_path": config.get("local_base_path", "./data/lake"),
            "pyiceberg_properties": dict(config.get("pyiceberg_properties", {})),
        }
    return config


def _partition_suffix(partition_values: dict[str, Any] | None) -> str:
    if not partition_values:
        return ""
    parts = [f"{k}={v}" for k, v in sorted(partition_values.items())]
    return "/".join(parts)


def _table_bytes(table: pa.Table) -> int:
    try:
        return int(table.nbytes)
    except Exception:
        return 0


class IcebergStorageWriter:
    """Write raw Parquet to S3 and append curated data through a PyIceberg catalog."""

    def __init__(self, config: dict[str, Any]) -> None:
        flat = _parse_nested_config(config)
        try:
            self._cfg = StorageConfig.model_validate(flat)
        except Exception as e:
            raise StorageError(f"Invalid storage configuration: {e}") from e
        self._log = logger.bind(writer="iceberg_s3", bucket=self._cfg.bucket)

    @staticmethod
    def iceberg_schema_from_sample(
        sample: dict[str, Any],
        *,
        merge_samples: list[dict[str, Any]] | None = None,
    ) -> Any:
        """Build a PyIceberg ``Schema`` with fresh field IDs from dict sample(s)."""
        try:
            from pyiceberg.io.pyarrow import _pyarrow_to_schema_without_ids
            from pyiceberg.schema import assign_fresh_schema_ids
        except ImportError as e:
            raise StorageError("pyiceberg is required for Iceberg schema conversion") from e
        rows = [sample]
        if merge_samples:
            rows.extend(merge_samples)
        try:
            table = pa.Table.from_pylist(rows)
        except (pa.ArrowInvalid, TypeError) as e:
            raise StorageError(f"Cannot infer PyArrow schema from sample: {e}") from e
        try:
            iceberg_no_ids = _pyarrow_to_schema_without_ids(table.schema)
            return assign_fresh_schema_ids(iceberg_no_ids)
        except Exception as e:
            raise StorageError(f"Iceberg schema conversion failed: {e}") from e

    def _s3_raw_prefix(self, source_id: str, partition_values: dict[str, Any] | None) -> str:
        bucket = self._cfg.bucket
        if not bucket:
            raise StorageError("S3 bucket is required for IcebergStorageWriter.write_raw")
        raw = self._cfg.raw_path.strip("/")
        part = _partition_suffix(partition_values)
        base = f"s3://{bucket}/{raw}/{source_id}"
        return f"{base}/{part}" if part else base

    def write_raw(
        self,
        source_id: str,
        records: list[dict[str, Any]],
        partition_values: dict[str, Any] | None = None,
    ) -> WriteResult:
        if not records:
            raise StorageError("write_raw requires at least one record")
        table = pa.Table.from_pylist(records)
        dest_dir = self._s3_raw_prefix(source_id, partition_values)
        file_name = f"part-{uuid.uuid4().hex}.parquet"
        dest = f"{dest_dir.rstrip('/')}/{file_name}"
        self._log.info("storage.write_raw.start", destination=dest, rows=len(records))
        try:
            fs, path = pa.fs.FileSystem.from_uri(dest)
            sink = fs.open_output_stream(path)
            try:
                pq.write_table(table, sink)
            finally:
                sink.close()
            info = fs.get_file_info(path)
            size = int(info.size) if info.size is not None else _table_bytes(table)
        except Exception as e:
            self._log.exception("storage.write_raw.failed", destination=dest)
            raise StorageError(f"Failed to write raw Parquet to {dest}: {e}") from e
        parts = [p for p in _partition_suffix(partition_values).split("/") if p] if partition_values else []
        self._log.info("storage.write_raw.done", destination=dest, bytes_written=size)
        return WriteResult(
            destination=dest,
            records_written=len(records),
            bytes_written=size,
            partitions=parts,
            timestamp=datetime.now(timezone.utc),
            format="parquet",
        )

    def _build_partition_spec(self, iceberg_schema: Any, partition_by: list[str]) -> Any:
        from pyiceberg.partitioning import PARTITION_FIELD_ID_START, PartitionField, PartitionSpec
        from pyiceberg.transforms import IdentityTransform

        if not partition_by:
            return PartitionSpec()
        fields: list[Any] = []
        next_pid = PARTITION_FIELD_ID_START
        for name in partition_by:
            nf = iceberg_schema.find_field(name)
            fields.append(
                PartitionField(
                    source_id=nf.field_id,
                    field_id=next_pid,
                    transform=IdentityTransform(),
                    name=name,
                )
            )
            next_pid += 1
        return PartitionSpec(*fields)

    def _load_catalog(self) -> Any:
        try:
            from pyiceberg.catalog import load_catalog
        except ImportError as e:
            raise StorageError("pyiceberg is not installed") from e
        props: dict[str, str] = {
            "type": self._cfg.catalog_type,
            "warehouse": self._cfg.warehouse_uri or f"s3://{self._cfg.bucket}/warehouse",
        }
        if self._cfg.region:
            props["region"] = self._cfg.region
        props.update(self._cfg.pyiceberg_properties)
        try:
            return load_catalog(self._cfg.catalog_name, **props)
        except Exception as e:
            self._log.exception("storage.catalog.load_failed")
            raise StorageError(f"Cannot load Iceberg catalog {self._cfg.catalog_name!r}: {e}") from e

    def write_curated(
        self,
        table_name: str,
        records: list[dict[str, Any]],
        partition_by: list[str],
    ) -> WriteResult:
        if not records:
            raise StorageError("write_curated requires at least one record")
        pa_table = pa.Table.from_pylist(records)
        catalog = self._load_catalog()
        self._log.info(
            "storage.write_curated.start",
            table=table_name,
            rows=len(records),
            partition_by=partition_by,
        )
        try:
            from pyiceberg.io.pyarrow import _pyarrow_to_schema_without_ids
            from pyiceberg.partitioning import PartitionSpec
            from pyiceberg.schema import assign_fresh_schema_ids
        except ImportError as e:
            raise StorageError("pyiceberg is required for write_curated") from e

        try:
            iceberg_schema = assign_fresh_schema_ids(_pyarrow_to_schema_without_ids(pa_table.schema))
            part_spec = self._build_partition_spec(iceberg_schema, partition_by)
            tbl = catalog.create_table_if_not_exists(
                table_name,
                schema=iceberg_schema,
                partition_spec=part_spec if partition_by else PartitionSpec(),
            )
            tbl.append(pa_table)
            loc = str(tbl.metadata.location)
        except Exception as e:
            self._log.exception("storage.write_curated.failed", table=table_name)
            raise StorageError(f"Curated Iceberg write failed for {table_name}: {e}") from e

        size = _table_bytes(pa_table)
        self._log.info("storage.write_curated.done", table=table_name, metadata_location=loc)
        return WriteResult(
            destination=loc,
            records_written=len(records),
            bytes_written=size,
            partitions=list(partition_by),
            timestamp=datetime.now(timezone.utc),
            format="iceberg",
        )


class LocalStorageWriter:
    """Filesystem-backed writer for local development (Parquet; curated as Parquet dataset)."""

    def __init__(self, config: dict[str, Any]) -> None:
        flat = _parse_nested_config(config)
        try:
            self._cfg = StorageConfig.model_validate(flat)
        except Exception as e:
            raise StorageError(f"Invalid storage configuration: {e}") from e
        self._root = Path(self._cfg.local_base_path).expanduser().resolve()
        self._log = logger.bind(writer="local", root=str(self._root))

    @staticmethod
    def iceberg_schema_from_sample(
        sample: dict[str, Any],
        *,
        merge_samples: list[dict[str, Any]] | None = None,
    ) -> Any:
        """Delegate to the same Iceberg schema helper (requires pyiceberg)."""
        return IcebergStorageWriter.iceberg_schema_from_sample(sample, merge_samples=merge_samples)

    def write_raw(
        self,
        source_id: str,
        records: list[dict[str, Any]],
        partition_values: dict[str, Any] | None = None,
    ) -> WriteResult:
        if not records:
            raise StorageError("write_raw requires at least one record")
        table = pa.Table.from_pylist(records)
        rel = Path(self._cfg.raw_path.strip("/")) / source_id
        part = _partition_suffix(partition_values)
        if part:
            rel = rel / part.replace("/", os.sep)
        out_dir = self._root / rel
        out_dir.mkdir(parents=True, exist_ok=True)
        file_path = out_dir / f"part-{uuid.uuid4().hex}.parquet"
        self._log.info("storage.local.write_raw.start", path=str(file_path), rows=len(records))
        try:
            pq.write_table(table, file_path)
            size = file_path.stat().st_size
        except OSError as e:
            self._log.exception("storage.local.write_raw.failed", path=str(file_path))
            raise StorageError(f"Failed to write raw Parquet locally: {e}") from e
        parts = [p for p in _partition_suffix(partition_values).split("/") if p] if partition_values else []
        return WriteResult(
            destination=str(file_path),
            records_written=len(records),
            bytes_written=size,
            partitions=parts,
            timestamp=datetime.now(timezone.utc),
            format="parquet",
        )

    def write_curated(
        self,
        table_name: str,
        records: list[dict[str, Any]],
        partition_by: list[str],
    ) -> WriteResult:
        if not records:
            raise StorageError("write_curated requires at least one record")
        safe_name = table_name.replace(".", "_")
        out_dir = self._root / Path(self._cfg.curated_path.strip("/")) / safe_name
        out_dir.mkdir(parents=True, exist_ok=True)
        file_path = out_dir / f"part-{uuid.uuid4().hex}.parquet"
        table = pa.Table.from_pylist(records)
        self._log.info(
            "storage.local.write_curated.start",
            path=str(file_path),
            partition_by=partition_by,
            rows=len(records),
        )
        try:
            pq.write_table(table, file_path)
            size = file_path.stat().st_size
        except OSError as e:
            self._log.exception("storage.local.write_curated.failed", path=str(file_path))
            raise StorageError(f"Failed to write curated Parquet locally: {e}") from e
        meta = out_dir / "hydra_table_meta.json"
        try:
            meta.write_text(
                json.dumps(
                    {
                        "table_name": table_name,
                        "partition_by": partition_by,
                        "format": "parquet",
                    },
                    indent=2,
                ),
                encoding="utf-8",
            )
        except OSError as e:
            self._log.warning("storage.local.write_curated.meta_failed", error=str(e))
        return WriteResult(
            destination=str(file_path),
            records_written=len(records),
            bytes_written=size,
            partitions=list(partition_by),
            timestamp=datetime.now(timezone.utc),
            format="parquet",
        )


def create_writer(config: dict[str, Any]) -> IcebergStorageWriter | LocalStorageWriter:
    """Choose S3/Iceberg vs local filesystem based on environment and flags."""
    env_local = os.environ.get("HYDRA_STORAGE_LOCAL", "").lower() in ("1", "true", "yes")
    cfg_flag = bool(config.get("local") or config.get("use_local_storage"))
    if env_local or cfg_flag:
        return LocalStorageWriter(config)
    return IcebergStorageWriter(config)
