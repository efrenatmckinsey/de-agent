"""CLI entrypoint for hydra-ingest: run, publish, discover, validate."""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

import click
import yaml

from src.core.context_layer import ContextLayer, load_source_config
from src.core.orchestrator import Orchestrator
from src.core.topic_router import IngestionAction, IngestionCommand, create_router
from src.governance.quality import QualityEngine
from src.transform.dsl_parser import DSLParser

DEFAULT_CONFIG = "config/platform.yaml"


def _load_platform(ctx: click.Context) -> dict:
    path = ctx.obj.get("config", DEFAULT_CONFIG)
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


@click.group()
@click.option("--config", "-c", default=DEFAULT_CONFIG, help="Platform config YAML path")
@click.pass_context
def main(ctx: click.Context, config: str) -> None:
    """hydra-ingest — Distributed data ingestion platform."""
    ctx.ensure_object(dict)
    ctx.obj["config"] = config


@main.command()
@click.pass_context
def run(ctx: click.Context) -> None:
    """Start the orchestrator loop (consumes commands from topic router)."""
    config_path = ctx.obj["config"]
    click.echo(f"Starting hydra-ingest orchestrator (config: {config_path})")
    orch = Orchestrator(config_path)
    try:
        asyncio.run(orch.run())
    except KeyboardInterrupt:
        click.echo("\nShutting down...")
        asyncio.run(orch.stop())


@main.command()
@click.argument("source_id")
@click.option("--action", "-a", type=click.Choice(["ingest", "transform", "backfill"]), default="ingest")
@click.option("--priority", "-p", type=int, default=3)
@click.option("--payload", type=str, default="{}", help="JSON payload string")
@click.pass_context
def publish(ctx: click.Context, source_id: str, action: str, priority: int, payload: str) -> None:
    """Publish an ingestion command to the topic router."""
    platform_config = _load_platform(ctx)
    router = create_router(platform_config)
    cmd = IngestionCommand(
        source_id=source_id,
        action=IngestionAction(action),
        priority=priority,
        payload=json.loads(payload),
    )

    async def _pub() -> None:
        await router.start()
        await router.publish(cmd)
        click.echo(f"Published: {cmd.model_dump_json(indent=2)}")
        await router.stop()

    asyncio.run(_pub())


@main.command()
@click.argument("source_config_path")
@click.pass_context
def discover(ctx: click.Context, source_config_path: str) -> None:
    """Run context discovery on a source endpoint."""
    platform_config = _load_platform(ctx)
    source_config = load_source_config(source_config_path)
    source_id = source_config.get("source_id", Path(source_config_path).stem)
    layer = ContextLayer(platform_config)

    async def _discover() -> None:
        context = await layer.get_or_discover(source_id, source_config)
        click.echo(context.model_dump_json(indent=2))

    asyncio.run(_discover())


@main.command()
@click.argument("transform_spec_path")
def validate(transform_spec_path: str) -> None:
    """Validate a transform DSL YAML file."""
    spec = DSLParser.parse_file(transform_spec_path)
    errors = DSLParser.validate_steps(list(spec.steps))
    click.echo(f"Transform ID: {spec.transform_id}")
    click.echo(f"Engine hint:  {spec.engine_hint}")
    click.echo(f"Steps:        {len(spec.steps)}")
    click.echo(f"Output:       {spec.output.table} ({spec.output.format})")
    if errors:
        click.echo(f"\nValidation warnings ({len(errors)}):")
        for e in errors:
            click.echo(f"  - {e}")
        sys.exit(1)
    else:
        click.echo("\nAll steps valid.")


@main.command()
@click.argument("source_config_path")
@click.option("--records-json", help="Path to JSON file with sample records for quality checking")
def quality_check(source_config_path: str, records_json: str | None) -> None:
    """Run quality checks defined in a source config against sample records."""
    source_config = load_source_config(source_config_path)
    checks_cfg = source_config.get("quality_checks", [])
    gate_cfg = source_config.get("governance", {}).get("quality_gate", {"fail_on_critical": True, "warn_threshold": 0.95})
    source_id = source_config.get("source_id", "unknown")

    records: list[dict] = []
    if records_json:
        with open(records_json, encoding="utf-8") as f:
            records = json.load(f)

    engine = QualityEngine()
    result = engine.run_checks(source_id, records, checks_cfg, gate_cfg)
    click.echo(result.model_dump_json(indent=2))
    if not result.passed_gate:
        sys.exit(2)


@main.command()
@click.pass_context
def list_sources(ctx: click.Context) -> None:
    """List all configured source endpoints."""
    config_path = Path(ctx.obj["config"])
    sources_dir = config_path.parent / "sources"
    if not sources_dir.is_dir():
        click.echo("No sources directory found.")
        return
    for path in sorted(sources_dir.glob("*.yaml")):
        with path.open(encoding="utf-8") as f:
            data = yaml.safe_load(f)
        sid = data.get("source_id", path.stem)
        stype = data.get("type", "unknown")
        schedule = (data.get("schedule") or {}).get("cron", "manual")
        click.echo(f"  {sid:<30} type={stype:<8} schedule={schedule}")


@main.command()
@click.pass_context
def list_transforms(ctx: click.Context) -> None:
    """List all configured transform specifications."""
    config_path = Path(ctx.obj["config"])
    transforms_dir = config_path.parent / "transforms"
    if not transforms_dir.is_dir():
        click.echo("No transforms directory found.")
        return
    for path in sorted(transforms_dir.glob("*.yaml")):
        spec = DSLParser.parse_file(str(path))
        click.echo(
            f"  {spec.transform_id:<30} engine={spec.engine_hint:<6} "
            f"steps={len(spec.steps)} → {spec.output.table}"
        )


@main.command()
@click.argument("source_id")
@click.pass_context
def ingest_now(ctx: click.Context, source_id: str) -> None:
    """Run a single ingestion cycle for a source (bypasses topic router)."""
    config_path = ctx.obj["config"]
    orch = Orchestrator(config_path)
    orch.load_source_configs()
    orch.load_transform_configs()
    cmd = IngestionCommand(source_id=source_id, action=IngestionAction.INGEST, priority=1)

    async def _run() -> None:
        await orch._router.start()
        await orch._process_command(cmd)
        await orch._router.stop()

    click.echo(f"Ingesting {source_id}...")
    asyncio.run(_run())
    click.echo("Done.")


if __name__ == "__main__":
    main()
