"""Tests for the topic router: local queue, priority, command validation."""

from __future__ import annotations

import asyncio

import pytest

from src.core.topic_router import (
    IngestionAction,
    IngestionCommand,
    LocalTopicRouter,
    PriorityCommandQueue,
    create_router,
)


def test_ingestion_command_validation():
    cmd = IngestionCommand(source_id="test_source", action="ingest", priority=3)
    assert cmd.action == IngestionAction.INGEST
    assert cmd.priority == 3
    assert cmd.payload == {}


def test_ingestion_command_strips_source_id():
    cmd = IngestionCommand(source_id="  padded  ", action="transform")
    assert cmd.source_id == "padded"


def test_ingestion_command_priority_bounds():
    with pytest.raises(Exception):
        IngestionCommand(source_id="s", action="ingest", priority=0)
    with pytest.raises(Exception):
        IngestionCommand(source_id="s", action="ingest", priority=6)


def test_ingestion_command_invalid_action():
    with pytest.raises(Exception):
        IngestionCommand(source_id="s", action="delete")


@pytest.mark.asyncio
async def test_priority_queue_ordering():
    pq = PriorityCommandQueue()
    await pq.put(IngestionCommand(source_id="low", action="ingest", priority=5))
    await pq.put(IngestionCommand(source_id="high", action="ingest", priority=1))
    await pq.put(IngestionCommand(source_id="med", action="ingest", priority=3))

    first = await pq.get()
    assert first.source_id == "high"
    second = await pq.get()
    assert second.source_id == "med"
    third = await pq.get()
    assert third.source_id == "low"


@pytest.mark.asyncio
async def test_local_router_publish_consume():
    router = LocalTopicRouter()
    await router.start()
    cmd = IngestionCommand(source_id="test", action="ingest")
    await router.publish(cmd)

    received = []
    async def consume():
        async for c in router.consume():
            received.append(c)
            break

    await asyncio.wait_for(consume(), timeout=2.0)
    assert len(received) == 1
    assert received[0].source_id == "test"
    await router.stop()


@pytest.mark.asyncio
async def test_local_router_stop_prevents_publish():
    router = LocalTopicRouter()
    await router.start()
    await router.stop()
    with pytest.raises(Exception, match="stopped"):
        await router.publish(IngestionCommand(source_id="s", action="ingest"))


def test_create_router_local_for_dev():
    config = {
        "platform": {"environment": "dev"},
        "topic_router": {"topic": "test.topic"},
    }
    router = create_router(config)
    assert isinstance(router, LocalTopicRouter)


def test_create_router_local_for_test_env():
    config = {
        "platform": {"environment": "test"},
        "topic_router": {},
    }
    router = create_router(config)
    assert isinstance(router, LocalTopicRouter)


def test_ingestion_command_serialization():
    cmd = IngestionCommand(
        source_id="demo",
        action="backfill",
        priority=2,
        payload={"start_date": "2024-01-01"},
    )
    data = cmd.model_dump(mode="json")
    assert data["source_id"] == "demo"
    assert data["action"] == "backfill"
    assert data["payload"]["start_date"] == "2024-01-01"

    roundtrip = IngestionCommand.model_validate(data)
    assert roundtrip == cmd
