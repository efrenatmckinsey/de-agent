"""Command routing for ingestion: local queues or Kafka (aiokafka)."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from enum import Enum
from typing import Any, Literal, Protocol, runtime_checkable

import structlog
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import KafkaError
from pydantic import BaseModel, Field, field_validator

from src.core.exceptions import TopicRouterError

logger = structlog.get_logger(__name__)

DEFAULT_COMMAND_TOPIC = "hydra.ingestion.commands"


class IngestionAction(str, Enum):
    """Allowed ingestion command actions."""

    INGEST = "ingest"
    TRANSFORM = "transform"
    BACKFILL = "backfill"


class IngestionCommand(BaseModel):
    """Single message on the hydra ingestion command topic."""

    source_id: str = Field(..., min_length=1)
    action: IngestionAction
    priority: int = Field(default=3, ge=1, le=5)
    payload: dict[str, Any] = Field(default_factory=dict)

    @field_validator("source_id")
    @classmethod
    def strip_source_id(cls, v: str) -> str:
        return v.strip()


class PriorityCommandQueue:
    """asyncio.PriorityQueue wrapper: lower ``priority`` value is dequeued first (1 before 5)."""

    def __init__(self) -> None:
        self._queue: asyncio.PriorityQueue[tuple[int, int, IngestionCommand]] = asyncio.PriorityQueue()
        self._seq: int = 0

    def qsize(self) -> int:
        return self._queue.qsize()

    async def put(self, command: IngestionCommand) -> None:
        self._seq += 1
        await self._queue.put((command.priority, self._seq, command))

    async def get(self) -> IngestionCommand:
        _pri, _seq, cmd = await self._queue.get()
        return cmd

    def task_done(self) -> None:
        self._queue.task_done()


@runtime_checkable
class TopicRouter(Protocol):
    """Publishes and consumes :class:`IngestionCommand` messages."""

    async def start(self) -> None:
        """Prepare connections (Kafka consumer/producer, etc.)."""

    async def stop(self) -> None:
        """Release resources."""

    async def publish(self, command: IngestionCommand) -> None:
        """Enqueue or produce a command."""

    def consume(self) -> AsyncIterator[IngestionCommand]:
        """Async iterator of commands until stopped."""


class LocalTopicRouter:
    """In-process router using ``asyncio.Queue`` (FIFO; priority preserved on the model only)."""

    def __init__(self, *, maxsize: int = 0) -> None:
        self._queue: asyncio.Queue[IngestionCommand] = asyncio.Queue(maxsize=maxsize)
        self._stop = asyncio.Event()
        self._log = logger.bind(router="local")

    async def start(self) -> None:
        self._stop.clear()
        self._log.info("local_topic_router.started")

    async def stop(self) -> None:
        self._stop.set()
        self._log.info("local_topic_router.stopped")

    async def publish(self, command: IngestionCommand) -> None:
        if self._stop.is_set():
            raise TopicRouterError("LocalTopicRouter is stopped; cannot publish.")
        await self._queue.put(command)
        self._log.debug("published", source_id=command.source_id, action=command.action.value)

    async def consume(self) -> AsyncIterator[IngestionCommand]:
        while not self._stop.is_set() or not self._queue.empty():
            try:
                cmd = await asyncio.wait_for(self._queue.get(), timeout=0.5)
            except TimeoutError:
                continue
            yield cmd


class KafkaTopicRouter:
    """Kafka-backed router using aiokafka."""

    def __init__(
        self,
        *,
        bootstrap_servers: str,
        topic: str,
        group_id: str,
        client_id: str | None = None,
    ) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._topic = topic
        self._group_id = group_id
        self._client_id = client_id or "hydra-ingest-router"
        self._producer: AIOKafkaProducer | None = None
        self._consumer: AIOKafkaConsumer | None = None
        self._stop = asyncio.Event()
        self._log = logger.bind(router="kafka", topic=topic)

    async def start(self) -> None:
        self._stop.clear()
        try:
            self._producer = AIOKafkaProducer(
                bootstrap_servers=self._bootstrap_servers,
                client_id=f"{self._client_id}-producer",
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            await self._producer.start()
            self._consumer = AIOKafkaConsumer(
                self._topic,
                bootstrap_servers=self._bootstrap_servers,
                group_id=self._group_id,
                client_id=f"{self._client_id}-consumer",
                enable_auto_commit=True,
                auto_offset_reset="earliest",
                value_deserializer=lambda b: json.loads(b.decode("utf-8")),
            )
            await self._consumer.start()
        except KafkaError as exc:
            raise TopicRouterError(f"Failed to start Kafka router: {exc}") from exc
        self._log.info("kafka_topic_router.started")

    async def stop(self) -> None:
        self._stop.set()
        if self._consumer is not None:
            await self._consumer.stop()
            self._consumer = None
        if self._producer is not None:
            await self._producer.stop()
            self._producer = None
        self._log.info("kafka_topic_router.stopped")

    async def publish(self, command: IngestionCommand) -> None:
        if self._producer is None:
            raise TopicRouterError("KafkaTopicRouter is not started.")
        payload = command.model_dump(mode="json")
        try:
            await self._producer.send_and_wait(self._topic, value=payload)
        except KafkaError as exc:
            raise TopicRouterError(f"Kafka publish failed: {exc}") from exc
        self._log.debug("published", source_id=command.source_id, action=command.action.value)

    async def consume(self) -> AsyncIterator[IngestionCommand]:
        if self._consumer is None:
            raise TopicRouterError("KafkaTopicRouter is not started.")
        try:
            async for msg in self._consumer:
                if self._stop.is_set():
                    break
                try:
                    data = msg.value
                    if not isinstance(data, dict):
                        raise TopicRouterError(f"Unexpected message payload type: {type(data)}")
                    yield IngestionCommand.model_validate(data)
                except TopicRouterError:
                    raise
                except Exception as exc:
                    self._log.warning("skip_invalid_message", error=str(exc))
                    continue
        except KafkaError as exc:
            if not self._stop.is_set():
                raise TopicRouterError(f"Kafka consume failed: {exc}") from exc


def _resolve_kafka_bootstrap(topic_router_cfg: dict[str, Any]) -> str:
    kafka_cfg = topic_router_cfg.get("kafka") or {}
    backend: Literal["local", "aws"] = topic_router_cfg.get("kafka_backend", "local")
    if backend not in ("local", "aws"):
        backend = "local"
    cluster = kafka_cfg.get(backend) or {}
    servers = cluster.get("bootstrap_servers")
    if not servers:
        raise TopicRouterError(f"Missing bootstrap_servers for kafka backend '{backend}'.")
    return str(servers)


def create_router(config: dict[str, Any]) -> TopicRouter:
    """Build a :class:`LocalTopicRouter` or :class:`KafkaTopicRouter` from platform config."""
    tr = config.get("topic_router") or {}
    mode: str | None = tr.get("mode")
    if mode is None:
        env = (config.get("platform") or {}).get("environment", "dev")
        mode = "local" if str(env).lower() in {"dev", "local", "test"} else "kafka"

    topic = str(tr.get("topic", DEFAULT_COMMAND_TOPIC))
    group = str(tr.get("consumer_group", "hydra-ingest-workers"))

    if mode == "local":
        maxsize = int(tr.get("local_queue_maxsize", 0))
        logger.info("topic_router.created", implementation="local")
        return LocalTopicRouter(maxsize=maxsize)

    bootstrap = _resolve_kafka_bootstrap(tr)
    router = KafkaTopicRouter(
        bootstrap_servers=bootstrap,
        topic=topic,
        group_id=group,
        client_id=tr.get("client_id"),
    )
    logger.info("topic_router.created", implementation="kafka", topic=topic)
    return router
