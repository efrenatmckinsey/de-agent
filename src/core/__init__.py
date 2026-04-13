"""hydra-ingest core engine: routing, discovery, scaling, orchestration.

Concrete components (orchestrator, routers, autoscaler, context layer) live in
their submodules — import them directly (e.g. ``from src.core.orchestrator import Orchestrator``)
to avoid loading optional dependencies at package import time.
"""

from src.core.exceptions import (
    ConfigurationError,
    ContextDiscoveryError,
    HydraIngestError,
    IngestionError,
    OrchestratorError,
    ScalingError,
    SourceConfigurationError,
    StorageError,
    TopicRouterError,
    TransformError,
)

__all__ = [
    "ConfigurationError",
    "ContextDiscoveryError",
    "HydraIngestError",
    "IngestionError",
    "OrchestratorError",
    "ScalingError",
    "SourceConfigurationError",
    "StorageError",
    "TopicRouterError",
    "TransformError",
]
