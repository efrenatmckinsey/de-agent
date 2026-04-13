"""Domain-specific exceptions for hydra-ingest core."""


class HydraIngestError(Exception):
    """Base error for hydra-ingest operations."""


class ConfigurationError(HydraIngestError):
    """Invalid or missing platform or component configuration."""


class SourceConfigurationError(HydraIngestError):
    """Source definition is missing required fields or is inconsistent."""


class TopicRouterError(HydraIngestError):
    """Kafka or local command routing failed."""


class ContextDiscoveryError(HydraIngestError):
    """HTTP discovery, schema reflection, or caching failed."""


class ScalingError(HydraIngestError):
    """Autoscaler or process management failed."""


class OrchestratorError(HydraIngestError):
    """Orchestration pipeline step failed."""


class IngestionError(HydraIngestError):
    """Connector fetch, parse, or transport failed during ingestion."""


class TransformError(HydraIngestError):
    """Transform DSL parsing or engine execution failed."""


class StorageError(HydraIngestError):
    """Lakehouse write, catalog, or filesystem operation failed."""
