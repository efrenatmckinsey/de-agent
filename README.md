# hydra-ingest

Distributed data ingestion platform with adaptive engine selection, embedded governance, lineage tracking, static quality checks, and full observability.

## Architecture

```
                          ┌──────────────────────────────┐
                          │   Kafka / Local Queue         │
                          │   hydra.ingestion.commands    │
                          └──────────┬───────────────────┘
                                     │
                          ┌──────────▼───────────────────┐
                          │       Topic Router            │
                          │  (priority queue + routing)   │
                          └──────────┬───────────────────┘
                                     │
                          ┌──────────▼───────────────────┐
                          │      Context Layer            │
                          │  endpoint_probe()             │
                          │  schema_discovery()           │
                          │  volume_estimation()          │
                          │  freshness_check()            │
                          └──────────┬───────────────────┘
                                     │
              ┌──────────────────────┼──────────────────────┐
              │                      │                      │
    ┌─────────▼────────┐  ┌─────────▼────────┐  ┌─────────▼────────┐
    │   API Source      │  │  Stream Source    │  │  Batch Source     │
    │   (REST/GraphQL)  │  │  (SSE/polling)   │  │  (file/bulk API)  │
    └─────────┬────────┘  └─────────┬────────┘  └─────────┬────────┘
              └──────────────────────┼──────────────────────┘
                                     │
                          ┌──────────▼───────────────────┐
                          │     Auto Engine Selector      │
                          │  volume < threshold → Python  │
                          │  volume ≥ threshold → Spark   │
                          └───────┬──────────┬───────────┘
                                  │          │
                        ┌─────────▼──┐  ┌────▼──────────┐
                        │  Python    │  │  Spark         │
                        │  Engine    │  │  Engine        │
                        └─────────┬──┘  └────┬──────────┘
                                  └─────┬────┘
                                        │
                          ┌─────────────▼────────────────┐
                          │     S3 / Iceberg Writer       │
                          │   raw → curated → governed    │
                          └─────────────┬────────────────┘
                                        │
            ┌───────────────────────────┼─────────────────────────┐
            │                           │                         │
  ┌─────────▼────────┐  ┌──────────────▼──────────┐  ┌──────────▼────────┐
  │   Governance      │  │     Lineage Tracker     │  │   Quality Engine   │
  │   - PII detect    │  │     - OpenLineage       │  │   - not_null       │
  │   - classify      │  │     - run tracking      │  │   - valid_range    │
  │   - retention     │  │     - graph query        │  │   - unique_key     │
  └──────────────────┘  └─────────────────────────┘  │   - freshness      │
                                                      │   - regex_match    │
                                                      └──────────────────┘
```

### Horizontal Scaling

```
┌──────────────────────────────────────────────────────┐
│                   AutoScaler                         │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐             │
│  │ Worker 1 │ │ Worker 2 │ │ Worker N │  ← scale up │
│  └──────────┘ └──────────┘ └──────────┘  → shutdown  │
│                                           if idle    │
│  Signals: CPU% | Memory% | Queue depth              │
│  Local:  multiprocessing    AWS: ECS/EKS             │
└──────────────────────────────────────────────────────┘
```

## Project Structure

```
├── config/
│   ├── platform.yaml              # Global platform settings
│   ├── sources/                   # Source endpoint configurations
│   │   ├── api_food_data.yaml     # USDA FoodData Central (API)
│   │   ├── stream_weather.yaml    # NOAA Weather (streaming)
│   │   └── batch_census.yaml      # US Census Bureau (batch)
│   └── transforms/                # Transform DSL specifications
│       ├── food_enrichment.yaml   # Nutrient enrichment pipeline
│       └── census_aggregation.yaml# Population trend aggregation
├── src/
│   ├── core/
│   │   ├── topic_router.py        # Kafka/local command routing
│   │   ├── context_layer.py       # Source discovery & reflection
│   │   ├── autoscaler.py          # Horizontal scaling decisions
│   │   └── orchestrator.py        # Main DAG orchestration
│   ├── ingest/
│   │   ├── base.py                # Abstract source interface
│   │   ├── api_source.py          # REST API connector
│   │   ├── stream_source.py       # SSE/polling connector
│   │   └── batch_source.py        # Batch file/API connector
│   ├── transform/
│   │   ├── dsl_parser.py          # YAML DSL → TransformSpec
│   │   ├── spark_engine.py        # Spark transformation engine
│   │   └── python_engine.py       # Lightweight Python engine
│   ├── storage/
│   │   └── iceberg_writer.py      # S3/Iceberg + local writer
│   ├── governance/
│   │   ├── classifier.py          # PII detection & classification
│   │   ├── lineage.py             # OpenLineage event tracking
│   │   └── quality.py             # Static quality check engine
│   ├── observability/
│   │   ├── metrics.py             # Prometheus metrics
│   │   ├── tracing.py             # OpenTelemetry distributed tracing
│   │   └── logging_config.py      # Structured JSON logging
│   ├── endpoints/                 # Sample data source clients
│   │   ├── food_api.py            # USDA FoodData Central
│   │   ├── weather_stream.py      # NOAA Weather API
│   │   └── census_batch.py        # US Census Bureau
│   └── cli.py                     # CLI entrypoint
├── spark_jobs/
│   └── transform_job.py           # spark-submit entrypoint
├── infra/
│   ├── docker-compose.yaml        # Local: Kafka, MinIO, Spark, Prometheus
│   ├── terraform/                 # AWS: S3, MSK, ECS, Glue, IAM
│   └── k8s/                       # K8s: HPA, CronJobs, ConfigMaps
├── tests/
├── requirements.txt
└── pyproject.toml
```

## Quick Start

### 1. Install

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Explore Commands

```bash
# List all configured sources
python -m src.cli list-sources

# List all transform specs
python -m src.cli list-transforms

# Validate a transform DSL file
python -m src.cli validate config/transforms/food_enrichment.yaml

# Discover a source endpoint (probes the API, infers schema)
python -m src.cli discover config/sources/api_food_data.yaml

# Run a single ingestion cycle
python -m src.cli ingest-now usda_food_data

# Publish a command to the topic router
python -m src.cli publish usda_food_data --action ingest --priority 1

# Start the full orchestrator loop
python -m src.cli run
```

### 3. Run Sample Endpoints Directly

```bash
# Fetch USDA food data
python -m src.endpoints.food_api

# Stream NOAA weather observations
python -m src.endpoints.weather_stream

# Batch fetch Census population data
python -m src.endpoints.census_batch
```

### 4. Local Infrastructure

```bash
cd infra && docker-compose up -d
# Kafka:      localhost:9092
# MinIO:      localhost:9000 (console: 9001)
# Spark UI:   localhost:8080
# Prometheus:  localhost:9090
# Grafana:    localhost:3000
```

### 5. Run Tests

```bash
pytest tests/ -v
```

## Transform DSL

Transforms are declared in YAML with a step-based pipeline:

```yaml
transform_id: my_pipeline
engine_hint: auto          # auto | spark | python
volume_threshold: 100000   # rows — above this, use Spark
source: my_source_id

steps:
  - type: filter
    condition: "status IS NOT NULL AND amount > 0"

  - type: rename
    mappings:
      old_col: new_col

  - type: cast
    columns:
      amount: float
      count: int

  - type: derive
    new_column: total
    expression: "amount * count"

  - type: aggregate
    group_by: [category]
    measures:
      - { column: amount, function: sum, alias: total_amount }
      - { column: amount, function: avg, alias: avg_amount }

  - type: quality_gate
    checks:
      - { type: not_null, columns: [category, total_amount] }
      - { type: valid_range, column: total_amount, min: 0 }

output:
  table: curated.my_output
  format: iceberg
  partition_by: [category]
```

**Supported step types:** `filter`, `flatten`, `rename`, `cast`, `derive`, `aggregate`, `join`, `quality_gate`

**Engine selection:** When `engine_hint: auto`, the platform estimates row count via the Context Layer. If estimated rows exceed `volume_threshold`, Spark is used; otherwise, the lightweight Python engine handles it.

## Governance

### Data Classification & PII Detection
The classifier scans column names and sample values for PII patterns (email, SSN, phone, credit card, IP). If PII is detected on a "public" dataset, classification is auto-escalated to CONFIDENTIAL.

### Lineage Tracking
Every ingestion and transform run emits OpenLineage-compatible events (START, COMPLETE, FAIL) with schema facets, data quality facets, and transform step metadata. Events can be emitted to an OpenLineage HTTP endpoint or stored in-memory.

### Static Quality Checks
Configurable per-source quality gates: `not_null`, `valid_range`, `unique_key`, `row_count`, `regex_match`, `freshness`, `referential`. The gate evaluates `fail_on_critical` and `warn_threshold` to pass/fail the pipeline.

## Observability

| Signal | Technology | Details |
|--------|-----------|---------|
| Metrics | Prometheus | Counters/histograms for ingestion, transforms, quality, storage |
| Tracing | OpenTelemetry | Distributed spans for each ingestion/transform cycle |
| Logging | structlog | Structured JSON logs with platform context (env, component, run_id) |

## AWS Deployment

```bash
cd infra/terraform
terraform init && terraform plan -var="environment=staging"
terraform apply
```

Provisions: S3 bucket, MSK cluster, ECS Fargate service, Glue catalog, CloudWatch logs, IAM roles.

## Kubernetes Scaling

```bash
kubectl apply -f infra/k8s/deployment.yaml
```

Includes HorizontalPodAutoscaler (CPU 70%, memory 80%, custom queue_depth metric) and a CronJob for scheduled batch ingestion.

## Sample Data Sources

| Source | Type | API | Data |
|--------|------|-----|------|
| USDA FoodData Central | API | `api.nal.usda.gov/fdc/v1` | Nutrients, food composition |
| NOAA Weather | Stream | `api.weather.gov` | Observations, alerts |
| US Census Bureau | Batch | `api.census.gov/data` | Population, poverty, housing |

All use free, public government APIs (no API key required for basic access).
