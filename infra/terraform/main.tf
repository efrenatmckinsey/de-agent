# -----------------------------------------------------------------------------
# hydra-ingest — AWS reference architecture
#
# Includes: S3 data lake, MSK (Kafka), ECR, ECS Fargate workers, Glue catalog DB,
# IAM roles (S3 / Glue / MSK), CloudWatch logs, and VPC wiring via default VPC.
#
# NOTE: MSK and Fargate incur cost. Review instance sizes before apply.
# -----------------------------------------------------------------------------

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # backend "s3" { ... }  # Configure remote state for team use.
}

provider "aws" {
  region = var.region

  default_tags {
    tags = local.common_tags
  }
}

# -----------------------------------------------------------------------------
# Data sources — default VPC (minimal footprint; replace with dedicated VPC for prod)
# -----------------------------------------------------------------------------
data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

# -----------------------------------------------------------------------------
# Locals — naming, tagging, subnet selection for MSK (>= 2 AZs)
# -----------------------------------------------------------------------------
locals {
  account_id   = data.aws_caller_identity.current.account_id
  region       = data.aws_region.current.name
  name_prefix  = "${var.project_name}-${var.environment}"
  common_tags = {
    Project     = "hydra-ingest"
    Environment = var.environment
    ManagedBy   = "terraform"
  }

  # MSK requires subnets in distinct AZs; take the first two default subnets.
  subnet_ids = slice(data.aws_subnets.default.ids, 0, min(2, length(data.aws_subnets.default.ids)))

  # Globally unique S3 bucket name (S3 namespace is global).
  # Pattern: hydra-data-lake-<env>-<account_id> — aligns with project data-lake naming.
  data_lake_bucket_name = "hydra-data-lake-${var.environment}-${local.account_id}"

  ecr_repo_name = "${local.name_prefix}-worker"
  glue_db_name  = replace("${local.name_prefix}_iceberg", "-", "_")
  log_group     = "/ecs/${local.name_prefix}-worker"
}

# -----------------------------------------------------------------------------
# S3 — versioned data lake bucket
# -----------------------------------------------------------------------------
resource "aws_s3_bucket" "data_lake" {
  bucket = local.data_lake_bucket_name

  tags = merge(local.common_tags, {
    Name = local.data_lake_bucket_name
    Role = "data-lake"
  })
}

resource "aws_s3_bucket_versioning" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# -----------------------------------------------------------------------------
# ECR — container images for hydra-worker
# -----------------------------------------------------------------------------
resource "aws_ecr_repository" "hydra_worker" {
  name                 = local.ecr_repo_name
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }

  tags = merge(local.common_tags, {
    Name = local.ecr_repo_name
  })
}

# -----------------------------------------------------------------------------
# Glue — Iceberg / analytics catalog database
# -----------------------------------------------------------------------------
resource "aws_glue_catalog_database" "iceberg" {
  name        = local.glue_db_name
  description = "Glue catalog database for Apache Iceberg tables (hydra-ingest)"

  tags = merge(local.common_tags, {
    Name = local.glue_db_name
  })
}

# -----------------------------------------------------------------------------
# CloudWatch — ECS task logs
# -----------------------------------------------------------------------------
resource "aws_cloudwatch_log_group" "ecs_worker" {
  name              = local.log_group
  retention_in_days = 30

  tags = merge(local.common_tags, {
    Name = local.log_group
  })
}

# -----------------------------------------------------------------------------
# Security groups — MSK broker access from ECS tasks only
# -----------------------------------------------------------------------------
resource "aws_security_group" "ecs_tasks" {
  name_prefix = "${local.name_prefix}-ecs-"
  description = "hydra ECS tasks (Fargate)"
  vpc_id      = data.aws_vpc.default.id

  egress {
    description = "Allow all outbound (MSK, S3, Glue API, ECR, etc.)"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(local.common_tags, {
    Name = "${local.name_prefix}-ecs-tasks"
  })

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_security_group" "msk" {
  name_prefix = "${local.name_prefix}-msk-"
  description = "MSK brokers — Kafka plaintext from ECS tasks"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    description     = "Kafka broker port from hydra workers"
    from_port       = 9092
    to_port         = 9092
    protocol        = "tcp"
    security_groups = [aws_security_group.ecs_tasks.id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(local.common_tags, {
    Name = "${local.name_prefix}-msk"
  })

  lifecycle {
    create_before_destroy = true
  }
}

# -----------------------------------------------------------------------------
# MSK — managed Kafka cluster (2 brokers)
# -----------------------------------------------------------------------------
resource "aws_msk_cluster" "hydra" {
  cluster_name           = "${local.name_prefix}-kafka"
  kafka_version          = var.kafka_version
  enhanced_monitoring    = "DEFAULT"
  number_of_broker_nodes = 2

  broker_node_group_info {
    client_subnets  = local.subnet_ids
    instance_type   = var.msk_broker_instance_type
    security_groups = [aws_security_group.msk.id]

    storage_info {
      ebs_storage_info {
        volume_size = var.msk_broker_volume_size_gb
      }
    }
  }

  encryption_info {
    encryption_in_transit {
      client_broker = "TLS_PLAINTEXT"
      in_cluster    = true
    }
  }

  client_authentication {
    unauthenticated = true
  }

  tags = merge(local.common_tags, {
    Name = "${local.name_prefix}-kafka"
  })
}

# -----------------------------------------------------------------------------
# IAM — ECS task execution (pull image, write logs)
# -----------------------------------------------------------------------------
data "aws_iam_policy_document" "ecs_task_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "ecs_execution" {
  name_prefix        = "${local.name_prefix}-exec-"
  assume_role_policy = data.aws_iam_policy_document.ecs_task_assume.json

  tags = merge(local.common_tags, {
    Name = "${local.name_prefix}-ecs-execution"
  })
}

resource "aws_iam_role_policy_attachment" "ecs_execution" {
  role       = aws_iam_role.ecs_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

# -----------------------------------------------------------------------------
# IAM — ECS task role (S3, Glue, MSK metadata)
# -----------------------------------------------------------------------------
data "aws_iam_policy_document" "ecs_task_policy" {
  statement {
    sid = "S3DataLake"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket",
      "s3:GetBucketLocation",
    ]
    resources = [
      aws_s3_bucket.data_lake.arn,
      "${aws_s3_bucket.data_lake.arn}/*",
    ]
  }

  statement {
    sid = "GlueCatalog"
    actions = [
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:GetTable",
      "glue:GetTables",
      "glue:GetPartition",
      "glue:GetPartitions",
      "glue:CreateTable",
      "glue:UpdateTable",
      "glue:BatchCreatePartition",
    ]
    resources = [
      "arn:aws:glue:${local.region}:${local.account_id}:catalog",
      "arn:aws:glue:${local.region}:${local.account_id}:database/${aws_glue_catalog_database.iceberg.name}",
      "arn:aws:glue:${local.region}:${local.account_id}:table/${aws_glue_catalog_database.iceberg.name}/*",
    ]
  }

  statement {
    sid = "MSKDescribe"
    actions = [
      "kafka:DescribeCluster",
      "kafka:DescribeClusterV2",
      "kafka:GetBootstrapBrokers",
    ]
    resources = [aws_msk_cluster.hydra.arn]
  }
}

resource "aws_iam_role" "ecs_task" {
  name_prefix        = "${local.name_prefix}-task-"
  assume_role_policy = data.aws_iam_policy_document.ecs_task_assume.json

  tags = merge(local.common_tags, {
    Name = "${local.name_prefix}-ecs-task"
  })
}

resource "aws_iam_role_policy" "ecs_task" {
  name   = "${local.name_prefix}-task-policy"
  role   = aws_iam_role.ecs_task.id
  policy = data.aws_iam_policy_document.ecs_task_policy.json
}

# -----------------------------------------------------------------------------
# ECS — Fargate cluster, task definition, service
# -----------------------------------------------------------------------------
resource "aws_ecs_cluster" "hydra" {
  name = "${local.name_prefix}-cluster"

  setting {
    name  = "containerInsights"
    value = "enabled"
  }

  tags = merge(local.common_tags, {
    Name = "${local.name_prefix}-cluster"
  })
}

resource "aws_ecs_task_definition" "hydra_worker" {
  family                   = "${local.name_prefix}-worker"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = tostring(var.ecs_cpu)
  memory                   = tostring(var.ecs_memory)
  execution_role_arn       = aws_iam_role.ecs_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  container_definitions = jsonencode([
    {
      name      = "hydra-worker"
      image     = "${aws_ecr_repository.hydra_worker.repository_url}:latest"
      essential = true
      environment = [
        { name = "AWS_DEFAULT_REGION", value = var.region },
        { name = "HYDRA_S3_BUCKET", value = aws_s3_bucket.data_lake.bucket },
        { name = "HYDRA_GLUE_DATABASE", value = aws_glue_catalog_database.iceberg.name },
        {
          # Plaintext endpoints first; falls back to TLS string if plaintext is unavailable.
          name  = "KAFKA_BOOTSTRAP_SERVERS"
          value = coalesce(aws_msk_cluster.hydra.bootstrap_brokers, aws_msk_cluster.hydra.bootstrap_brokers_tls)
        },
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = aws_cloudwatch_log_group.ecs_worker.name
          awslogs-region        = var.region
          awslogs-stream-prefix = "hydra-worker"
        }
      }
    }
  ])

  tags = merge(local.common_tags, {
    Name = "${local.name_prefix}-worker-taskdef"
  })
}

resource "aws_ecs_service" "hydra_worker" {
  name            = "${local.name_prefix}-worker"
  cluster         = aws_ecs_cluster.hydra.id
  task_definition = aws_ecs_task_definition.hydra_worker.arn
  desired_count   = var.ecs_desired_count
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = local.subnet_ids
    security_groups  = [aws_security_group.ecs_tasks.id]
    assign_public_ip = true # Simplifies ECR pulls; use VPC endpoints + private subnets in production.
  }

  tags = merge(local.common_tags, {
    Name = "${local.name_prefix}-worker-service"
  })

  depends_on = [aws_msk_cluster.hydra]
}

# -----------------------------------------------------------------------------
# Outputs — wire CI/CD and application config
# -----------------------------------------------------------------------------
output "data_lake_bucket" {
  description = "S3 bucket for the hydra data lake"
  value       = aws_s3_bucket.data_lake.bucket
}

output "ecr_repository_url" {
  description = "Push hydra-worker images here"
  value       = aws_ecr_repository.hydra_worker.repository_url
}

output "msk_bootstrap_brokers" {
  description = "Kafka bootstrap string (TLS/plain per cluster config)"
  value       = aws_msk_cluster.hydra.bootstrap_brokers_tls != "" ? aws_msk_cluster.hydra.bootstrap_brokers_tls : aws_msk_cluster.hydra.bootstrap_brokers
}

output "glue_database_name" {
  description = "Glue catalog database for Iceberg"
  value       = aws_glue_catalog_database.iceberg.name
}

output "ecs_cluster_name" {
  value = aws_ecs_cluster.hydra.name
}

output "ecs_service_name" {
  value = aws_ecs_service.hydra_worker.name
}
