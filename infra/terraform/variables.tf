# -----------------------------------------------------------------------------
# hydra-ingest — Terraform input variables (AWS)
# -----------------------------------------------------------------------------

variable "region" {
  description = "AWS region for all regional resources (MSK, ECS, S3, etc.)."
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Deployment stage (dev, staging, prod). Drives naming and tagging."
  type        = string
  default     = "dev"
}

variable "project_name" {
  description = "Short project prefix for resource names and tags."
  type        = string
  default     = "hydra-ingest"
}

variable "kafka_version" {
  description = "MSK Kafka engine version."
  type        = string
  default     = "3.6.0"
}

variable "msk_broker_instance_type" {
  description = "MSK broker EC2 instance type."
  type        = string
  default     = "kafka.m5.large"
}

variable "msk_broker_volume_size_gb" {
  description = "EBS volume size per broker (GiB)."
  type        = number
  default     = 20
}

variable "ecs_cpu" {
  description = "Fargate task CPU units (256, 512, 1024, ...)."
  type        = number
  default     = 512
}

variable "ecs_memory" {
  description = "Fargate task memory (MiB), must pair validly with cpu."
  type        = number
  default     = 1024
}

variable "ecs_desired_count" {
  description = "Desired number of hydra-worker tasks."
  type        = number
  default     = 1
}
