terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region                      = "us-east-1"
  access_key                  = "test"
  secret_key                  = "test"
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true

  endpoints {
    dynamodb = "http://localhost:4566"
    sqs      = "http://localhost:4566"
  }
}

module "tenants_table" {
  source     = "./modules/dynamodb"
  table_name = "tenants"
  hash_key   = "api_key_hash"
}

module "budgets_table" {
  source        = "./modules/dynamodb"
  table_name    = "budgets"
  hash_key      = "tenant_id"
  range_key     = "period"
  ttl_attribute = "ttl"
}

module "requests_table" {
  source     = "./modules/dynamodb"
  table_name = "requests"
  hash_key   = "request_id"
}

module "trace_events_table" {
  source     = "./modules/dynamodb"
  table_name = "trace_events"
  hash_key   = "request_id"
  range_key  = "seq"
}

module "reservations_table" {
  source        = "./modules/dynamodb"
  table_name    = "reservations"
  hash_key      = "reservation_id"
  ttl_attribute = "ttl"
}

module "usage_events_queue" {
  source     = "./modules/sqs"
  queue_name = "usage-events"
}

# ECS is not emulated by LocalStack Community, so this module is opt-in via
# `enable_fargate`. CI leaves it off and plans only DynamoDB + SQS against
# LocalStack; a real `apply` sets it to true (see docs/deploy-fargate.md).
variable "enable_fargate" {
  type    = bool
  default = false
}

variable "image" {
  type    = string
  default = "ai-gateway:latest"
}

variable "subnet_ids" {
  type    = list(string)
  default = []
}

# Leave both empty to let the Fargate module create the ALB/task security
# group pair itself (recommended). Supplying them means supplying both.
variable "alb_security_group_ids" {
  type    = list(string)
  default = []
}

variable "task_security_group_ids" {
  type    = list(string)
  default = []
}

variable "vpc_id" {
  type    = string
  default = null
}

# Required (via -var or terraform.tfvars) when enable_fargate is true: the
# ECR repository the task's execution role is allowed to pull the image from.
variable "ecr_repository_arn" {
  type    = string
  default = null
}

# Required (via -var or terraform.tfvars) when enable_fargate is true: the
# ACM certificate the ALB's HTTPS:443 listener terminates TLS with. See
# docs/deploy-fargate.md for the prerequisite (request/validate the
# certificate for the gateway's domain before applying).
variable "acm_certificate_arn" {
  type    = string
  default = null
}

# Provider API keys (env var name -> Secrets Manager/SSM ARN), keyed exactly
# as `api_key_env` in config/routing.yaml (e.g. OPENROUTER_API_KEY,
# GEMINI_API_KEY). See docs/deploy-fargate.md for how to create them.
variable "provider_secret_arns" {
  type    = map(string)
  default = {}
}

module "gateway_service" {
  count                   = var.enable_fargate ? 1 : 0
  source                  = "./modules/ecs_fargate"
  image                   = var.image
  container_port          = 8080
  vpc_id                  = var.vpc_id
  subnet_ids              = var.subnet_ids
  ecr_repository_arn      = var.ecr_repository_arn
  acm_certificate_arn     = var.acm_certificate_arn
  alb_security_group_ids  = var.alb_security_group_ids
  task_security_group_ids = var.task_security_group_ids

  dynamodb_table_arns = [
    module.tenants_table.table_arn,
    module.budgets_table.table_arn,
    module.requests_table.table_arn,
    module.trace_events_table.table_arn,
    module.reservations_table.table_arn,
  ]
  usage_queue_arn = module.usage_events_queue.queue_arn

  environment = {
    GATEWAY_ADDR       = ":8080"
    ROUTING_CONFIG     = "config/routing.yaml"
    AWS_REGION         = "us-east-1"
    TENANTS_TABLE      = module.tenants_table.table_name
    BUDGETS_TABLE      = module.budgets_table.table_name
    REQUESTS_TABLE     = module.requests_table.table_name
    TRACE_EVENTS_TABLE = module.trace_events_table.table_name
    RESERVATIONS_TABLE = module.reservations_table.table_name
    USAGE_QUEUE_NAME   = module.usage_events_queue.queue_name
  }

  secrets = var.provider_secret_arns
}
