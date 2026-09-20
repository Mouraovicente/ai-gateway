variable "cluster_name" {
  type    = string
  default = "ai-gateway"
}

variable "service_name" {
  type    = string
  default = "gateway"
}

variable "image" {
  type = string
}

variable "container_port" {
  type    = number
  default = 8080
}

variable "cpu" {
  type    = number
  default = 512
}

variable "memory" {
  type    = number
  default = 1024
}

variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "log_retention_days" {
  type    = number
  default = 14
}

# --- Networking (no VPC is created by this module; see docs/deploy-fargate.md) ---

variable "vpc_id" {
  type    = string
  default = null
}

variable "subnet_ids" {
  type    = list(string)
  default = []
}

variable "security_group_ids" {
  type    = list(string)
  default = []
}

# --- DynamoDB tables the task role is allowed to read/write ---

variable "dynamodb_table_arns" {
  type    = list(string)
  default = []
}

# --- SQS queue the task role is allowed to send to ---

variable "usage_queue_arn" {
  type    = string
  default = null
}

# --- Application environment (non-secret) ---

variable "environment" {
  type    = map(string)
  default = {}
}

# --- Secrets, injected via the ECS `secrets` block (Secrets Manager / SSM), never as plain env values ---

variable "secrets" {
  type    = map(string)
  default = {}
}
