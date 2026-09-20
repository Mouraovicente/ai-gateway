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

# Security groups are split by role on purpose: the ALB takes 443/80 from
# the internet, the task takes 8080 from the ALB's SG only. Leave both empty
# (the default) and the module creates that pair itself — which is the only
# configuration that cannot be got wrong. Supplying them means supplying
# BOTH, non-empty; see the precondition on aws_lb.this.
variable "alb_security_group_ids" {
  type    = list(string)
  default = []
}

variable "task_security_group_ids" {
  type    = list(string)
  default = []
}

# CIDRs allowed to reach the ALB on 443/80 when the module creates the SGs.
variable "alb_ingress_cidr_blocks" {
  type    = list(string)
  default = ["0.0.0.0/0"]
}

# Required when enable_fargate is true at the root: scopes the execution
# role's image-pull permissions to this one repository instead of "*".
variable "ecr_repository_arn" {
  type    = string
  default = null
}

# Required when enable_fargate is true at the root: the ALB always
# terminates TLS on 443 and redirects plain HTTP to it, so a certificate is
# mandatory, not optional. See docs/deploy-fargate.md for the prerequisite
# (an ACM certificate for the gateway's domain, validated beforehand).
variable "acm_certificate_arn" {
  type    = string
  default = null

  validation {
    condition     = var.acm_certificate_arn != null
    error_message = "acm_certificate_arn is required: the ALB listener terminates TLS on 443 and has no HTTP-only fallback."
  }
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
