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
