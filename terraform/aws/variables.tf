variable "aws_region" {
  description = "AWS region to deploy into. Match your Neon region to minimize latency."
  type        = string
  default     = "us-east-1"
}

variable "aws_profile" {
  description = <<-EOT
    Named AWS CLI/SDK profile to authenticate with, from ~/.aws/credentials.
    Deliberately has NO default: a hard-coded profile name silently fails on
    every machine but the one it was written on. Set it in terraform.tfvars, or
    export TF_VAR_aws_profile (see .envrc.example).
  EOT
  type        = string
}

variable "name_prefix" {
  description = "Prefix applied to all resource names."
  type        = string
  default     = "orch-bakeoff"
}

variable "neon_database_url" {
  description = <<-EOT
    Neon Postgres connection string (DSN) for DAG 3/4. Provide it via the
    environment (TF_VAR_neon_database_url) so it never lands in a committed
    file. Stored as an SSM SecureString parameter.
  EOT
  type        = string
  sensitive   = true
}

variable "lambda_runtime" {
  description = "Lambda Python runtime. Must match the psycopg2 layer build."
  type        = string
  default     = "python3.12"
}

variable "lambda_architecture" {
  description = "Lambda CPU architecture. Must match the psycopg2 layer build."
  type        = string
  default     = "x86_64"
}

variable "mock_service_base_domain" {
  description = <<-EOT
    Base domain for the publicly-reachable mock services. Hostnames are
    <prefix><service>.<domain>, e.g. orch-callback-fetch.<domain>. Must be a
    domain whose DNS you control, since cert-manager issues certificates for
    those names. No default: set it in terraform.tfvars or export
    TF_VAR_mock_service_base_domain (see .envrc.example).
  EOT
  type        = string
}

variable "mock_service_subdomain_prefix" {
  description = "Prefix applied to each mock-service subdomain. Must match the domains in shared-services/deploy/values.yaml."
  type        = string
  default     = "orch-"
}

variable "bakeoff_ns" {
  description = <<-EOT
    Schema namespace for this runner (shared-services/init-db.sql). The lambdas'
    db.py pins search_path to <bakeoff_ns>_dag1/_dag3/_dag4. Neon is shared with
    the Google Workflows implementation, so this is what keeps the two apart.
    Seed it first: SELECT bootstrap_bakeoff('stepfunctions');
  EOT
  type        = string
  default     = "stepfunctions"
}
