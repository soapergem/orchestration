variable "aws_region" {
  description = "AWS region to deploy into. Match your Neon region to minimize latency."
  type        = string
  default     = "us-east-1"
}

variable "aws_profile" {
  description = "Named AWS CLI/SDK profile to authenticate with."
  type        = string
  default     = "soapergem"
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
  description = "Base domain for the mock services on K3s. Hostnames are <prefix><service>.<domain>, e.g. orch-callback-fetch.<domain>."
  type        = string
  default     = "gemovationlabs.com"
}

variable "mock_service_subdomain_prefix" {
  description = "Prefix applied to each mock-service subdomain. Must match the domains in shared-services/deploy/values.yaml."
  type        = string
  default     = "orch-"
}
