# Neon connection string, held as a free-tier SSM Parameter Store SecureString
# (encrypted with the AWS-managed `aws/ssm` KMS key). The value flows in from the
# sensitive TF_VAR_neon_database_url variable. NOTE: the value is stored in
# Terraform state, so treat state as sensitive (use an encrypted remote backend).
resource "aws_ssm_parameter" "neon_database_url" {
  name        = "/${var.name_prefix}/neon-database-url"
  description = "Neon Postgres DSN for DAG 3/4 lambdas"
  type        = "SecureString"
  value       = var.neon_database_url
}
