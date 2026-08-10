output "dag3_state_machine_arn" {
  description = "ARN of the DAG 3 payment state machine. Start an execution against this."
  value       = aws_sfn_state_machine.dag3.arn
}

output "neon_param_name" {
  description = "SSM parameter name holding the Neon DSN."
  value       = aws_ssm_parameter.neon_database_url.name
}

output "lambda_function_names" {
  description = "DAG 3 lambda function names."
  value       = [for fn in aws_lambda_function.dag3 : fn.function_name]
}

output "dag1_state_machine_arn" {
  description = "ARN of the DAG 1 CSV ETL state machine."
  value       = aws_sfn_state_machine.dag1.arn
}

output "dag1_bucket" {
  description = "S3 bucket for DAG 1 input/output."
  value       = aws_s3_bucket.dag1.id
}

output "dag1_sample_zip_key" {
  description = "Key of the seeded sample ZIP (use as zip_key in the DAG 1 input)."
  value       = "input/sample-data.zip"
}

output "dag2_state_machine_arn" {
  description = "ARN of the DAG 2 API fan-out state machine."
  value       = aws_sfn_state_machine.dag2.arn
}

output "dag4_state_machine_arn" {
  description = "ARN of the DAG 4 order-fulfillment state machine (start executions here)."
  value       = aws_sfn_state_machine.dag4.arn
}

output "callback_fetch_service_url" {
  description = "Public K3s URL the DAG 2 submit lambda calls."
  value       = local.callback_fetch_url
}

output "ecr_repository_urls" {
  description = "ECR repo URLs per mock service (used by build-push-mock-services.sh and the K8s manifests)."
  value       = { for k, r in aws_ecr_repository.mock : k => r.repository_url }
}

output "callback_resume_access_key_id" {
  description = "Access key ID for the SendTaskSuccess IAM user (put in the K8s aws-resume-creds Secret)."
  value       = aws_iam_access_key.callback_resume.id
}

output "callback_resume_secret_access_key" {
  description = "Secret access key for the SendTaskSuccess IAM user (sensitive)."
  value       = aws_iam_access_key.callback_resume.secret
  sensitive   = true
}

output "fixture_sample_zip_url" {
  description = "s3:// URI of DAG 1's archive; set as FIXTURE_SAMPLE_ZIP_URL on fixture-service."
  value       = "s3://${aws_s3_bucket.dag1.id}/input/sample-data.zip"
}

output "fixture_books_url" {
  description = "s3:// URI of the Open Library corpus; set as FIXTURE_BOOKS_URL on fixture-service."
  value       = "s3://${aws_s3_bucket.dag1.id}/input/books.json.gz"
}

output "fixture_objects_uploaded" {
  description = "Which fixture artefacts were present locally and uploaded (build them first if empty)."
  value = compact([
    length(aws_s3_object.sample_zip) > 0 ? "sample-data.zip" : "",
    length(aws_s3_object.books_corpus) > 0 ? "books.json.gz" : "",
  ])
}

output "fixture_reader_access_key_id" {
  description = "Access key ID for the fixture-service S3 reader (put in the K8s fixture-s3-creds Secret)."
  value       = aws_iam_access_key.fixture_reader.id
}

output "fixture_reader_secret_access_key" {
  description = "Secret access key for the fixture-service S3 reader (sensitive)."
  value       = aws_iam_access_key.fixture_reader.secret
  sensitive   = true
}
