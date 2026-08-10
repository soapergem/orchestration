output "task_service_url" {
  description = "Cloud Run base URL every workflow step calls."
  value       = google_cloud_run_v2_service.tasks.uri
}

output "data_bucket" {
  description = "GCS bucket holding DAG 1's ZIP input, extracted CSVs, and Parquet output."
  value       = google_storage_bucket.data.name
}

output "zip_url" {
  description = "DAG 1's input object."
  value       = "gs://${google_storage_bucket.data.name}/${google_storage_bucket_object.sample_data.name}"
}

output "workflow_names" {
  description = "Deployed workflow names, for `gcloud workflows execute`."
  value = {
    dag1 = google_workflows_workflow.dag1_csv_etl.name
    dag2 = google_workflows_workflow.dag2_api_fanout.name
    dag3 = google_workflows_workflow.dag3_payment.name
    dag4 = google_workflows_workflow.dag4_order_fulfillment.name
  }
}

output "workflow_revisions" {
  description = <<-EOT
    Server-side revision per workflow. Each apply that changes a source file
    mints a new one; running executions stay on the revision they started with.
  EOT
  value = {
    dag1 = google_workflows_workflow.dag1_csv_etl.revision_id
    dag2 = google_workflows_workflow.dag2_api_fanout.revision_id
    dag3 = google_workflows_workflow.dag3_payment.revision_id
    dag4 = google_workflows_workflow.dag4_order_fulfillment.revision_id
  }
}

output "resume_service_account_email" {
  description = "Identity the off-cluster mock services use to deliver callbacks."
  value       = google_service_account.resume.email
}

output "resume_service_account_key" {
  description = <<-EOT
    Base64 JSON key for the resume service account, to be mounted into the K3s
    mock services (shared-services/deploy). Read it with:
      terraform output -raw resume_service_account_key | base64 -d
  EOT
  value       = var.create_resume_service_account_key ? google_service_account_key.resume[0].private_key : ""
  sensitive   = true
}
