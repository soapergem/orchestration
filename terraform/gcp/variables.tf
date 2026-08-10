variable "project_id" {
  description = "GCP project to deploy into."
  type        = string
}

variable "region" {
  description = "Region for Cloud Run and Workflows. Both must support it."
  type        = string
  default     = "us-central1"
}

variable "name_prefix" {
  description = "Prefix for every resource name, so the project stays readable."
  type        = string
  default     = "orch"
}

variable "bakeoff_ns" {
  description = <<-EOT
    Schema namespace for this runner (shared-services/init-db.sql). The task
    service pins search_path to <bakeoff_ns>_dag1/_dag3/_dag4. Seed it first:
    SELECT bootstrap_bakeoff('google_workflows');
  EOT
  type        = string
  default     = "google_workflows"
}

variable "task_service_image" {
  description = <<-EOT
    Full image reference for the task service. Defaults to the Artifact Registry
    path this stack creates; scripts/build-push-task-service.sh builds and pushes
    it. The image must exist BEFORE the first apply -- Cloud Run validates it.
  EOT
  type        = string
  default     = ""
}

# ---------------------------------------------------------------------------
# Mock services. These already run on Kubernetes and are publicly reachable for
# the Step Functions path (terraform/aws/mock_services.tf,
# shared-services/deploy, RUNNING.md 7c-i). Google Workflows calls the same
# hostnames, so nothing is redeployed here.
#
# Hostnames are derived, not listed one by one: <prefix><service>.<domain>,
# matching terraform/aws and shared-services/deploy/values.yaml.
# ---------------------------------------------------------------------------

variable "mock_service_base_domain" {
  description = <<-EOT
    Base domain for the publicly-reachable mock services. Must be a domain whose
    DNS you control, since cert-manager issues certificates for those names. No
    default: set it in terraform.tfvars or export
    TF_VAR_mock_service_base_domain (see .envrc.example).

    DAG 2 additionally requires the fixture host to be reachable from OUTSIDE
    GCP -- the callback-fetch service performs the initial fetch, and the
    workflow itself fetches each per-item detail URL.
  EOT
  type        = string
}

variable "mock_service_subdomain_prefix" {
  description = "Prefix applied to each mock-service subdomain. Must match shared-services/deploy/values.yaml."
  type        = string
  default     = "orch-"
}

variable "create_resume_service_account_key" {
  description = <<-EOT
    Create a JSON key for the resume service account. The K3s mock services need
    it to authenticate the POST to a Workflows callback endpoint (the GCP
    analogue of the IAM user terraform/aws creates for SendTaskSuccess). Long-
    lived keys are a liability -- evaluation-grade only.
  EOT
  type        = bool
  default     = true
}
