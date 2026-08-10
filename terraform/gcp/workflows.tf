# The four DAGs.
#
# NOTE on why there is no templatefile() here, unlike terraform/aws where the ASL
# JSON is templated to inject Lambda ARNs: the Workflows language uses `${...}`
# for its OWN expressions, which is exactly Terraform's template interpolation
# syntax. Running these YAMLs through templatefile() makes Terraform try to
# evaluate every `${input.zip_url}` and `${sys.get_env(...)}` in the file and
# fail. Environment-specific values therefore arrive as `user_env_vars`, read in
# the workflow with sys.get_env() -- which also keeps the same file deployable by
# `gcloud workflows deploy --set-env-vars`, with no Terraform-only escaping.
#
# Each apply creates a new server-side revision (revision_id); in-flight
# executions keep running against the revision they started on.

locals {
  workflow_dir = "${path.module}/../../google-workflows"

  # <prefix><service>.<domain>, the same shape terraform/aws builds.
  mock = { for svc in ["callback-fetch", "approval", "shipping", "fixture"] :
    svc => "https://${var.mock_service_subdomain_prefix}${svc}.${var.mock_service_base_domain}"
  }

  # Config every workflow gets. Keys must not start with GOOGLE_ (reserved).
  common_env = {
    TASK_SERVICE_URL = google_cloud_run_v2_service.tasks.uri
    BAKEOFF_NS       = var.bakeoff_ns
  }
}

resource "google_workflows_workflow" "dag1_csv_etl" {
  name            = "${var.name_prefix}-dag1-csv-etl"
  region          = var.region
  description     = "DAG 1: unzip, parallel CSV load, SQL transform, Parquet export"
  service_account = google_service_account.workflows.id
  source_contents = file("${local.workflow_dir}/dag1_csv_etl.yaml")

  deletion_protection = false
  call_log_level      = "LOG_ALL_CALLS"

  user_env_vars = merge(local.common_env, {
    GCS_BUCKET = google_storage_bucket.data.name
    ZIP_URL    = "gs://${google_storage_bucket.data.name}/${google_storage_bucket_object.sample_data.name}"
  })

  depends_on = [google_project_service.required]
}

resource "google_workflows_workflow" "dag2_api_fanout" {
  name            = "${var.name_prefix}-dag2-api-fanout"
  region          = var.region
  description     = "DAG 2: async fetch suspended on a native callback, then parallel detail fan-out"
  service_account = google_service_account.workflows.id
  source_contents = file("${local.workflow_dir}/dag2_api_fanout.yaml")

  deletion_protection = false
  call_log_level      = "LOG_ALL_CALLS"

  user_env_vars = merge(local.common_env, {
    CALLBACK_FETCH_SERVICE_URL = local.mock["callback-fetch"]
    FIXTURE_SERVICE_URL        = local.mock["fixture"]
  })

  depends_on = [google_project_service.required]
}

resource "google_workflows_workflow" "dag3_payment" {
  name            = "${var.name_prefix}-dag3-payment"
  region          = var.region
  description     = "DAG 3: validate, flaky gateway with backoff, idempotent DB update, best-effort notify"
  service_account = google_service_account.workflows.id
  source_contents = file("${local.workflow_dir}/dag3_payment.yaml")

  deletion_protection = false
  call_log_level      = "LOG_ALL_CALLS"

  user_env_vars = local.common_env

  depends_on = [google_project_service.required]
}

resource "google_workflows_workflow" "dag4_order_fulfillment" {
  name            = "${var.name_prefix}-dag4-order-fulfillment"
  region          = var.region
  description     = "DAG 4: reserve, human approval on a native callback, ship, with saga compensation"
  service_account = google_service_account.workflows.id
  source_contents = file("${local.workflow_dir}/dag4_order_fulfillment.yaml")

  deletion_protection = false
  call_log_level      = "LOG_ALL_CALLS"

  user_env_vars = merge(local.common_env, {
    APPROVAL_SERVICE_URL = local.mock["approval"]
    SHIPPING_SERVICE_URL = local.mock["shipping"]
  })

  depends_on = [google_project_service.required]
}
