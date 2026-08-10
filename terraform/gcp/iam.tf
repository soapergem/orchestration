# ---------------------------------------------------------------------------
# Three identities, one per trust boundary:
#   task_service -- what Cloud Run runs as (reads/writes the bucket)
#   workflows    -- what a workflow execution runs as (invokes Cloud Run, logs)
#   resume       -- what the OFF-CLUSTER mock services authenticate as when they
#                   POST to a Workflows callback endpoint. Direct analogue of the
#                   IAM user terraform/aws creates for SendTaskSuccess.
# ---------------------------------------------------------------------------

resource "google_service_account" "task_service" {
  account_id   = "${var.name_prefix}-task-service"
  display_name = "Cloud Run task service (bake-off HTTP task layer)"
  depends_on   = [google_project_service.required]
}

resource "google_service_account" "workflows" {
  account_id   = "${var.name_prefix}-workflows"
  display_name = "Google Workflows executions (bake-off)"
  depends_on   = [google_project_service.required]
}

resource "google_service_account" "resume" {
  account_id   = "${var.name_prefix}-resume"
  display_name = "Off-cluster mock services resuming suspended workflows"
  depends_on   = [google_project_service.required]
}

# --- task service: object read/write on its own bucket only ----------------

resource "google_storage_bucket_iam_member" "task_service_objects" {
  bucket = google_storage_bucket.data.name
  role   = "roles/storage.objectAdmin"
  member = google_service_account.task_service.member
}

# --- workflows: call the task service, and write logs ----------------------
#
# Cloud Run stays private (no allUsers binding); the workflow authenticates with
# an OIDC token, which is why every http.post in the YAMLs carries an `auth`
# block. Scoped to this one service rather than project-wide run.invoker.

resource "google_cloud_run_v2_service_iam_member" "workflows_invoke_tasks" {
  name     = google_cloud_run_v2_service.tasks.name
  location = google_cloud_run_v2_service.tasks.location
  role     = "roles/run.invoker"
  member   = google_service_account.workflows.member
}

resource "google_project_iam_member" "workflows_logging" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = google_service_account.workflows.member
}

# --- resume identity: deliver callbacks into a running execution -----------
#
# roles/workflows.invoker carries the execution-callback permission. Verify the
# 401/403 path during DAG 2/4 testing before trusting a narrower custom role.

resource "google_project_iam_member" "resume_workflows_invoker" {
  project = var.project_id
  role    = "roles/workflows.invoker"
  member  = google_service_account.resume.member
}

# Long-lived JSON key, because the caller lives on K3s and has no workload
# identity federation into this project. Same trade-off (and same evaluation-only
# caveat) as the AWS access key in terraform/aws/mock_services.tf.
resource "google_service_account_key" "resume" {
  count              = var.create_resume_service_account_key ? 1 : 0
  service_account_id = google_service_account.resume.name
}
