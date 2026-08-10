# DAG 1's object store: the ZIP goes in, extracted CSVs land under extracted/,
# and the Parquet output under output/. The S3 bucket in terraform/aws plays the
# same role.
resource "google_storage_bucket" "data" {
  name          = "${var.name_prefix}-bakeoff-${var.project_id}"
  location      = var.region
  force_destroy = true

  uniform_bucket_level_access = true

  # Evaluation data -- don't accumulate cost from forgotten runs.
  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type = "Delete"
    }
  }

  depends_on = [google_project_service.required]
}

# The DAG 1 input, uploaded from the repo so an execution needs no manual setup.
resource "google_storage_bucket_object" "sample_data" {
  name   = "input/sample-data.zip"
  bucket = google_storage_bucket.data.name
  source = "${path.module}/../../test-data/sample-data.zip"
}
