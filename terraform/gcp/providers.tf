# The project is set here rather than via `gcloud config set project`, so running
# terraform never depends on (or disturbs) whatever the local gcloud default is.
provider "google" {
  project = var.project_id
  region  = var.region
}
