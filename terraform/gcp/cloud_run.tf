resource "google_artifact_registry_repository" "images" {
  location      = var.region
  repository_id = "${var.name_prefix}-images"
  format        = "DOCKER"
  description   = "Bake-off task service images"

  depends_on = [google_project_service.required]
}

locals {
  # Where scripts/build-push-task-service.sh pushes to, unless overridden.
  default_image = join("", [
    "${var.region}-docker.pkg.dev/${var.project_id}/",
    "${google_artifact_registry_repository.images.repository_id}/task-service:latest",
  ])
  task_service_image = var.task_service_image != "" ? var.task_service_image : local.default_image
}

# The HTTP task layer every workflow step calls. Google Workflows runs no code of
# its own, so this is where all the DAG logic actually executes -- the role
# Lambdas play in the Step Functions stack.
resource "google_cloud_run_v2_service" "tasks" {
  name     = "${var.name_prefix}-task-service"
  location = var.region

  # Evaluation stack; let `terraform destroy` actually destroy.
  deletion_protection = false

  # Reached only by Workflows over the internet with an OIDC token, never
  # anonymously (see the run.invoker binding in iam.tf).
  ingress = "INGRESS_TRAFFIC_ALL"

  template {
    service_account = google_service_account.task_service.email

    # DAG 1's transform + Parquet write is the long pole; the YAML allows 900s.
    timeout = "900s"

    scaling {
      min_instance_count = 0
      max_instance_count = 10
    }

    containers {
      image = local.task_service_image

      ports {
        container_port = 8080
      }

      env {
        name  = "BAKEOFF_NS"
        value = var.bakeoff_ns
      }

      # Neon refuses non-TLS connections.
      env {
        name  = "PGSSLMODE"
        value = "require"
      }

      # Pushing a new :latest does not change this resource, so Cloud Run would
      # keep serving the old revision. Hashing the source forces a new revision
      # whenever the handler code actually changes.
      env {
        name  = "APP_SHA"
        value = filesha256("${path.module}/../../shared-services/gcp-task-service/app.py")
      }

      resources {
        limits = {
          # pyarrow alone needs a few hundred MB resident; 512Mi OOMs on import.
          cpu    = "1"
          memory = "1Gi"
        }
      }
    }
  }

  depends_on = [google_project_service.required]
}
