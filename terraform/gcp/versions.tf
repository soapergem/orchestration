terraform {
  required_version = ">= 1.9"

  # PARTIAL backend configuration -- see terraform/aws/versions.tf for the full
  # rationale. The block is empty so no bucket name is committed; supply it at
  # init time from a gitignored backend.hcl:
  #
  #   cp backend.hcl.example backend.hcl     # then edit
  #   terraform -chdir=terraform/gcp init -backend-config=backend.hcl
  #
  # S3 rather than GCS deliberately: one state bucket for the whole repo is
  # simpler than one per cloud, and the AWS credentials are already present.
  # State contains the Neon DSN and the resume service-account key, so the
  # bucket must be private, encrypted and versioned.
  backend "s3" {}

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.50"
    }
  }
}
