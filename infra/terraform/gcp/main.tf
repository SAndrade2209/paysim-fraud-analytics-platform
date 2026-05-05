###############################################################################
# PaySim Fraud Analytics — GCP Infrastructure
# Provisions: GCS bucket · service account · IAM bindings
###############################################################################

terraform {
  required_version = ">= 1.5"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

# ─── Provider ─────────────────────────────────────────────────────────────────
provider "google" {
  project = var.gcp_project_id
  region  = var.gcp_region
  # Uses Application Default Credentials — run once before terraform apply:
  #   gcloud auth application-default login
  # The fraud-loader SA key (secrets/fraud-loader-key.json) is the RUNTIME
  # identity for Airflow/ingestion, not the Terraform operator identity.
}

# ─── GCS Landing Bucket ───────────────────────────────────────────────────────
resource "google_storage_bucket" "paysim_landing" {
  name          = "${var.gcp_project_id}-paysim-landing"
  location      = var.gcs_bucket_location
  storage_class = "STANDARD"

  # Prevent accidental deletion while the pipeline is active
  force_destroy = false

  # Lifecycle: move raw batches to Nearline after 30 days, delete after 90
  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type          = "SetStorageClass"
      storage_class = "COLDLINE"
    }
  }

  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type = "Delete"
    }
  }

  # Uniform bucket-level access (no per-object ACLs)
  uniform_bucket_level_access = true

  versioning {
    enabled = false   # raw CSVs are immutable; no need for versioning
  }

  labels = {
    project     = "paysim-fraud-analytics"
    environment = var.environment
    managed_by  = "terraform"
  }
}

# ─── Service Account — Fraud Loader ──────────────────────────────────────────
# The service account and its key already exist at secrets/fraud-loader-key.json.
# We reference it by email directly (no IAM API lookup required).
locals {
  fraud_loader_email = "fraud-loader@${var.gcp_project_id}.iam.gserviceaccount.com"
}

# ─── IAM: Fraud Loader → GCS bucket ─────────────────────────────────────────
resource "google_storage_bucket_iam_member" "fraud_loader_creator" {
  bucket = google_storage_bucket.paysim_landing.name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${local.fraud_loader_email}"
}

resource "google_storage_bucket_iam_member" "fraud_loader_viewer" {
  bucket = google_storage_bucket.paysim_landing.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${local.fraud_loader_email}"
}



