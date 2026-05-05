###############################################################################
# Variables — GCP module
###############################################################################

variable "gcp_project_id" {
  description = "GCP project ID where resources will be created"
  type        = string
}

variable "gcp_region" {
  description = "Default GCP region for regional resources"
  type        = string
  default     = "us-central1"
}

variable "gcs_bucket_location" {
  description = "GCS bucket location — use a single region (e.g. us-central1) not a multi-region (e.g. US)"
  type        = string
  default     = "us-central1"
}

variable "environment" {
  description = "Deployment environment tag (dev | staging | prod)"
  type        = string
  default     = "dev"
}

variable "fraud_loader_key_path" {
  description = "Path to the existing fraud-loader service account JSON key"
  type        = string
  default     = "../../../secrets/fraud-loader-key.json"
}


