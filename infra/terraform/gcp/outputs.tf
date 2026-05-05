###############################################################################
# Outputs — GCP module
###############################################################################

output "landing_bucket_name" {
  description = "GCS bucket used for raw batch landing"
  value       = google_storage_bucket.paysim_landing.name
}

output "landing_bucket_url" {
  description = "gs:// URL of the landing bucket"
  value       = "gs://${google_storage_bucket.paysim_landing.name}"
}

output "fraud_loader_sa_email" {
  description = "Email of the fraud-loader service account"
  value       = local.fraud_loader_email
}

output "fraud_loader_key_path" {
  description = "Local path to the fraud-loader service account key used by Terraform"
  value       = var.fraud_loader_key_path
}


