"""
Upload a PaySim batch CSV to GCS landing zone.
This module is designed to be used by Airflow (no CLI).
"""

from ingestion.config.settings import GCP_BUCKET, INCOMING_PREFIX
from ingestion.core.landing.upload_to_gcs import upload_batch_to_gcs


def run_upload_batch(batch_filename: str, local_batches_dir: str = "dataset/batches_output") -> None:
    """Airflow-callable wrapper — same as upload_batch."""
    upload_batch(batch_filename=batch_filename, local_batches_dir=local_batches_dir)


def upload_batch(batch_filename: str, local_batches_dir: str = "dataset/batches_output") -> None:

    if not batch_filename:
        raise ValueError("batch_filename is required")

    upload_batch_to_gcs(
        batch_filename=batch_filename,
        bucket_name=GCP_BUCKET,
        incoming_prefix=INCOMING_PREFIX,
        local_batches_dir=local_batches_dir,
    )