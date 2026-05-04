import os

from google.cloud import storage
from loguru import logger


def upload_batch_to_gcs(
    batch_filename: str,
    bucket_name: str,
    incoming_prefix: str,
    local_batches_dir: str = "dataset/batches_output",
) -> None:
    """
    Upload a batch CSV file to GCS landing zone.

    Args:
        batch_filename: name of the CSV file
        bucket_name: GCS bucket name
        incoming_prefix: prefix inside the bucket (e.g. 'landing/incoming/')
        local_batches_dir: local directory containing batches
    """

    local_path = os.path.join(local_batches_dir, batch_filename)

    if not os.path.exists(local_path):
        raise FileNotFoundError(f"Batch file not found: {local_path}")

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    destination_blob_name = f"{incoming_prefix}paysim/{batch_filename}"
    blob = bucket.blob(destination_blob_name)

    logger.info(f"Uploading {batch_filename} → gs://{bucket_name}/{destination_blob_name}")

    blob.upload_from_filename(local_path)

    logger.info(f"Upload complete: {batch_filename}")
