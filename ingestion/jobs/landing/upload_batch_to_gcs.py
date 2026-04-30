"""
upload_batch_to_gcs.py
----------------------
Simulates real-world batch arrival by uploading a single CSV batch file
from dataset/batches_output/ into the GCS landing/incoming/ zone.

In production this would be replaced by an external feed or event trigger.
Usage:
    python -m ingestion.jobs.landing.upload_batch_to_gcs --batch batch_0001_161501rows.csv
"""

import argparse
import os
from loguru import logger
from google.cloud import storage
from ingestion.config.settings import GCP_BUCKET, INCOMING_PREFIX


def upload_batch(batch_filename: str, local_batches_dir: str = "dataset/batches_output") -> None:
    local_path = os.path.join(local_batches_dir, batch_filename)

    if not os.path.exists(local_path):
        raise FileNotFoundError(f"Batch file not found: {local_path}")

    client = storage.Client()
    bucket = client.bucket(GCP_BUCKET)

    destination_blob_name = f"{INCOMING_PREFIX}paysim/{batch_filename}"
    blob = bucket.blob(destination_blob_name)

    logger.info(f"Uploading {batch_filename} → gs://{GCP_BUCKET}/{destination_blob_name}")
    blob.upload_from_filename(local_path)
    logger.info(f"Upload complete: {batch_filename}")


def main():
    parser = argparse.ArgumentParser(description="Upload a PaySim batch CSV to GCS landing zone")
    parser.add_argument(
        "--batch",
        required=True,
        help="Filename of the batch to upload, e.g. batch_0001_161501rows.csv"
    )
    parser.add_argument(
        "--batches-dir",
        default="dataset/batches_output",
        help="Local directory containing batch files (default: dataset/batches_output)"
    )
    args = parser.parse_args()

    upload_batch(batch_filename=args.batch, local_batches_dir=args.batches_dir)

## batch_0003_166326rows.csv
if __name__ == "__main__":
    main()

