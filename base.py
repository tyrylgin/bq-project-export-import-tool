import json
import logging
from google.cloud import bigquery, storage
import google.api_core.exceptions
from typing import Dict, Any

class BigQueryBase:
    """Base class for BigQuery operations."""

    def __init__(self, project_id: str, bucket_name: str, region: str = 'europe-north1'):
        self.project_id = project_id
        self.bucket_name = bucket_name
        self.bq_client = bigquery.Client(project=project_id)
        self.storage_client = storage.Client(project=project_id)

        self.region = region

        try:
            self.bucket = self.storage_client.get_bucket(bucket_name)
        except google.api_core.exceptions.NotFound:
            self.bucket = None
        except google.api_core.exceptions.Forbidden:
            logging.error(f"Access denied to bucket {bucket_name}. Please check the permissions. Ensure the service account has the necessary roles OR the name of new bucket is unique globally.")
            raise

        # Create bucket if not exists
        if not self.bucket:
            try:
                self.bucket = self.storage_client.create_bucket(bucket_name, location=self.region)
                logging.info(f"Bucket {self.bucket_name} created in {self.region}")
            except google.api_core.exceptions.Conflict:
                logging.error(f"Bucket {bucket_name} already exists in another project. Please choose a different bucket name. It should be unique globally.")
                raise


    def get_project_config(self) -> Dict[str, Any]:
        datasets = list(self.bq_client.list_datasets())
        return {
            'project_id': self.project_id,
            'datasets': [dataset.dataset_id for dataset in datasets]
        }

    def save_to_gcs(self, blob_name: str, data: Any) -> None:
        blob = self.bucket.blob(blob_name)
        blob.upload_from_string(json.dumps(data, indent=2, default=str))

    def read_from_gcs(self, blob_name: str) -> Any:
        blob = self.bucket.blob(blob_name)
        return json.loads(blob.download_as_text())
