import logging

from google.cloud import bigquery, bigquery_datatransfer_v1
from google.api_core.exceptions import NotFound
from tqdm import tqdm
from typing import List, Optional
import concurrent.futures
from base import BigQueryBase
import os

class BigQueryImporter(BigQueryBase):
    """Class for importing data from Google Cloud Storage to BigQuery."""

    def __init__(self, project_id: str, bucket_name: str, progress_bar: tqdm, region: str = 'europe-north1'):
        super().__init__(project_id, bucket_name, region)
        self.progress_bar = progress_bar

    def upload_all_project_objects(self, local_dir: Optional[str]) -> None:
        """Upload all objects from the local directory to the GCS bucket."""
        if not os.path.exists(local_dir):
            logging.warning(f"Local directory {local_dir} does not exist. Skipping upload.")
            return

        def upload_file(local_path, blob_name):
            blob = self.bucket.blob(blob_name)
            blob.upload_from_filename(local_path)
            logging.info(f"Uploaded {local_path} to gs://{self.bucket_name}/{blob_name}")

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            futures = []
            for root, _, files in os.walk(local_dir):
                for file in files:
                    local_path = os.path.join(root, file)
                    blob_name = os.path.relpath(local_path, local_dir)
                    futures.append(executor.submit(upload_file, local_path, blob_name))
            for future in concurrent.futures.as_completed(futures):
                future.result()  # Raise exception if any

    def import_routines(self, dataset_id: str) -> None:
        try:
            routines_data = self.read_from_gcs(f"{dataset_id}_routines.json")
            for routine_data in routines_data:
                routine = bigquery.Routine(
                    f"{self.project_id}.{dataset_id}.{routine_data['routine_id']}",
                    type_=routine_data['type_'],
                    language=routine_data['language'],
                    body=routine_data['body'],
                    location=self.region,
                    arguments=[bigquery.RoutineArgument(
                        name=arg['name'],
                        data_type=arg['data_type'],
                        mode=arg['mode']
                    ) for arg in routine_data['arguments']]
                )
                routine.description = routine_data['description']
                self.bq_client.create_routine(routine)
            logging.info(f"Imported {len(routines_data)} routines into {dataset_id}")
        except Exception as e:
            logging.error(f"Error importing routines into {dataset_id}: {str(e)}")
        finally:
            self.progress_bar.update(1)
            self.progress_bar.refresh()

    def import_views(self, dataset_id: str) -> None:
        try:
            views_data = self.read_from_gcs(f"{dataset_id}_views.json")
            for view_data in views_data:
                try:
                    view = bigquery.Table(f"{self.project_id}.{dataset_id}.{view_data['table_id']}")
                    view.view_query = view_data['view_query']
                    view.description = view_data['description']
                    self.bq_client.create_table(view)
                    logging.info(f"Imported view {view_data['table_id']} into {dataset_id}")
                except Exception as e:
                    logging.error(f"Error importing view {view_data['table_id']} into {dataset_id}: {str(e)}")
        except Exception as e:
            logging.error(f"Error reading views data for {dataset_id}: {str(e)}")
        finally:
            self.progress_bar.update(1)
            self.progress_bar.refresh()

    def import_external_tables(self, dataset_id: str) -> None:
        try:
            ext_tables_data = self.read_from_gcs(f"{dataset_id}_external_tables.json")
            for ext_table_data in ext_tables_data:
                ext_table = bigquery.Table(f"{self.project_id}.{dataset_id}.{ext_table_data['table_id']}")
                ext_table.schema = [bigquery.SchemaField.from_api_repr(field) for field in ext_table_data['schema']]
                ext_table.external_data_configuration = bigquery.ExternalConfig.from_api_repr(
                    ext_table_data['external_data_configuration']
                )
                ext_table.description = ext_table_data['description']
                self.bq_client.create_table(ext_table)
            logging.info(f"Imported {len(ext_tables_data)} external tables into {dataset_id}")
        except Exception as e:
            logging.error(f"Error importing external tables into {dataset_id}: {str(e)}")
        finally:
            self.progress_bar.update(1)
            self.progress_bar.refresh()

    def import_table(self, dataset_id: str, table_id: str) -> None:
        try:
            job_config = bigquery.LoadJobConfig()
            job_config.source_format = bigquery.SourceFormat.PARQUET

            # Read the schema from GSC
            schema_file_path = f"{dataset_id}/{table_id}_schema.json"
            schema = self.read_from_gcs(schema_file_path)
            job_config.schema = [bigquery.SchemaField.from_api_repr(field) for field in schema]

            uri = f"gs://{self.bucket_name}/{dataset_id}/{table_id}/*.parquet"
            table_ref = self.bq_client.dataset(dataset_id).table(table_id)

            load_job = self.bq_client.load_table_from_uri(uri, table_ref, job_config=job_config, location=self.region)
            load_job.result()  # Wait for the job to complete

            logging.info(f"Table {self.project_id}.{dataset_id}.{table_id} imported from {uri}")
        except Exception as e:
            logging.error(f"Error importing table {dataset_id}.{table_id}: {str(e)}")
        finally:
            self.progress_bar.update(1)
            self.progress_bar.refresh()

    def import_scheduled_queries(self) -> None:
        """Imports scheduled queries into the project."""
        try:
            queries_data = self.read_from_gcs("scheduled_queries.json")
            client = bigquery_datatransfer_v1.DataTransferServiceClient()

            for query_data in queries_data:
                parent = f"projects/{self.project_id}/locations/{self.region}"
                transfer_config = bigquery_datatransfer_v1.TransferConfig(
                    display_name=query_data['display_name'],
                    data_source_id=query_data['data_source_id'],
                    params={"query": query_data['params_query']},
                    schedule=query_data['schedule'],
                    destination_dataset_id=query_data['destination_dataset_id'],
                    disabled=query_data['disabled'],
                )

                response = client.create_transfer_config(
                    parent=parent,
                    transfer_config=transfer_config
                )
                logging.info(f"Created scheduled query: {response.name}")

            logging.info(f"Imported {len(queries_data)} scheduled queries")
        except Exception as e:
            logging.error(f"Error importing scheduled queries: {str(e)}")
        finally:
            self.progress_bar.update(1)
            self.progress_bar.refresh()

    def import_project(self, components: Optional[List[str]] = None, upload_before_import: bool = False, local_dir: Optional[str] = None) -> None:
        if components is None:
            components = ['routines', 'views', 'external_tables', 'tables', 'scheduled_queries']

        if upload_before_import:
            self.upload_all_project_objects(local_dir)
            logging.info(f"Uploaded all local objects from {local_dir} to GCS bucket {self.bucket_name}")

        blobs = self.bucket.list_blobs()
        config_file_name = None
        for blob in blobs:
            if blob.name.endswith('_config.json'):
                config_file_name = blob.name
                break

        if not config_file_name:
            raise FileNotFoundError("Configuration file with '_config.json' suffix not found in the bucket")

        config = self.read_from_gcs(config_file_name)

        for dataset_id in config['datasets']:
            try:
                self.bq_client.get_dataset(dataset_id)
            except NotFound:
                dataset_ref = self.bq_client.dataset(dataset_id)
                dataset = bigquery.Dataset(dataset_ref)
                dataset.location = self.region
                self.bq_client.create_dataset(dataset)

            # WARNING: The import order is important!

            if 'tables' in components:
                blobs = self.bucket.list_blobs(prefix=f"{dataset_id}/")
                table_ids = set(blob.name.split('/')[1] for blob in blobs if blob.name.endswith('.parquet'))

                # Imports do in parallel only for tables, because others are dependent on them
                # max_workers=8 because the BigQuery API has a limit of 10 concurrent requests
                with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
                    futures = [executor.submit(self.import_table, dataset_id, table_id) for table_id in table_ids]
                    for future in concurrent.futures.as_completed(futures):
                        future.result()  # Raise exception if any

            if 'external_tables' in components:
                self.import_external_tables(dataset_id)

            if 'views' in components:
                self.import_views(dataset_id)

            if 'routines' in components:
                self.import_routines(dataset_id)

        if 'scheduled_queries' in components:
            self.import_scheduled_queries()

        logging.info(f"Import of project {self.project_id} completed")
