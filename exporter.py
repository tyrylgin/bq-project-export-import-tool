import logging
from google.cloud import bigquery, bigquery_datatransfer_v1
from tqdm.auto import tqdm
import concurrent.futures
import re
from datetime import datetime
from typing import List, Optional
from utils import BigQueryUtils
from base import BigQueryBase
import os

def log_projects_in_sql_query(query: str) -> None:
    """Logs the project IDs found in the view query."""
    project_ids = set(re.findall(r'`([^`]+)\.\w+\.\w+`', query))
    if project_ids:
        logging.warning(f"Projects used in view query: {', '.join(project_ids)}. Ensure these projects are created first.")

def log_projects_in_scheduled_queries(params: dict) -> None:
    """Logs the project IDs found in the scheduled query parameters."""
    if 'query' in params:
        project_ids = set(re.findall(r'`([^`]+)\.\w+\.\w+`', params['query']))
        if project_ids:
            logging.warning(f"Projects used in scheduled query: {', '.join(project_ids)}. Ensure these projects are created first.")

class BigQueryExporter(BigQueryBase):
    """Class for exporting data from BigQuery to Google Cloud Storage."""

    def __init__(self, project_id: str, bucket_name: str, progress_bar: tqdm, region: str = 'europe-north1'):
        super().__init__(project_id, bucket_name, region)
        self.progress_bar = progress_bar

    def download_all_project_objects(self, local_dir: Optional[str] = None) -> None:
        if local_dir is None:
            local_dir = f"./{self.project_id}_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        os.makedirs(local_dir, exist_ok=True)

        def download_file(blob, local_path):
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            blob.download_to_filename(local_path)
            logging.info(f"Downloaded {blob.name} to {local_path}")

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            futures = []
            blobs = self.bucket.list_blobs()
            for blob in blobs:
                local_path = os.path.join(local_dir, blob.name)
                futures.append(executor.submit(download_file, blob, local_path))
            for future in concurrent.futures.as_completed(futures):
                future.result()  # Raise exception if any

    def get_all_regions(self) -> List[str]:
        try:
            datasets = list(self.bq_client.list_datasets())
            regions = set()
            for dataset in datasets:
                full_dataset = self.bq_client.get_dataset(dataset.dataset_id)
                regions.add(full_dataset.location)

            logging.info(f"Detected regions with datasets: {', '.join(regions)}")
            return list(regions)
        except Exception as e:
            logging.error(f"Error fetching regions: {str(e)}")
            fallback_regions = ['US', 'EU', 'europe-north1']
            logging.info(f"Using fallback regions: {', '.join(fallback_regions)}")
            return fallback_regions

    def export_routines(self, dataset_id: str) -> None:
        try:
            routines = list(self.bq_client.list_routines(dataset_id))
            serialized_routines = []
            for routine in routines:
                routine_ref = self.bq_client.get_routine(routine.reference) # Workaround to fetch routine metadata
                routine_body = routine_ref.body.replace(f"{self.project_id}.", "")
                routine.body = routine_body

                log_projects_in_sql_query(routine.body)

                if not routine_ref.body:
                    logging.warning(f"Skipping routine {routine.routine_id} in {dataset_id} due to empty body")
                    continue
                serialized_routines.append(BigQueryUtils.serialize_routine(routine))

            self.save_to_gcs(f"{dataset_id}_routines.json", serialized_routines)
            logging.info(f"Exported {len(serialized_routines)} routines from {dataset_id}")
        except Exception as e:
            logging.error(f"Error exporting routines from {dataset_id}: {str(e)}")
        finally:
            self.progress_bar.update(1)

    def export_views(self, dataset_id: str) -> None:
        try:
            tables = list(self.bq_client.list_tables(dataset_id))
            views = [self.bq_client.get_table(table.reference) for table in tables if table.table_type == 'VIEW']
            serialized_views = []
            for view in views:
                view_query = view.view_query.replace(f"{self.project_id}.", "")
                view.view_query = view_query
                # Log external project IDs found in the view query, they need to be created first
                log_projects_in_sql_query(view.view_query)
                serialized_views.append(BigQueryUtils.serialize_view(view))
            self.save_to_gcs(f"{dataset_id}_views.json", serialized_views)
            logging.info(f"Exported {len(views)} views from {dataset_id}")
        except Exception as e:
            logging.error(f"Error exporting views from {dataset_id}: {str(e)}")
        finally:
            self.progress_bar.update(1)

    def export_external_tables(self, dataset_id: str) -> None:
        try:
            tables = list(self.bq_client.list_tables(dataset_id))
            ext_tables = [self.bq_client.get_table(table.reference) for table in tables if table.table_type == 'EXTERNAL']
            serialized_ext_tables = [BigQueryUtils.serialize_external_table(ext_table) for ext_table in ext_tables]
            self.save_to_gcs(f"{dataset_id}_external_tables.json", serialized_ext_tables)
            logging.info(f"Exported {len(ext_tables)} external tables from {dataset_id}")
        except Exception as e:
            logging.error(f"Error exporting external tables from {dataset_id}: {str(e)}")
        finally:
            self.progress_bar.update(1)

    def export_table(self, dataset_id: str, table_id: str) -> None:
        try:
            destination_uri = f"gs://{self.bucket_name}/{dataset_id}/{table_id}/*.parquet"
            dataset_ref = self.bq_client.dataset(dataset_id, project=self.project_id)
            table_ref = dataset_ref.table(table_id)

            # Fetch the table schema, because it is not available in the Table object
            table = self.bq_client.get_table(table_ref)
            schema = [field.to_api_repr() for field in table.schema]

            job_config = bigquery.ExtractJobConfig()
            job_config.compression = bigquery.Compression.GZIP
            job_config.destination_format = bigquery.DestinationFormat.PARQUET

            extract_job = self.bq_client.extract_table(
                table_ref,
                destination_uri,
                job_config=job_config
            )
            extract_job.result()  # Wait for the job to complete

            # Save the schema to GCS
            schema_file = f"{dataset_id}/{table_id}_schema.json"
            self.save_to_gcs(schema_file, schema)

            logging.info(f"Table {dataset_id}.{table_id} exported to {destination_uri}")
        except Exception as e:
            logging.error(f"Error exporting table {dataset_id}.{table_id}: {str(e)}")
        finally:
            self.progress_bar.update(1)

    def export_scheduled_queries(self) -> None:
        try:
            regions = self.get_all_regions()
            all_queries = []

            for region in regions:
                try:
                    client = bigquery_datatransfer_v1.DataTransferServiceClient()
                    parent = f"projects/{self.project_id}/locations/{region}"

                    transfers = list(client.list_transfer_configs(parent=parent))
                    scheduled_queries = [
                        transfer for transfer in transfers
                        if transfer.data_source_id == 'scheduled_query'
                    ]

                    for query in scheduled_queries:
                        # Log external project IDs found in the scheduled query parameters
                        query.params['query'] = query.params['query'].replace(f"{self.project_id}.", "")
                        log_projects_in_scheduled_queries(query.params)
                        serialized_query = BigQueryUtils.serialize_scheduled_query(query)
                        all_queries.append(serialized_query)

                    logging.info(f"Exported {len(scheduled_queries)} scheduled queries from region {region}")
                except Exception as e:
                    logging.error(f"Error exporting scheduled queries from region {region}: {str(e)}")

            self.save_to_gcs("scheduled_queries.json", all_queries)
            logging.info(f"Exported a total of {len(all_queries)} scheduled queries from all regions")

            if not all_queries:
                logging.warning("No scheduled queries found in any region. This might be unexpected.")
        except Exception as e:
            logging.error(f"Error exporting scheduled queries: {str(e)}")
            raise
        finally:
            self.progress_bar.update(1)

    def export_project(self, components: Optional[List[str]] = None, download_after_export: bool = False, local_dir: Optional[str] = None) -> None:
        if components is None:
            components = ['routines', 'views', 'external_tables', 'tables', 'scheduled_queries']

        config = self.get_project_config()
        self.save_to_gcs(f"{self.project_id}_config.json", config)

        # max_workers=8 because the BigQuery API has a limit of 10 concurrent requests
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            futures = []

            for dataset_id in config['datasets']:
                if 'routines' in components:
                    futures.append(executor.submit(self.export_routines, dataset_id))
                if 'views' in components:
                    futures.append(executor.submit(self.export_views, dataset_id))
                if 'external_tables' in components:
                    futures.append(executor.submit(self.export_external_tables, dataset_id))
                if 'tables' in components:
                    tables = list(self.bq_client.list_tables(dataset_id))
                    for table in tables:
                        if table.table_type == 'TABLE':
                            futures.append(executor.submit(self.export_table, dataset_id, table.table_id))

            if 'scheduled_queries' in components:
                self.export_scheduled_queries()

            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logging.error(f"Error in export operation: {str(e)}")

        logging.info(f"Export of project {self.project_id} completed")

        if download_after_export:
            self.download_all_project_objects(local_dir)
            logging.info(f"Downloaded all exported objects to local directory {local_dir}")
