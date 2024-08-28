import argparse
import logging
from tqdm.auto import tqdm
from base import BigQueryBase
from exporter import BigQueryExporter
from importer import BigQueryImporter
import os
from datetime import datetime

# Need to define handler class for logging preventing tqdm from breaking
class TqdmLoggingHandler(logging.Handler):
    def __init__(self, level=logging.NOTSET):
        super().__init__(level)

    def emit(self, record):
        try:
            msg = self.format(record)
            tqdm.write(msg)
            self.flush()
        except Exception:
            self.handleError(record)

def setup_auth(credentials_path: str) -> None:
    if not os.path.exists(credentials_path):
        logging.error(f"Credentials file not found: {credentials_path}")
        raise FileNotFoundError(f"Credentials file not found: {credentials_path}")

    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
    logging.info(f"Using credentials from {credentials_path}")

def count_tasks(config, components, bucket_name, operation):
    """Counts the total number of tasks based on the components and datasets."""
    total_tasks = 0
    datasets = config['datasets']

    bq_base = BigQueryBase(config['project_id'], bucket_name)

    if 'routines' in components:
        total_tasks += len(datasets)
    if 'views' in components:
        total_tasks += len(datasets)
    if 'external_tables' in components:
        total_tasks += len(datasets)
    if 'tables' in components:
        for dataset_id in datasets:
            if operation == 'export':
                tables = list(bq_base.bq_client.list_tables(dataset_id))
                total_tasks += len([table for table in tables if table.table_type == 'TABLE'])
            if operation == 'import':
                blobs = bq_base.bucket.list_blobs(prefix=f"{dataset_id}/")
                table_ids = set(blob.name.split('/')[1] for blob in blobs if blob.name.endswith('.parquet'))
                total_tasks += len(table_ids)
    if 'scheduled_queries' in components:
        total_tasks += 1  # One task for all scheduled queries

    return total_tasks

def main():
    parser = argparse.ArgumentParser(description="BigQuery Project Export/Import Tool")
    parser.add_argument("mode", choices=["export", "import"], help="Operation mode: export or import")
    parser.add_argument("project_id", help="Google Cloud project ID")
    parser.add_argument("bucket_name", help="Google Cloud Storage bucket name for saving/reading data")
    parser.add_argument("credentials_path", help="Path to the Google Cloud credentials JSON file")
    parser.add_argument("--components", nargs='+', choices=['routines', 'views', 'external_tables', 'tables', 'scheduled_queries'],
                        help="Components to export/import (default: all)")
    parser.add_argument("--region", default="europe-north1", help="Google Cloud region (default: europe-north1)")
    parser.add_argument("--download_after_export", action='store_true', help="Download all objects from the bucket after export")
    parser.add_argument("--upload_before_import", action='store_true', help="Upload all objects to the bucket before import")
    parser.add_argument("--local_dir", help="Local directory for uploading/downloading files")

    args = parser.parse_args()

    try:
        # Ensure the logs directory exists
        os.makedirs('logs', exist_ok=True)
        log_file_name = f"logs/{args.mode}-{args.project_id}-{datetime.now().strftime('%Y%m%d%H%M%S')}.log"
        logging.basicConfig(
            filename=log_file_name,
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        handler = TqdmLoggingHandler()
        handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
        logging.getLogger().addHandler(handler)
        logging.getLogger().setLevel(logging.INFO)

        setup_auth(args.credentials_path)

        config = BigQueryBase(args.project_id, args.bucket_name, args.region).get_project_config()
        components = args.components or ['routines', 'views', 'external_tables', 'tables', 'scheduled_queries']
        total_tasks = count_tasks(config, components, args.bucket_name, args.mode)

        with tqdm(total=total_tasks, desc=f"{args.mode.capitalize()}ing project", position=0, leave=True) as progress_bar:
            if args.mode == "export":
                exporter = BigQueryExporter(args.project_id, args.bucket_name, progress_bar, args.region)
                exporter.export_project(components, args.download_after_export, args.local_dir)
            elif args.mode == "import":
                importer = BigQueryImporter(args.project_id, args.bucket_name, progress_bar, args.region)
                importer.import_project(components, args.upload_before_import, args.local_dir)
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        raise

if __name__ == "__main__":
    main()
