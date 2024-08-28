# BigQuery Project Export/Import Tool

This project is a tool for exporting and importing BigQuery project data to/from Google Cloud Storage. It allows you to:
Export BigQuery project components (routines, views, external tables, tables, scheduled queries) to Google Cloud Storage.
Import these components back into the BigQuery project from Google Cloud Storage.

## Environment Setup

1. Python 3.6 or later.
2. Create and activate a virtual environment:

```bash
pip install virtualenv
python -m venv venv
```

```bash
source venv/bin/activate # for Linux and macOS
venv\Scripts\activate # for Windows
```
3. Install dependencies:

```bash
pip install -r requirements.txt
```

## Google Cloud Platform Project Setup

1. Create a Service Account and download the credentials file in JSON format. To do this, go to [Google Cloud Console](https://console.cloud.google.com/), select the project, go to "IAM & Admin" -> "Service Accounts" and create a new service account. After creating the service account, add the roles `BigQuery Admin` and `Storage Admin`. Download the credentials file in JSON format.
2. Create a Google Cloud Storage bucket. To do this, go to [Google Cloud Console](https://console.cloud.google.com/), select the project, go to "Storage" -> "Storage" and create a new bucket.
3. Optionally, you can add permissions for the service account in each BigQuery project to be able to export/import BigQuery project components from a single service account within one account. To do this, add the service account in the "Settings" -> "Permissions" section of the BigQuery project and grant the `BigQuery Admin` and `Storage Admin` roles.

## Usage

To run the script, execute the command:

```bash
python main.py <mode> <project_id> <bucket_name> <credentials_path> [--components <components>] [--region REGION] [--download_after_export] [--upload_before_import] [--local_dir LOCAL_DIR]
```

- `<mode>` - script mode: `export` or `import`.
- `<project_id>` - BigQuery project ID.
- `<bucket_name>` - Google Cloud Storage bucket name.
- `<credentials_path>` - path to the credentials file.
- `[--components <components>]` - optional parameter, list of components to export/import. Possible values: `routines`, `views`, `external_tables`, `tables`, `scheduled_queries`. By default, all components are exported/imported.
- `--region` - optional parameter, Google Cloud region (default: `europe-north1`).
- `--download_after_export` - optional parameter, download all objects from the bucket after export.
- `--upload_before_import` - optional parameter, upload all objects to the bucket before import.
- `--local_dir` - optional parameter, local directory for downloading/uploading files.

After export, BigQuery project components will be saved in Google Cloud Storage in JSON and PARQUET formats.

To import, transfer the Google Cloud Storage bucket with the exported components to another project and run the import command.
Alternatively, you can use the `--upload_before_import`/`--download_before_import` and `--local_dir` parameters to upload files to the bucket before import.

### How to Transfer a Google Cloud Storage Bucket to Another Project

1. Create a new bucket in another project.
2. Copy files from the old bucket to the new bucket. You can use [Google Cloud Console](https://console.cloud.google.com/), [gsutil](https://cloud.google.com/storage/docs/gsutil), or [Google Cloud Storage Transfer Service](https://cloud.google.com/storage-transfer/docs).
3. Download the credentials file for the new project.

### WARNING

- If the `--download_after_export` and `--upload_before_import` parameters are not specified, the script will work with files in the bucket.
- If the `--download_after_export` and `--local_dir` parameters are specified, all files from the bucket will be downloaded to the local directory. Files with the same names will be overwritten.
- If the `--upload_before_import` and `--local_dir` parameters are specified, all files from the local directory will be uploaded to the bucket. Files with the same names will be overwritten.
- If the specified bucket does not exist, the script will create a new bucket, both for import and export.

### Logging
When the script is executed, a `log` file is created in the `./logs` directory, recording all actions and errors.
Pay attention to warnings (WARNING), as some views, routines, and scheduled queries depend on datasets in other projects. Such projects must be created in advance and possibly with the same `project_id`.

If warnings are ignored, views will not be created during import, and an error message will appear in the log:
```bash
ERROR - Error importing view <view_name> into <current_dataset_id>: 403 POST https://bigquery.googleapis.com/bigquery/v2/projects/<current_project_name>/datasets/<current_dataset_id>/tables?prettyPrint=false: Access Denied: Table <external_project_id>:<external_dataset_id>.<external_table>: User does not have permission to query table <external_project_id>:<external_dataset_id>.<external_table>, or perhaps it does not exist.
```
At the same time, routines and scheduled queries will be created but will not be able to execute.

**Examples:**

To export all BigQuery project components to Google Cloud Storage:

```bash
python main.py export my-project my-bucket /path/to/credentials.json
```

Export the project and download files locally:

```bash
python main.py export my-project my-bucket /path/to/credentials.json --download_after_export --local_dir /path/to/local/dir
```

To import all components from Google Cloud Storage to the BigQuery project:

```bash
python main.py import my-project my-bucket /path/to/credentials.json
```

To import all components from Google Cloud Storage to the BigQuery project and upload files to the bucket before import:

```bash
python main.py import my-project my-bucket /path/to/credentials.json --upload_before_import --local_dir /path/to/local/dir
```
