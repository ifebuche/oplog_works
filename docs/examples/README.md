# Examples

| File | Description |
|------|-------------|
| [mongo_redshift_legacy.ipynb](mongo_redshift_legacy.ipynb) | Legacy `DataExtraction` + `Loader` (Mongo oplog → Redshift/Postgres) |
| [dynamo_bq_example.ipynb](dynamo_bq_example.ipynb) | Framework API: DynamoDB GSI → BigQuery via `PipelineOrchestrator` |
| [dynamo_bq_example.py](dynamo_bq_example.py) | Same Dynamo/BQ wiring as a `.py` module for copy-paste into your service repo |

## Environment variables

### Mongo legacy notebook

| Variable | Purpose |
|----------|---------|
| `oplog_test_source_url` | MongoDB connection URI |
| `oplog_test_host` | Warehouse host |
| `oplog_test_user` | Warehouse user |
| `oplog_test_password` | Warehouse password |
| `oplog_test_db` | Warehouse database name |

Install: `pip install "MI-ETLx[dev]"` (or base package + `pymongo`, `psycopg2-binary`, `python-dotenv`).

### Dynamo → BigQuery notebook

| Variable | Purpose |
|----------|---------|
| `PIPELINE_RUNS_TABLE` | DynamoDB table for run metadata (default: `data-pipe-runs`) |
| `DYNAMO_SOURCE_TABLE` | Source DynamoDB table name |
| `BQ_WAREHOUSE_TABLE` | BigQuery table id `project.dataset.table` |
| `BQ_GSI_INDEX` | GSI name for incremental queries |
| `BQ_PRIMARY_KEY` | Merge primary key column |
| `BQ_SCHEMA_COLUMNS` | Comma-separated warehouse columns |
| `BQ_COLD_START_CURSOR` | ISO cursor for first run |
| `BQ_COLD_START_PARTITIONS` | Comma-separated GSI partition keys (e.g. `2025q4,2026q1`) |
| `BQ_SA_BASE64` | Base64-encoded service account JSON (or use ADC via `GOOGLE_APPLICATION_CREDENTIALS`) |
| `BQ_PROJECT_ID` | Optional GCP project when using `BQ_SA_BASE64` |

Install: `pip install "MI-ETLx[dynamo,bigquery]"`. AWS credentials via the default boto3 chain.
