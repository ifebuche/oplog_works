# MI-ETLx

**CDC-driven incremental ETL framework for operational stores → analytical warehouses**

Move only what changed since the last successful run — with per-table watermarks, schema-safe upserts, and pluggable sources (MongoDB oplog, DynamoDB GSI queries, and more).

## What it is (and is not)

MI-ETLx is a **composable Python framework** for CDC-style incremental pipelines: extract changed records, transform in your code, align schema, upsert into the warehouse, and log run metadata. You embed it in Lambda, cron, Airflow, or scripts.

It is **not** a hosted platform (no control plane, scheduler, or built-in table catalog). You supply `TableSpec` definitions, transforms, and credentials in your own service repo.

## Overview

Full-table reloads do not scale. Payloads grow, runtimes slip, and cost becomes hard to predict. MI-ETLx treats incremental load as a **first-class pipeline**: read a watermark from the last successful run, fetch only newer changes, load with merge/upsert semantics, and advance the cursor only after a successful load.

That turns “how much changed?” into a bounded, budgetable question instead of a full-scan gamble.

## Features

- **CDC-style incremental extract** — MongoDB via oplog tailing; DynamoDB via GSI queries on `updated` / `updated_at` (and similar cursor fields). No full-table scan by default.

- **Per-table run state** — Watermarks and run history per source table (DynamoDB run ledger or MongoDB `pipeline_runs`). Empty extracts do not advance the cursor.

- **Orchestrated pipeline** — `PipelineOrchestrator` wires extract → transform → schema align → load. Register your own transforms and table config; nothing production-specific is baked into the wheel.

- **Warehouse upserts** — BigQuery MERGE via staging tables; Redshift/Postgres temp-table delete + insert. Primary keys and schemas are yours to define.

- **Controlled payloads** — Runs move deltas, not whole datasets. Teams can reason about duration, row counts, and cost per table.

- **Optional notifications** — Slack and Google Chat summaries on success or failure; omit the notifier entirely if you only need logs and run metadata.

- **Deploy anywhere** — Library-first API with optional Lambda adapter. See [docs/deploying.md](docs/deploying.md).

## Quick start (framework API)

```python
from MI_ETL.core.orchestrator import PipelineOrchestrator
from MI_ETL.core.models import TableSpec
from MI_ETL.core.run_state import DynamoRunStateStore
from MI_ETL.core.schema import SchemaAligner
from MI_ETL.destinations.bigquery import BigQueryLoader
from MI_ETL.sources.dynamo_gsi import DynamoGsiExtractor
from MI_ETL.transforms.registry import TransformRegistry

# Your TableSpec list, transforms, and clients live in your application — see docs/examples/.
```

Reference wiring: [docs/examples/dynamo_bq_example.py](docs/examples/dynamo_bq_example.py). Tests: `python -m unittest discover -s tests`.

**Mongo users upgrading to 0.0.38:** per-collection run state replaces the global `metadata.metadata` cursor — see [CHANGELOG.md](CHANGELOG.md).

## Supported sources and destinations

| Role | Technology | Mechanism |
|------|------------|-----------|
| Source | MongoDB | Oplog CDC (`mongo_oplog`) |
| Source | DynamoDB | GSI incremental query on cursor + partition key (`dynamo_gsi`) |
| Destination | BigQuery | MERGE upsert (`bigquery`) |
| Destination | AWS Redshift / PostgreSQL | Temp-table upsert (`redshift`) |
| Destination | AWS S3 | Parquet upload (legacy `Loader` API) |

**Integrations** (icons load from CDN; see [docs/images/README.md](docs/images/README.md) to vendor local copies):

<p>
  <img src="https://cdn.jsdelivr.net/gh/devicons/devicon@latest/icons/mongodb/mongodb-original.svg" height="36" alt="MongoDB" title="MongoDB" />
  <img src="https://cdn.jsdelivr.net/gh/devicons/devicon@latest/icons/amazonwebservices/amazonwebservices-plain-wordmark.svg" height="36" alt="DynamoDB / AWS" title="DynamoDB (AWS)" />
  <img src="https://cdn.jsdelivr.net/gh/devicons/devicon@latest/icons/googlecloud/googlecloud-original.svg" height="36" alt="BigQuery" title="BigQuery" />
  <img src="https://cdn.jsdelivr.net/gh/devicons/devicon@latest/icons/postgresql/postgresql-original.svg" height="36" alt="PostgreSQL" title="PostgreSQL" />
  <img src="https://cdn.jsdelivr.net/gh/devicons/devicon@latest/icons/amazonwebservices/amazonwebservices-original-wordmark.svg" height="36" alt="Redshift / S3" title="Redshift / S3" />
</p>

## Installation

```bash
pip install MI-ETLx==0.0.38

# Optional capability groups
pip install "MI-ETLx[dynamo,bigquery,notify]"
```

For TestPyPI:

```bash
pip install -i https://test.pypi.org/simple/ MI-ETLx==0.0.38 --extra-index-url https://pypi.org/simple
```

## Quick start (legacy Mongo → Redshift API)

The original `DataExtraction` + `Loader` classes remain for MongoDB oplog → Redshift/Postgres workflows:

### Example 1

```python
from MI_ETL.Connector import Source
from MI_ETL.data_extraction import DataExtraction
from MI_ETL.loader import Loader
import os

host = os.getenv('oplog_test_host')
user = os.getenv('oplog_test_user')
password = os.getenv('oplog_test_password')
db = os.getenv('oplog_test_db')

required_params = {"host":host, "user":user, "password": password, "db":db}

if all(required_params.values()):
    conn = Source.mongo(os.getenv('oplog_test_source_url'))

    data_extraction = DataExtraction(connection=conn, extract_all=[], db='sample_analytics')
    extracted_data = data_extraction.extract_oplog_data()

    #Initiate loader
    # The MongoDB connection is also used to update the time metadata for the next run,
    # ensuring that each run's timing information is accurately recorded.
    loader = Loader(mongo_conn=conn, data=extracted_data, datalake=False, datawarehouse=True, aws={})

    # Provide connection to datawarehouse. NOTE !! only redshift and respective postgres dbs
    # are supported as at this release
    # Result holds meta data information about the load process highlighting if it passed or fail,
    # schema information , e.t.c
    result = loader.run(host=host, user=user, password=password, db=db, port=5432)
else:
    for key, val in required_params.items():
        if not val:
            print(f"'{key}' is needed for the destination database connection")
```

### Example 2

```python
#Example 2
from MI_ETL.Connector import Source
from MI_ETL.data_extraction import DataExtraction
from MI_ETL.loader import Loader
import os

host = os.getenv('oplog_test_host')
user = os.getenv('oplog_test_user')
password = os.getenv('oplog_test_password')
db = os.getenv('oplog_test_db')

required_params = {"host":host, "user":user, "password": password, "db":db}

if all(required_params.values()):
    conn = Source.mongo(os.getenv('oplog_test_source_url'))

    # Intitialize data extraction from specified collections 'collection_1' and 'collection_2' within 'sample_analytics' database.
    data_extraction = DataExtraction(connection=conn, extract_all=['collection_1', 'collection_2'], db='sample_analytics')
    extracted_data = data_extraction.extract_oplog_data()

    #Initiate loader
    # The MongoDB connection is also used to update the time metadata for the next run,
    # ensuring that each run's timing information is accurately recorded.
    loader = Loader(mongo_conn=conn, data=extracted_data, datalake=False, datawarehouse=True, aws={})

    # Provide connection to datawarehouse. NOTE !! only redshift and respective postgres dbs
    # are supported as at this release
    # Result holds meta data information about the load process highlighting if it passed or fail,
    # schema information , e.t.c
    result = loader.run(host=host, user=user, password=password, db=db, port=5432)
else:
    for key, val in required_params.items():
        if not val:
            print(f"'{key}' is needed for the destination database connection")
```

### Example 3

```python
from MI_ETL.Connector import Source
from MI_ETL.data_extraction import DataExtraction
from MI_ETL.loader import Loader
import os

host = os.getenv('oplog_test_host')
user = os.getenv('oplog_test_user')
password = os.getenv('oplog_test_password')
db = os.getenv('oplog_test_db')

required_params = {"host":host, "user":user, "password": password, "db":db}

if all(required_params.values()):
    conn = Source.mongo(os.getenv('oplog_test_source_url'))

    # Initialize data extraction from 'collection_1' and 'collection_2' in 'sample_analytics',
    # extracting from data modified after '2023/12/28' (backfill date).
    data_extraction = DataExtraction(connection=conn, extract_all=['collection_1', 'collection_2'],  db='sample_analytics', backfill='2023/12/28')
    extracted_data = data_extraction.extract_oplog_data()

    #Initiate loader
    # The MongoDB connection is also used to update the time metadata for the next run,
    # ensuring that each run's timing information is accurately recorded.
    loader = Loader(mongo_conn=conn, data=extracted_data, datalake=False, datawarehouse=True, aws={})

    # Provide connection to datawarehouse. NOTE !! only redshift and respective postgres dbs
    # are supported as at this release
    # Result holds meta data information about the load process highlighting if it passed or fail,
    # schema information , e.t.c
    result = loader.run(host=host, user=user, password=password, db=db, port=5432)
else:
    for key, val in required_params.items():
        if not val:
            print(f"'{key}' is needed for the destination database connection")
```
