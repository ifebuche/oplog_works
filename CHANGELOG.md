# Changelog

## 0.1.1

### Changed

- Package metadata: author email order; `MANIFEST.in` includes `requirements.txt` for reproducible builds.

## 0.1.0

First minor release of the unified CDC ETL framework. Supersedes unreleased `0.0.38` development builds.

### Added

- Unified `PipelineOrchestrator` with injectable extractors, loaders, transforms, and optional `PipelineNotifier`
- `DynamoGsiExtractor`, `BigQueryLoader`, `DynamoRunStateStore`
- `MongoOplogExtractor`, `MongoRunStateStore`, `RedshiftLoader`
- Optional `MI_ETL/adapters/aws_lambda.py` reference handler
- Optional extras: `[dynamo]`, `[bigquery]`, `[notify]`, `[dev]`
- Example notebooks: `docs/examples/mongo_redshift_legacy.ipynb`, `docs/examples/dynamo_bq_example.ipynb`

### Changed

- **Breaking (Mongo users):** per-collection run state in `metadata.pipeline_runs`; cursors advance after successful load, not at extract start. Migrate or seed from legacy `metadata.metadata` (see README).

### Removed

- Dependency on missing `requirements3.txt` (replaced by `requirements.txt`)

## 0.0.38 (unreleased)

Internal development tag; use **0.1.1** (or **0.1.0**) for installs and upgrades.
