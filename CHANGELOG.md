# Changelog

## 0.0.38

### Added

- Unified `PipelineOrchestrator` with injectable extractors, loaders, transforms, and optional `PipelineNotifier`
- `DynamoGsiExtractor`, `BigQueryLoader`, `DynamoRunStateStore`
- `MongoOplogExtractor`, `MongoRunStateStore`, `RedshiftLoader`
- Optional `MI_ETL/adapters/aws_lambda.py` reference handler
- Optional extras: `[dynamo]`, `[bigquery]`, `[notify]`, `[dev]`

### Changed

- **Breaking (Mongo users):** per-collection run state in `metadata.pipeline_runs`; cursors advance after successful load, not at extract start. Migrate or seed from legacy `metadata.metadata` (see README).

### Removed

- Dependency on missing `requirements3.txt` (replaced by `requirements.txt`)
