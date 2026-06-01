# Deploying MI-ETLx

MI-ETLx is a **library**. You provide `TableSpec` definitions, transforms, credentials, and optional notifications.

## Install

```bash
pip install -e ".[dynamo,bigquery,notify]"
```

## Programmatic usage

```python
from MI_ETL.core.models import TableSpec
from MI_ETL.core.orchestrator import PipelineOrchestrator
from MI_ETL.core.run_state import DynamoRunStateStore
from MI_ETL.core.schema import SchemaAligner
from MI_ETL.destinations.bigquery import BigQueryLoader
from MI_ETL.sources.dynamo_gsi import DynamoGsiExtractor
from MI_ETL.transforms.registry import TransformRegistry

# Register your transforms and specs in your service repo (not shipped in the wheel).
```

## AWS Lambda (optional)

- Package with optional extras to keep the zip small.
- Point the handler at `MI_ETL.adapters.aws_lambda.handler`.
- Set `MI_ETL_PIPELINE_FACTORY=your_module:build_orchestrator` returning `(orchestrator, specs)`.
- Inject secrets at invoke time; leave `notifier=None` if webhooks are unavailable.

## Tests

From the repository root:

```bash
python -m unittest discover -s tests -p "test_*.py"
```

Tests live under `tests/` and are **not** installed with the package.
