"""
Optional AWS Lambda entrypoint. Users wire secrets and TableSpec list in their deploy repo.
"""

from __future__ import annotations

import json
import os
from typing import Any, Optional

from ..core.models import RunOptions, TableSpec
from ..core.orchestrator import PipelineOrchestrator


def parse_run_options(event: Optional[dict]) -> RunOptions:
    if not event:
        return RunOptions()
    return RunOptions(
        tables=event.get("tables"),
        backfill_from=event.get("backfill_from"),
        force_partitions=event.get("force_partitions"),
    )


def handler(event: Optional[dict], context: Any) -> dict:
    """
    Reference Lambda handler. Expects MI_ETL_PIPELINE_FACTORY env or subclass in deploy repo.

    Set env MI_ETL_SPECS_MODULE to a dotted path returning (orchestrator, specs) if needed.
    """
    factory_path = os.environ.get("MI_ETL_PIPELINE_FACTORY")
    if not factory_path:
        return {
            "status": "error",
            "message": "Set MI_ETL_PIPELINE_FACTORY to 'module:function' building PipelineOrchestrator",
        }

    module_name, func_name = factory_path.rsplit(":", 1)
    import importlib

    module = importlib.import_module(module_name)
    build = getattr(module, func_name)
    orchestrator, specs = build(event, context)
    options = parse_run_options(event if isinstance(event, dict) else None)
    results = orchestrator.run_all(specs, options)
    return {"status": "success", "results": results}
