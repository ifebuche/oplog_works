from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Optional

TransformFn = Callable[[list[dict]], tuple[bool, list[dict] | str]]


@dataclass
class TransformResult:
    ok: bool
    load_targets: list = field(default_factory=list)
    message: Optional[str] = None


class TransformRegistry:
    """User-registered per-source transform functions."""

    def __init__(self):
        self._handlers: dict[str, TransformFn] = {}

    def register(self, source_name: str, handler: TransformFn) -> None:
        self._handlers[source_name] = handler

    def transform(self, source_name: str, data: list[dict]) -> TransformResult:
        handler = self._handlers.get(source_name)
        if not handler:
            return TransformResult(ok=False, message=f"No transformer registered for '{source_name}'")
        ok, outcome = handler(data)
        if not ok:
            return TransformResult(ok=False, message=str(outcome))
        if not isinstance(outcome, list):
            return TransformResult(ok=False, message="Transformer must return list of load targets")
        return TransformResult(ok=True, load_targets=outcome)
