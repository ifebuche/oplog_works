from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal
from typing import Any


def now_now_str() -> str:
    return str(datetime.now().astimezone())


def iso_tz_to_epoch(iso_tz_str: str) -> float:
    dt_obj = datetime.fromisoformat(iso_tz_str.replace(" ", "T"))
    return dt_obj.timestamp()


def bytes_to_mb(bytes_size: int, binary: bool = False) -> float:
    divisor = 1024**2 if binary else 1000**2
    return bytes_size / divisor


def default_serializer(obj: Any) -> Any:
    if isinstance(obj, Decimal):
        return float(obj)
    return obj


def data_volume(data: list[dict]) -> tuple[Optional[int], Optional[float]]:
    try:
        data_size = sum(
            len(json.dumps(item, separators=(",", ":"), default=default_serializer).encode("utf-8"))
            for item in data
        )
        return data_size, bytes_to_mb(data_size)
    except Exception:
        return None, None


def number_to_decimal(data: dict) -> tuple[bool, dict]:
    if not isinstance(data, dict):
        return False, data

    for key, value in list(data.items()):
        if isinstance(value, (float, int)):
            data[key] = round(Decimal(str(value)), 2)
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    number_to_decimal(item)
        elif isinstance(value, dict):
            number_to_decimal(value)
    return True, data
