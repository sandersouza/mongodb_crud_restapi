"""Utility helpers for parsing user supplied values."""

from __future__ import annotations

import json
from typing import Any


def coerce_value(value: str) -> Any:
    """Attempt to coerce a string value into JSON, int, float or bool."""

    try:
        return json.loads(value)
    except (TypeError, json.JSONDecodeError):
        lowered = value.lower()
        if lowered in {"true", "false"}:
            return lowered == "true"
        return value
