"""Dynamic route discovery for FastAPI routers."""

from __future__ import annotations

import importlib
import pkgutil
from pathlib import Path
from typing import Iterable, List

from fastapi import APIRouter


def discover_routers() -> List[APIRouter]:
    """Import every module under ``routes`` and collect their routers."""

    package_dir = Path(__file__).resolve().parent
    routers: List[APIRouter] = []

    for module_info in pkgutil.iter_modules([str(package_dir)]):
        if module_info.ispkg or module_info.name.startswith("__"):
            continue
        module = importlib.import_module(f"{__name__}.{module_info.name}")
        router = getattr(module, "router", None)
        if isinstance(router, APIRouter):
            routers.append(router)

    return routers


def include_routers(app, routers: Iterable[APIRouter], prefix: str = "") -> None:
    """Attach every discovered router to the given application."""

    for router in routers:
        router_prefix = "" if getattr(router, "skip_app_prefix", False) else prefix
        app.include_router(router, prefix=router_prefix)
