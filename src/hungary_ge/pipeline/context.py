"""Shared execution context for pipeline stages."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from hungary_ge.config import ProcessedPaths


@dataclass
class PipelineContext:
    """Resolved paths and run metadata passed to each stage ``run(ctx)``."""

    args: Any
    repo_root: Path
    paths: ProcessedPaths
    pq_graph: Path
    stages: list[str]
    exclude_maz_set: frozenset[str] | None
    run_id: str | None
    szavkor: Path
