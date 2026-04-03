"""Argument parser for ``hungary_ge.pipeline`` CLI."""

from __future__ import annotations

import argparse

from hungary_ge.pipeline.stages import register_stage_arguments
from hungary_ge.pipeline.stages.core import (
    DEFAULT_STAGES,
    STAGE_CHOICES,
    add_core_arguments,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Pilot pipeline: ETL precinct layer, electoral parquets, adjacency Parquet; "
            "optional Folium map (uv sync --extra viz). "
            "County mode graph writes adjacency + graph_health meta per megye and, "
            "by default, adjacency_map.html after each county (see --no-county-maps)."
        ),
    )
    add_core_arguments(parser)
    register_stage_arguments(parser)
    return parser


__all__ = ["DEFAULT_STAGES", "STAGE_CHOICES", "build_parser"]
