"""Processed GeoJSON I/O (canonical precinct layer).

Full ``szavkor_topo`` → GeoJSON conversion will live here or in a companion
module; stubs keep import boundaries stable per docs/data-model.md.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

# Type alias for GeoPandas GeoDataFrame once geopandas is a dependency.
GeoDataFrame = Any


def load_processed_geojson(path: str | Path) -> GeoDataFrame:
    """Load precinct GeoJSON from ``data/processed/`` (or similar)."""
    raise NotImplementedError(
        "load_processed_geojson: add geopandas and read_file implementation"
    )


def write_processed_geojson(gdf: GeoDataFrame, path: str | Path) -> None:
    """Write a precinct GeoDataFrame to GeoJSON."""
    raise NotImplementedError(
        "write_processed_geojson: add geopandas to_file implementation"
    )


def load_szavkor_settlement_json(path: str | Path) -> dict[str, Any]:
    """Load one settlement file from ``data/raw/szavkor_topo``."""
    raise NotImplementedError(
        "load_szavkor_settlement_json: implement JSON parse for szavkor_topo"
    )
