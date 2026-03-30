"""Processed GeoJSON / GeoParquet I/O and raw settlement JSON loading."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import geopandas as gpd
from geopandas import GeoDataFrame


def load_szavkor_settlement_json(path: str | Path) -> dict[str, Any]:
    """Load one settlement file from ``data/raw/szavkor_topo``."""
    path = Path(path)
    return json.loads(path.read_text(encoding="utf-8"))


def load_processed_geojson(path: str | Path) -> GeoDataFrame:
    """Load precinct GeoJSON from ``data/processed/`` (or similar)."""
    return gpd.read_file(Path(path))


def load_processed_geoparquet(path: str | Path) -> GeoDataFrame:
    """Load canonical precinct layer written by :func:`write_processed_geoparquet`."""
    return gpd.read_parquet(Path(path))


def write_processed_geojson(gdf: GeoDataFrame, path: str | Path) -> None:
    """Write a precinct GeoDataFrame to GeoJSON."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    gdf.to_file(path, driver="GeoJSON")


def write_processed_geoparquet(gdf: GeoDataFrame, path: str | Path) -> None:
    """Write a precinct GeoDataFrame to GeoParquet (preferred for national layer)."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    gdf.to_parquet(path, index=False)
