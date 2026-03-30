"""Build a national precinct :class:`~geopandas.GeoDataFrame` from ``szavkor_topo``."""

from __future__ import annotations

import logging
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import geopandas as gpd
from geopandas import GeoDataFrame

from hungary_ge.io.geoio import load_szavkor_settlement_json
from hungary_ge.io.szavkor_parse import (
    SzavkorRecord,
    composite_precinct_id,
    record_to_geometry,
)
from hungary_ge.problem import DEFAULT_PRECINCT_ID_COLUMN

logger = logging.getLogger(__name__)


@dataclass
class PrecinctBuildStats:
    """Counts and messages from :func:`build_precinct_gdf`."""

    n_files_read: int = 0
    n_records_in: int = 0
    n_rows_out: int = 0
    n_dropped_unrepaired: int = 0
    warnings: list[str] = field(default_factory=list)

    def add_warning(self, msg: str) -> None:
        self.warnings.append(msg)
        logger.warning(msg)


def iter_settlement_json_paths(root: Path) -> Iterator[Path]:
    """Yield ``*.json`` paths under ``root`` (sorted for reproducible builds)."""
    paths = sorted(root.glob("*/*.json"))
    for p in paths:
        if p.is_file():
            yield p


def _rows_from_settlement(
    data: dict[str, Any],
    stats: PrecinctBuildStats,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for raw in data.get("list", []):
        stats.n_records_in += 1
        maz = str(raw.get("maz", "")).strip()
        taz = str(raw.get("taz", "")).strip()
        szk = str(raw.get("szk", "")).strip()
        centrum = str(raw.get("centrum", "") or "")
        poligon = str(raw.get("poligon", "") or "")
        rec = SzavkorRecord(maz=maz, taz=taz, szk=szk, centrum=centrum, poligon=poligon)
        geom, _centroid = record_to_geometry(rec)
        if geom is None:
            stats.n_dropped_unrepaired += 1
            pid = composite_precinct_id(maz, taz, szk)
            stats.add_warning(f"drop precinct (unrepaired geometry): {pid}")
            continue
        pid = composite_precinct_id(maz, taz, szk)
        rows.append(
            {
                "maz": maz,
                "taz": taz,
                "szk": szk,
                DEFAULT_PRECINCT_ID_COLUMN: pid,
                "geometry": geom,
            }
        )
    return rows


def build_precinct_gdf(
    root: Path,
    *,
    crs: str = "EPSG:4326",
) -> tuple[GeoDataFrame, PrecinctBuildStats]:
    """Walk ``szavkor_topo`` root, parse polygons, return a single GeoDataFrame.

    Invalid rings are repaired via :func:`~hungary_ge.io.szavkor_parse.repair_polygonal_geometry`.
    Rows that cannot be repaired to a non-empty polygon or multipolygon are **dropped**
    and counted in ``stats.n_dropped_unrepaired``.

    Args:
        root: Typically ``data/raw/szavkor_topo``.
        crs: Stored CRS (source coordinates are WGS84 lat/lon strings).

    Returns:
        ``(gdf, stats)`` with unique ``precinct_id`` column
        :data:`~hungary_ge.problem.DEFAULT_PRECINCT_ID_COLUMN`.
    """
    root = Path(root)
    stats = PrecinctBuildStats()
    all_rows: list[dict[str, Any]] = []
    for path in iter_settlement_json_paths(root):
        stats.n_files_read += 1
        data = load_szavkor_settlement_json(path)
        all_rows.extend(_rows_from_settlement(data, stats))
    if not all_rows:
        msg = f"no precinct rows built from {root}"
        raise ValueError(msg)
    gdf = gpd.GeoDataFrame(all_rows, crs=crs)
    dup = gdf[DEFAULT_PRECINCT_ID_COLUMN].duplicated()
    if dup.any():
        n = int(dup.sum())
        stats.add_warning(f"duplicate precinct_id rows: {n}; keeping first occurrence")
        gdf = gdf.loc[~gdf[DEFAULT_PRECINCT_ID_COLUMN].duplicated()].copy()
    stats.n_rows_out = len(gdf)
    return gdf, stats


def raw_precinct_list_total(root: Path) -> int:
    """Count ``list`` elements across all settlement JSON files (for QA vs output)."""
    root = Path(root)
    total = 0
    for path in iter_settlement_json_paths(root):
        data = load_szavkor_settlement_json(path)
        total += len(data.get("list", []))
    return total
