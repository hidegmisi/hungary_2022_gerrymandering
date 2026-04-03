"""Slice 1: precinct geometry QA — scalar metrics in metric CRS (no overlap / flags yet)."""

from __future__ import annotations

import math
from typing import Any

import pandas as pd
from geopandas import GeoDataFrame

from hungary_ge.problem import DEFAULT_PRECINCT_ID_COLUMN


def filter_szvk_rows(gdf: GeoDataFrame) -> GeoDataFrame:
    """Keep rows that are official szvk units (exclude void / gap rows).

    If ``unit_kind`` is missing, treat all rows as szvk (backward compatible
    with layers built before that column existed).
    """
    if "unit_kind" not in gdf.columns:
        return gdf.copy()
    uk = gdf["unit_kind"].astype(str)
    return gdf.loc[uk == "szvk"].copy()


def _exterior_perimeter_m(geom: Any) -> float:
    """Sum of exterior ring lengths (metric units of *geom*'s CRS)."""
    if geom is None or geom.is_empty:
        return float("nan")
    gt = geom.geom_type
    if gt == "Polygon":
        return float(geom.exterior.length)
    if gt == "MultiPolygon":
        return float(sum(p.exterior.length for p in geom.geoms))
    return float("nan")


def _polygon_part_and_hole_counts(geom: Any) -> tuple[int, int]:
    if geom is None or geom.is_empty:
        return (0, 0)
    gt = geom.geom_type
    if gt == "Polygon":
        return (1, len(geom.interiors))
    if gt == "MultiPolygon":
        parts = len(geom.geoms)
        holes = sum(len(p.interiors) for p in geom.geoms)
        return (parts, holes)
    return (0, 0)


def _polsby_popper(area_m2: float, exterior_perimeter_m: float) -> float:
    if (
        area_m2 <= 0
        or not math.isfinite(area_m2)
        or not math.isfinite(exterior_perimeter_m)
        or exterior_perimeter_m <= 0
    ):
        return float("nan")
    return 4.0 * math.pi * area_m2 / (exterior_perimeter_m**2)


def compute_precinct_metrics(
    gdf: GeoDataFrame,
    *,
    metric_crs: str = "EPSG:32633",
    id_column: str = DEFAULT_PRECINCT_ID_COLUMN,
) -> pd.DataFrame:
    """Scalar geometry metrics per row in *metric_crs* (default UTM 33N).

    Does not mutate *gdf*. Row order matches *gdf*.

    Columns: ``precinct_id``, ``area_m2``, ``perimeter_m``, ``polsby_popper``,
    ``n_polygon_parts``, ``n_holes``. Polsby–Popper uses total area and **exterior**
    ring lengths only (holes excluded from perimeter).
    """
    if id_column not in gdf.columns:
        msg = f"missing id column {id_column!r}"
        raise ValueError(msg)
    if gdf.crs is None:
        msg = "GeoDataFrame has no CRS; assign CRS before compute_precinct_metrics"
        raise ValueError(msg)

    m = gdf.to_crs(metric_crs)
    geoms = m.geometry

    area_m2 = geoms.area.astype("float64")
    perimeter_m = geoms.apply(_exterior_perimeter_m)
    parts_holes = geoms.apply(_polygon_part_and_hole_counts)
    n_polygon_parts = parts_holes.apply(lambda t: t[0]).astype("int64")
    n_holes = parts_holes.apply(lambda t: t[1]).astype("int64")

    polsby = pd.Series(
        [
            _polsby_popper(float(a), float(p))
            for a, p in zip(area_m2.tolist(), perimeter_m.tolist(), strict=True)
        ],
        index=gdf.index,
        dtype="float64",
    )

    return pd.DataFrame(
        {
            "precinct_id": gdf[id_column].astype(str),
            "area_m2": area_m2,
            "perimeter_m": perimeter_m,
            "polsby_popper": polsby,
            "n_polygon_parts": n_polygon_parts,
            "n_holes": n_holes,
        },
        index=gdf.index,
    )
