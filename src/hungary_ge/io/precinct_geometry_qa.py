"""Precinct geometry QA: scalar metrics (slice 1) and per-county overlaps (slice 2)."""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Iterator
from typing import Any

import numpy as np
import pandas as pd
from geopandas import GeoDataFrame
from shapely import area as shp_area
from shapely import intersection as shp_intersection

from hungary_ge.problem import DEFAULT_PRECINCT_ID_COLUMN


def _normalize_maz(value: str | int | float) -> str:
    s = str(value).strip()
    if s.isdigit():
        return s.zfill(2)
    return s


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


def _empty_overlap_agg() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "precinct_id": pd.Series(dtype=str),
            "n_overlap_partners": pd.Series(dtype="int64"),
            "sum_overlap_area_m2": pd.Series(dtype="float64"),
            "max_overlap_area_m2": pd.Series(dtype="float64"),
            "max_overlap_ratio": pd.Series(dtype="float64"),
        }
    )


def _empty_overlap_edges() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "precinct_id_a": pd.Series(dtype=str),
            "precinct_id_b": pd.Series(dtype=str),
            "intersection_area_m2": pd.Series(dtype="float64"),
        }
    )


def _iter_maz_groups(m_work: GeoDataFrame) -> Iterator[tuple[str, GeoDataFrame]]:
    """Yield ``(maz_key, subframe)`` with rows in original index order within each group."""
    keys = m_work["_maz_n"].unique()
    keys = sorted(keys, key=lambda x: (len(str(x)), str(x)))
    for maz_key in keys:
        sub = m_work.loc[m_work["_maz_n"] == maz_key].sort_index()
        yield str(maz_key), sub


def compute_precinct_overlaps(
    gdf: GeoDataFrame,
    *,
    metric_crs: str = "EPSG:32633",
    maz_column: str = "maz",
    id_column: str = DEFAULT_PRECINCT_ID_COLUMN,
    min_overlap_m2: float = 1.0,
    min_overlap_ratio: float | None = 0.001,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Detect **material** polygon overlaps between distinct precincts, scoped per county.

    For each normalized ``maz``, self-joins on ``intersects``, computes intersection
    area in *metric_crs*, and keeps pairs above ``min_overlap_m2`` and (if set)
    ``min_overlap_ratio`` = intersection area / min(area_i, area_j).

    Returns:
        ``(per_precinct, edges)``. **per_precinct** has one row per input row
        (same ``precinct_id`` order as *gdf*): ``n_overlap_partners``,
        ``sum_overlap_area_m2``, ``max_overlap_area_m2``, ``max_overlap_ratio``.
        **edges** lists unique undirected pairs with ``precinct_id_a`` < ``precinct_id_b``
        lexicographically.

    Cross-county overlaps are not considered (each pair must share a ``maz``).
    """
    if maz_column not in gdf.columns:
        msg = f"missing maz column {maz_column!r}"
        raise ValueError(msg)
    if id_column not in gdf.columns:
        msg = f"missing id column {id_column!r}"
        raise ValueError(msg)
    if gdf.crs is None:
        msg = "GeoDataFrame has no CRS; assign CRS before compute_precinct_overlaps"
        raise ValueError(msg)
    if min_overlap_m2 < 0:
        msg = f"min_overlap_m2 must be non-negative, got {min_overlap_m2}"
        raise ValueError(msg)

    if len(gdf) == 0:
        return _empty_overlap_agg(), _empty_overlap_edges()

    m_work = gdf.to_crs(metric_crs).copy()
    m_work["_maz_n"] = m_work[maz_column].map(_normalize_maz)

    stats_by_id: dict[str, dict[str, Any]] = {}
    edge_frames: list[pd.DataFrame] = []

    for _maz_key, sub in _iter_maz_groups(m_work):
        sub = sub.reset_index(drop=True)
        n = len(sub)
        ids = sub[id_column].astype(str).to_numpy()
        geoms = sub.geometry.to_numpy()
        poly_areas = sub.geometry.area.to_numpy(dtype="float64")

        partners: dict[str, set[str]] = defaultdict(set)
        sum_overlap: dict[str, float] = defaultdict(float)
        max_area: dict[str, float] = defaultdict(float)
        max_ratio: dict[str, float] = defaultdict(float)

        if n < 2:
            for pid in ids:
                spid = str(pid)
                stats_by_id[spid] = {
                    "precinct_id": spid,
                    "n_overlap_partners": 0,
                    "sum_overlap_area_m2": 0.0,
                    "max_overlap_area_m2": 0.0,
                    "max_overlap_ratio": 0.0,
                }
            continue

        sub["_oid"] = np.arange(n, dtype=np.int64)
        left = sub[[id_column, "geometry", "_oid"]].copy()
        right = sub[[id_column, "geometry", "_oid"]].rename(
            columns={id_column: "_pid_r", "_oid": "_oid_r"},
        )
        joined = left.sjoin(right, predicate="intersects", how="inner")
        sel = joined["_oid"].to_numpy() != joined["_oid_r"].to_numpy()
        joined = joined.loc[sel].copy()
        if joined.empty:
            for pid in ids:
                spid = str(pid)
                stats_by_id[spid] = {
                    "precinct_id": spid,
                    "n_overlap_partners": 0,
                    "sum_overlap_area_m2": 0.0,
                    "max_overlap_area_m2": 0.0,
                    "max_overlap_ratio": 0.0,
                }
            continue

        rows_l = joined["_oid"].to_numpy(dtype=np.int64)
        rows_r = joined["_oid_r"].to_numpy(dtype=np.int64)
        inter = shp_intersection(geoms[rows_l], geoms[rows_r])
        inter_areas = np.asarray(shp_area(inter), dtype="float64")
        min_side = np.minimum(poly_areas[rows_l], poly_areas[rows_r])
        with np.errstate(divide="ignore", invalid="ignore"):
            ratios = np.where(min_side > 0.0, inter_areas / min_side, 0.0)

        ok = inter_areas >= float(min_overlap_m2)
        if min_overlap_ratio is not None:
            ok &= ratios >= float(min_overlap_ratio)

        if not ok.any():
            for pid in ids:
                spid = str(pid)
                stats_by_id[spid] = {
                    "precinct_id": spid,
                    "n_overlap_partners": 0,
                    "sum_overlap_area_m2": 0.0,
                    "max_overlap_area_m2": 0.0,
                    "max_overlap_ratio": 0.0,
                }
            continue

        id_l = ids[rows_l[ok]]
        id_r = ids[rows_r[ok]]
        areas_ok = inter_areas[ok]
        ratios_ok = ratios[ok]

        pairs = np.stack([id_l, id_r], axis=1)
        pairs.sort(axis=1)
        ca = pairs[:, 0]
        cb = pairs[:, 1]

        edges_staging = pd.DataFrame(
            {
                "precinct_id_a": ca,
                "precinct_id_b": cb,
                "intersection_area_m2": areas_ok,
                "_overlap_ratio": ratios_ok,
            }
        )
        edges_maz = edges_staging.groupby(
            ["precinct_id_a", "precinct_id_b"],
            as_index=False,
        ).agg(
            intersection_area_m2=("intersection_area_m2", "max"),
            overlap_ratio_max=("_overlap_ratio", "max"),
        )
        edge_frames.append(
            edges_maz[
                ["precinct_id_a", "precinct_id_b", "intersection_area_m2"]
            ].copy(),
        )

        for _, er in edges_maz.iterrows():
            a = str(er["precinct_id_a"])
            b = str(er["precinct_id_b"])
            ar = float(er["intersection_area_m2"])
            rt = float(er["overlap_ratio_max"])
            for u, v in ((a, b), (b, a)):
                partners[u].add(v)
                sum_overlap[u] += ar
                max_area[u] = max(max_area[u], ar)
                max_ratio[u] = max(max_ratio[u], rt)

        for pid in ids:
            spid = str(pid)
            pset = partners[spid]
            stats_by_id[spid] = {
                "precinct_id": spid,
                "n_overlap_partners": len(pset),
                "sum_overlap_area_m2": float(sum_overlap[spid]),
                "max_overlap_area_m2": float(max_area[spid]) if pset else 0.0,
                "max_overlap_ratio": float(max_ratio[spid]) if pset else 0.0,
            }

    order = gdf[id_column].astype(str).tolist()
    agg_df = pd.DataFrame([stats_by_id[pid] for pid in order])
    if edge_frames:
        edges_df = pd.concat(edge_frames, ignore_index=True)
    else:
        edges_df = _empty_overlap_edges()

    return agg_df, edges_df
