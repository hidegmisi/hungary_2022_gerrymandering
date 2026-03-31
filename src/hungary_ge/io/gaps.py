"""Void (gap) polygon features: official shell minus union(szvk) (per county)."""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import geopandas as gpd
import pandas as pd
from geopandas import GeoDataFrame
from shapely.ops import unary_union

from hungary_ge.io.gaps_hex import HexVoidOptions, subdivide_gap_polygons_hex
from hungary_ge.problem import DEFAULT_PRECINCT_ID_COLUMN

logger = logging.getLogger(__name__)

VOID_TAZ_PLACEHOLDER = "000"
VOID_SZK_PLACEHOLDER = "000"


@dataclass(frozen=True)
class GapShellSource:
    """Where to load county (or multi-county) shell polygons for gap extraction."""

    path: Path
    maz_column: str = "maz"
    layer: str | None = None


def read_shell_gdf(source: GapShellSource) -> GeoDataFrame:
    """Load shell geometries with ``maz_column`` from GeoPackage, GeoJSON, or Shapefile.

    Args:
        source: Path and column names. ``layer`` is passed to ``geopandas.read_file``
            for multi-layer formats (e.g. GPKG).

    Returns:
        A non-empty :class:`~geopandas.GeoDataFrame` with geometry and ``maz_column``.
    """
    path = Path(source.path)
    kwargs: dict[str, Any] = {}
    if source.layer is not None:
        kwargs["layer"] = source.layer
    gdf = gpd.read_file(path, **kwargs)
    if source.maz_column not in gdf.columns:
        msg = f"shell missing maz column {source.maz_column!r} (columns: {list(gdf.columns)})"
        raise ValueError(msg)
    if gdf.empty:
        msg = f"shell layer is empty: {path}"
        raise ValueError(msg)
    return gdf


@dataclass(frozen=True)
class GapBuildOptions:
    """Parameters for :func:`build_gap_features_for_maz`."""

    metric_crs: str = "EPSG:32633"
    min_area_m2: float = 100.0
    void_id_prefix: str = "gap"
    """If > 0, buffer the precinct union by this many meters before ``difference`` (closes hairline cracks)."""
    precinct_union_buffer_m: float = 0.0
    """Optional small negative buffer on shell (meters) before difference; use with care."""
    shell_buffer_m: float = 0.0
    """Optional hex tessellation of large void polygons (see :class:`~hungary_ge.io.gaps_hex.HexVoidOptions`)."""
    hex_void: HexVoidOptions | None = None


@dataclass
class GapBuildStats:
    """Aggregated counts from :func:`build_gap_features_all_counties`."""

    n_shell_features_read: int = 0
    n_counties_processed: int = 0
    n_gap_polygons: int = 0
    """Gap polygon count after ``min_area_m2`` filter, before optional hex subdivision."""
    n_gap_polygons_raw: int = 0
    """Final void polygon row count (after hex tessellation when enabled)."""
    n_void_cells_after_hex: int = 0
    n_dropped_below_min_area: int = 0
    total_gap_area_m2: float = 0.0
    median_szvk_area_m2: float | None = None
    hex_cell_area_m2_used: float | None = None
    n_hex_cells_truncated: int = 0
    warnings: list[str] = field(default_factory=list)
    per_maz: dict[str, dict[str, int | float]] = field(default_factory=dict)

    def add_warning(self, msg: str) -> None:
        self.warnings.append(msg)
        logger.warning(msg)


def _normalize_maz(val: str | int) -> str:
    s = str(val).strip()
    try:
        return f"{int(s):02d}"
    except ValueError:
        return s


def _geoms_from_gap_multipiece(geom: Any) -> list[Any]:
    """Split Polygon / MultiPolygon / GeometryCollection polygon parts into a list."""
    if geom is None or geom.is_empty:
        return []
    gtyp = getattr(geom, "geom_type", type(geom).__name__)
    if gtyp == "Polygon":
        return [geom]
    if gtyp == "MultiPolygon":
        return list(geom.geoms)
    if gtyp == "GeometryCollection":
        out: list[Any] = []
        for part in geom.geoms:
            out.extend(_geoms_from_gap_multipiece(part))
        return out
    return []


def build_gap_features_for_maz(
    shell_gdf: GeoDataFrame,
    precinct_gdf: GeoDataFrame,
    maz: str,
    *,
    shell_maz_column: str,
    options: GapBuildOptions | None = None,
    output_crs: str | None = None,
) -> tuple[GeoDataFrame, GapBuildStats]:
    """Compute ``shell \\ union(precincts)`` for one county code.

    Args:
        shell_gdf: Multi-row or single-row shell for many counties; filtered by ``maz``.
        precinct_gdf: Precinct polygons for the same county only (``maz`` column).
        maz: Two-digit county code.
        shell_maz_column: Column in ``shell_gdf`` matching ``maz``.
        options: Metric thresholds and buffers.
        output_crs: CRS for output (default: ``precinct_gdf.crs``).

    Returns:
        ``(gap_gdf, stats)`` with columns aligned to typical szvk layer:
        ``maz``, ``taz``, ``szk``, ``precinct_id``, ``geometry``, ``unit_kind``.
    """
    opts = options if options is not None else GapBuildOptions()
    maz_n = _normalize_maz(maz)
    stats = GapBuildStats()

    maz_shell = shell_gdf[shell_maz_column].map(_normalize_maz)
    shell_part = shell_gdf[maz_shell == maz_n].copy()
    if shell_part.empty:
        stats.add_warning(f"no shell polygon for maz={maz_n}")
        return GeoDataFrame(
            {
                "maz": [],
                "taz": [],
                "szk": [],
                DEFAULT_PRECINCT_ID_COLUMN: [],
                "unit_kind": [],
                "geometry": [],
            },
            crs=output_crs or precinct_gdf.crs,
        ), stats

    maz_p = precinct_gdf["maz"].map(_normalize_maz)
    prec_part = precinct_gdf[maz_p == maz_n].copy()
    if prec_part.empty:
        stats.add_warning(f"no precinct rows for maz={maz_n}")
        return GeoDataFrame(
            {
                "maz": [],
                "taz": [],
                "szk": [],
                DEFAULT_PRECINCT_ID_COLUMN: [],
                "unit_kind": [],
                "geometry": [],
            },
            crs=output_crs or precinct_gdf.crs,
        ), stats

    out_crs = output_crs if output_crs is not None else prec_part.crs
    if out_crs is None:
        msg = "precinct_gdf has no CRS; set output_crs or assign CRS"
        raise ValueError(msg)

    shell_m = shell_part.to_crs(opts.metric_crs)
    prec_m = prec_part.to_crs(opts.metric_crs)

    shell_u: Any = unary_union(shell_m.geometry.tolist())
    if opts.shell_buffer_m != 0.0:
        shell_u = shell_u.buffer(opts.shell_buffer_m)

    prec_u: Any = unary_union(prec_m.geometry.tolist())
    if opts.precinct_union_buffer_m > 0.0:
        prec_u = prec_u.buffer(opts.precinct_union_buffer_m)

    gap_m = shell_u.difference(prec_u)
    pieces = _geoms_from_gap_multipiece(gap_m)

    raw_polygons: list[Any] = []
    dropped = 0
    for poly in pieces:
        if poly.is_empty:
            continue
        a = float(poly.area)
        if a < opts.min_area_m2:
            dropped += 1
            continue
        raw_polygons.append(poly)

    stats.n_dropped_below_min_area = dropped
    stats.n_gap_polygons_raw = len(raw_polygons)

    med_series = prec_m.geometry.area
    median_m2 = float(med_series.median())
    stats.median_szvk_area_m2 = median_m2 if not math.isnan(median_m2) else None

    final_polygons: list[Any] = list(raw_polygons)
    if opts.hex_void is not None and opts.hex_void.enabled:
        final_polygons, hex_meta = subdivide_gap_polygons_hex(
            raw_polygons,
            median_szvk_area_m2=median_m2,
            hex_opts=opts.hex_void,
            min_fragment_m2=opts.min_area_m2,
        )
        stats.hex_cell_area_m2_used = hex_meta.get("hex_cell_area_m2_used")
        stats.n_hex_cells_truncated = int(hex_meta.get("n_truncated_cells", 0))
        for w in hex_meta.get("resolve_warnings", []):
            stats.add_warning(w)
        if hex_meta.get("skipped_hex"):
            stats.add_warning(
                "hex_void skipped (invalid auto-size); using raw gap pieces"
            )
            final_polygons = list(raw_polygons)

    rows = [{"geometry": poly} for poly in final_polygons]
    stats.n_gap_polygons = len(rows)
    stats.n_void_cells_after_hex = len(rows)
    stats.total_gap_area_m2 = sum(float(r["geometry"].area) for r in rows)

    if not rows:
        g_out = GeoDataFrame(
            {
                "maz": [],
                "taz": [],
                "szk": [],
                DEFAULT_PRECINCT_ID_COLUMN: [],
                "unit_kind": [],
                "geometry": [],
            },
            crs=out_crs,
        )
        stats.per_maz[maz_n] = {"n_gaps": 0, "area_skipped_m2": 0.0}
        return g_out, stats

    g_metric = gpd.GeoDataFrame(rows, crs=opts.metric_crs)
    cxs = g_metric.geometry.centroid.x
    cys = g_metric.geometry.centroid.y
    g_metric = (
        g_metric.assign(_cx=cxs, _cy=cys)
        .sort_values(["_cx", "_cy"])
        .drop(columns=["_cx", "_cy"])
    )
    g_metric = g_metric.reset_index(drop=True)

    prefix = opts.void_id_prefix
    pids = [f"{prefix}-{maz_n}-{i:04d}" for i in range(len(g_metric))]

    g_metric[DEFAULT_PRECINCT_ID_COLUMN] = pids
    g_metric["maz"] = maz_n
    g_metric["taz"] = VOID_TAZ_PLACEHOLDER
    g_metric["szk"] = VOID_SZK_PLACEHOLDER
    g_metric["unit_kind"] = "void"

    g_out = g_metric.to_crs(out_crs)
    stats.per_maz[maz_n] = {
        "n_gaps": len(g_out),
        "area_m2_metric": float(stats.total_gap_area_m2),
    }
    return g_out, stats


def build_gap_features_all_counties(
    shell_gdf: GeoDataFrame,
    precinct_gdf: GeoDataFrame,
    *,
    shell_maz_column: str,
    options: GapBuildOptions | None = None,
) -> tuple[GeoDataFrame, GapBuildStats]:
    """Run :func:`build_gap_features_for_maz` for every county present in ``precinct_gdf``."""
    opts = options if options is not None else GapBuildOptions()
    agg = GapBuildStats()
    agg.n_shell_features_read = len(shell_gdf)

    maz_list = sorted({_normalize_maz(m) for m in precinct_gdf["maz"].unique()})
    parts: list[GeoDataFrame] = []

    for maz in maz_list:
        g, st = build_gap_features_for_maz(
            shell_gdf,
            precinct_gdf,
            maz,
            shell_maz_column=shell_maz_column,
            options=opts,
        )
        agg.n_counties_processed += 1
        agg.n_gap_polygons += st.n_gap_polygons
        agg.n_gap_polygons_raw += st.n_gap_polygons_raw
        agg.n_void_cells_after_hex += st.n_void_cells_after_hex
        agg.n_dropped_below_min_area += st.n_dropped_below_min_area
        agg.total_gap_area_m2 += st.total_gap_area_m2
        agg.n_hex_cells_truncated += st.n_hex_cells_truncated
        if st.median_szvk_area_m2 is not None:
            agg.median_szvk_area_m2 = st.median_szvk_area_m2
        if st.hex_cell_area_m2_used is not None:
            agg.hex_cell_area_m2_used = st.hex_cell_area_m2_used
        agg.per_maz.update(st.per_maz)
        agg.warnings.extend(st.warnings)
        if len(g) > 0:
            parts.append(g)

    if not parts:
        crs = precinct_gdf.crs
        return GeoDataFrame(
            {
                "maz": [],
                "taz": [],
                "szk": [],
                DEFAULT_PRECINCT_ID_COLUMN: [],
                "unit_kind": [],
                "geometry": [],
            },
            crs=crs,
        ), agg

    return pd.concat(parts, ignore_index=True), agg


def merge_szvk_and_gaps(szvk_gdf: GeoDataFrame, gap_gdf: GeoDataFrame) -> GeoDataFrame:
    """Concatenate szvk and void rows; ensure disjoint ``precinct_id`` and ``unit_kind`` on szvk."""
    col = DEFAULT_PRECINCT_ID_COLUMN
    sz_ids = set(szvk_gdf[col].astype(str))
    gp_ids = set(gap_gdf[col].astype(str))
    inter = sz_ids & gp_ids
    if inter:
        msg = f"precinct_id overlap between szvk and gap: {sorted(inter)[:10]}…"
        raise ValueError(msg)

    sz = szvk_gdf.copy()
    if "unit_kind" not in sz.columns:
        sz["unit_kind"] = "szvk"

    return pd.concat([sz, gap_gdf], ignore_index=True)
