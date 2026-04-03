"""Metric-CRS geometry repair for precinct layers (szvk by default).

Used before :func:`~hungary_ge.io.gaps.build_gap_features_all_counties` so
``union(szvk)`` and difference operations see GEOS-stable polygons. See
``geometry_repair`` in the precinct ETL manifest for provenance.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd
from geopandas import GeoDataFrame
from shapely import make_valid

from hungary_ge.problem import DEFAULT_PRECINCT_ID_COLUMN

logger = logging.getLogger(__name__)


@dataclass
class RepairStats:
    """Accountability stats from :func:`repair_precinct_geometries` (JSON-serializable)."""

    metric_crs: str
    large_delta_threshold_m2: float
    n_rows_in_scope: int = 0
    n_invalid_before: int = 0
    n_empty_after: int = 0
    max_abs_area_delta_m2: float = 0.0
    precinct_ids_large_area_delta: list[str] = field(default_factory=list)

    def as_manifest_dict(self) -> dict[str, Any]:
        return {
            "metric_crs": self.metric_crs,
            "large_delta_threshold_m2": self.large_delta_threshold_m2,
            "n_rows_in_scope": self.n_rows_in_scope,
            "n_invalid_before": self.n_invalid_before,
            "n_empty_after": self.n_empty_after,
            "max_abs_area_delta_m2": self.max_abs_area_delta_m2,
            "precinct_ids_large_area_delta": list(self.precinct_ids_large_area_delta),
            "n_ids_large_delta_reported": len(self.precinct_ids_large_area_delta),
        }


def _repair_one_metric(geom: Any) -> Any:
    """``make_valid`` (if needed) + ``buffer(0)`` in metric CRS."""
    if geom is None or geom.is_empty:
        return geom
    g = geom
    if not g.is_valid:
        g = make_valid(g)
    return g.buffer(0)


def repair_precinct_geometries(
    gdf: GeoDataFrame,
    *,
    metric_crs: str = "EPSG:32633",
    id_column: str = DEFAULT_PRECINCT_ID_COLUMN,
    only_unit_kind: str | None = "szvk",
    large_delta_threshold_m2: float = 10.0,
    max_ids_in_manifest: int = 50,
) -> tuple[GeoDataFrame, RepairStats]:
    """Return a copy with repaired geometries for rows in scope.

    Rows in scope: if ``unit_kind`` is missing, **all** rows (raw szvk-only
    frames). If present and *only_unit_kind* is ``\"szvk\"`` (default), only
    ``unit_kind == \"szvk\"``. If *only_unit_kind* is ``None``, all rows.

    Repair pipeline (per in-scope row, in *metric_crs*): ``make_valid`` when
    invalid, then ``buffer(0)``. Out-of-scope rows are unchanged.

    *large_delta_threshold_m2*: report ``precinct_id`` in stats when
    absolute area change (metric m²) exceeds this (capped by *max_ids_in_manifest*).
    """
    if id_column not in gdf.columns:
        msg = f"missing id column {id_column!r}"
        raise ValueError(msg)
    if gdf.crs is None:
        msg = "GeoDataFrame has no CRS; assign CRS before repair_precinct_geometries"
        raise ValueError(msg)

    out = gdf.copy()
    n = len(out)
    if n == 0:
        return out, RepairStats(
            metric_crs=metric_crs,
            large_delta_threshold_m2=large_delta_threshold_m2,
        )

    if only_unit_kind is None:
        scope = pd.Series(True, index=out.index)
    elif "unit_kind" not in out.columns:
        scope = pd.Series(True, index=out.index)
    else:
        scope = out["unit_kind"].astype(str) == str(only_unit_kind)

    stats = RepairStats(
        metric_crs=metric_crs,
        large_delta_threshold_m2=large_delta_threshold_m2,
        n_rows_in_scope=int(scope.sum()),
    )
    if not scope.any():
        return out, stats

    m = out.to_crs(metric_crs)
    idx_scope = out.index[scope.to_numpy()]
    before = m.loc[idx_scope, "geometry"].area.to_numpy(dtype="float64")

    stats.n_invalid_before = int((~m.loc[idx_scope, "geometry"].is_valid).sum())

    repaired = m.loc[idx_scope, "geometry"].apply(_repair_one_metric)
    m.loc[idx_scope, "geometry"] = repaired
    after = m.loc[idx_scope, "geometry"].area.to_numpy(dtype="float64")

    stats.n_empty_after = int((m.loc[idx_scope, "geometry"].is_empty).sum())

    delta = np.abs(after - before)
    if delta.size > 0:
        stats.max_abs_area_delta_m2 = float(np.nanmax(delta))

    large_idx = np.flatnonzero(delta > float(large_delta_threshold_m2))
    pids = out.loc[idx_scope, id_column].astype(str).to_numpy()
    for j in large_idx[:max_ids_in_manifest]:
        stats.precinct_ids_large_area_delta.append(str(pids[j]))
    if len(large_idx) > max_ids_in_manifest:
        logger.warning(
            "geometry repair: %d precincts exceed area delta threshold; manifest lists %d",
            int(len(large_idx)),
            max_ids_in_manifest,
        )

    out["geometry"] = m.to_crs(out.crs).geometry
    return out, stats
