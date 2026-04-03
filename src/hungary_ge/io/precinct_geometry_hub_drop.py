"""Drop extreme overlap-hub szvk polygons from a spatial layer (optional ETL stage).

Uses overlap graph statistics from :func:`~hungary_ge.io.precinct_geometry_qa.compute_precinct_overlaps`
and areas from :func:`~hungary_ge.io.precinct_geometry_qa.compute_precinct_metrics`. Run only on
geometries that have already been repaired (``repair_geometries=False`` on the overlap pass).

Votes and raw JSON extracts are unchanged; dropped ``precinct_id`` values may still appear in vote tables.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Literal

import numpy as np
import pandas as pd
from geopandas import GeoDataFrame

from hungary_ge.io.precinct_geometry_qa import (
    compute_precinct_metrics,
    compute_precinct_overlaps,
    filter_szvk_rows,
)
from hungary_ge.problem import DEFAULT_PRECINCT_ID_COLUMN


def _tier_disabled(value: int | None) -> bool:
    return value is None or int(value) <= 0


@dataclass(frozen=True)
class OverlapHubDropOptions:
    """Thresholds for :func:`drop_overlap_hub_szvk`.

    Hard tier: drop if ``n_overlap_partners >= hard_min_partners`` (when enabled).
    Soft tier: drop if ``n_overlap_partners >= soft_min_partners`` and
    ``sum_overlap_area_m2 >= mass_ratio * area_m2`` with finite positive ``area_m2``.

    Use ``None`` or non-positive integers to disable a tier.
    """

    hard_min_partners: int | None = 100
    soft_min_partners: int | None = 30
    mass_ratio: float = 1.5
    max_drop_rows: int = 200
    allow_exceed_max: bool = False
    overlap_min_overlap_m2: float = 5.0
    overlap_min_overlap_ratio: float | None = 0.001
    metric_crs: str = "EPSG:32633"

    def __post_init__(self) -> None:
        if self.mass_ratio <= 0.0 or not np.isfinite(self.mass_ratio):
            msg = f"mass_ratio must be finite and > 0, got {self.mass_ratio!r}"
            raise ValueError(msg)
        if self.overlap_min_overlap_m2 < 0.0:
            msg = "overlap_min_overlap_m2 must be non-negative"
            raise ValueError(msg)


@dataclass
class OverlapHubDropStats:
    """Accountability stats from :func:`drop_overlap_hub_szvk`."""

    enabled: bool
    options_snapshot: dict[str, Any]
    n_candidates_hard: int
    n_candidates_soft: int
    n_dropped: int
    dropped_records: list[dict[str, Any]] = field(default_factory=list)

    def manifest_dict(self, max_detail: int = 500) -> dict[str, Any]:
        """JSON-serializable manifest block; caps ``dropped_detail`` length."""
        detail = list(self.dropped_records)
        truncated = len(detail) > int(max_detail)
        if truncated:
            detail = detail[: int(max_detail)]
        return {
            "enabled": self.enabled,
            "options": dict(self.options_snapshot),
            "n_candidates_hard": self.n_candidates_hard,
            "n_candidates_soft": self.n_candidates_soft,
            "n_dropped": self.n_dropped,
            "dropped_detail": detail,
            "dropped_detail_truncated": truncated,
        }


def hub_drop_masks_for_precinct_table(
    work: pd.DataFrame,
    *,
    options: OverlapHubDropOptions,
) -> tuple[pd.Series, pd.Series, pd.Series]:
    """Pure mask logic for testing: *work* has ``precinct_id``, ``area_m2``,
    ``n_overlap_partners``, ``sum_overlap_area_m2`` (one row per precinct).

    Returns ``(hard_mask, soft_mask, combined_mask)`` boolean Series aligned to *work*.index.
    """
    required = (
        "precinct_id",
        "area_m2",
        "n_overlap_partners",
        "sum_overlap_area_m2",
    )
    for c in required:
        if c not in work.columns:
            msg = f"work must contain column {c!r}"
            raise ValueError(msg)

    nop = work["n_overlap_partners"].to_numpy(dtype=np.int64)
    som = work["sum_overlap_area_m2"].to_numpy(dtype="float64")
    area = work["area_m2"].to_numpy(dtype="float64")

    hard_on = not _tier_disabled(options.hard_min_partners)
    soft_on = not _tier_disabled(options.soft_min_partners)
    hard_thr = int(options.hard_min_partners) if hard_on else 0
    soft_thr = int(options.soft_min_partners) if soft_on else 0

    hard_mask = np.zeros(len(work), dtype=bool) if not hard_on else (nop >= hard_thr)

    area_ok = np.isfinite(area) & (area > 0.0)
    rhs = options.mass_ratio * area
    soft_mask = (
        np.zeros(len(work), dtype=bool)
        if not soft_on
        else (nop >= soft_thr) & area_ok & (som >= rhs)
    )

    combined = hard_mask | soft_mask
    idx = work.index
    return (
        pd.Series(hard_mask, index=idx),
        pd.Series(soft_mask, index=idx),
        pd.Series(combined, index=idx),
    )


def _szvk_row_mask(gdf: GeoDataFrame) -> pd.Series:
    if "unit_kind" not in gdf.columns:
        return pd.Series(True, index=gdf.index)
    return gdf["unit_kind"].astype(str) == "szvk"


def _merge_metrics_overlap(
    metrics_df: pd.DataFrame,
    overlap_df: pd.DataFrame,
) -> pd.DataFrame:
    work = metrics_df.merge(
        overlap_df,
        on="precinct_id",
        how="left",
        validate="one_to_one",
    )
    for col in (
        "n_overlap_partners",
        "sum_overlap_area_m2",
        "max_overlap_area_m2",
        "max_overlap_ratio",
    ):
        if col in work.columns:
            work[col] = work[col].fillna(0.0)
    if "n_overlap_partners" in work.columns:
        work["n_overlap_partners"] = work["n_overlap_partners"].astype("int64")
    return work


def drop_overlap_hub_szvk(
    gdf: GeoDataFrame,
    *,
    options: OverlapHubDropOptions | None = None,
    id_column: str = DEFAULT_PRECINCT_ID_COLUMN,
    maz_column: str = "maz",
) -> tuple[GeoDataFrame, OverlapHubDropStats]:
    """Return a copy of *gdf* with overlap-hub szvk rows removed per *options*.

    When both tiers are disabled, returns a copy without computing overlaps.
    If ``unit_kind`` is present, only ``szvk`` rows are candidates for removal;
    overlap metrics are computed on the szvk subset only. Non-szvk rows are never dropped.
    """
    opts = options if options is not None else OverlapHubDropOptions()
    snap = {k: v for k, v in asdict(opts).items()}

    if id_column not in gdf.columns:
        msg = f"missing id column {id_column!r}"
        raise ValueError(msg)
    if gdf.crs is None:
        msg = "GeoDataFrame has no CRS; assign CRS before drop_overlap_hub_szvk"
        raise ValueError(msg)

    both_off = _tier_disabled(opts.hard_min_partners) and _tier_disabled(
        opts.soft_min_partners,
    )
    if both_off:
        stats = OverlapHubDropStats(
            enabled=False,
            options_snapshot=snap,
            n_candidates_hard=0,
            n_candidates_soft=0,
            n_dropped=0,
            dropped_records=[],
        )
        return gdf.copy(), stats

    if maz_column not in gdf.columns:
        msg = f"missing maz column {maz_column!r}"
        raise ValueError(msg)

    work_gdf = filter_szvk_rows(gdf) if "unit_kind" in gdf.columns else gdf
    if len(work_gdf) == 0:
        stats = OverlapHubDropStats(
            enabled=True,
            options_snapshot=snap,
            n_candidates_hard=0,
            n_candidates_soft=0,
            n_dropped=0,
            dropped_records=[],
        )
        return gdf.copy(), stats

    metrics_df = compute_precinct_metrics(
        work_gdf,
        metric_crs=opts.metric_crs,
        id_column=id_column,
    )
    overlap_df, _edges = compute_precinct_overlaps(
        work_gdf,
        metric_crs=opts.metric_crs,
        maz_column=maz_column,
        id_column=id_column,
        min_overlap_m2=opts.overlap_min_overlap_m2,
        min_overlap_ratio=opts.overlap_min_overlap_ratio,
        repair_geometries=False,
    )
    merged = _merge_metrics_overlap(metrics_df, overlap_df)

    hard_m, soft_m, comb_m = hub_drop_masks_for_precinct_table(merged, options=opts)
    n_hard = int(hard_m.sum())
    n_soft = int(soft_m.sum())
    drop_ids = set(merged.loc[comb_m, "precinct_id"].astype(str).tolist())
    n_drop = len(drop_ids)

    max_r = opts.max_drop_rows
    if max_r > 0 and n_drop > max_r and not opts.allow_exceed_max:
        msg = (
            f"overlap hub drop would remove {n_drop} precincts, "
            f"max_drop_rows={max_r} (set allow_exceed_max or raise max_drop_rows)"
        )
        raise ValueError(msg)

    dropped_records: list[dict[str, Any]] = []
    for _i, row in merged.loc[comb_m].iterrows():
        pid = str(row["precinct_id"])
        is_hard = bool(hard_m.loc[_i])
        reason: Literal["hard", "soft"] = "hard" if is_hard else "soft"
        dropped_records.append(
            {
                "precinct_id": pid,
                "reason": reason,
                "n_overlap_partners": int(row["n_overlap_partners"]),
                "sum_overlap_area_m2": float(row["sum_overlap_area_m2"]),
                "area_m2": float(row["area_m2"]),
            },
        )
    dropped_records.sort(key=lambda d: d["precinct_id"])

    szvk_m = _szvk_row_mask(gdf)
    pid_series = gdf[id_column].astype(str)
    remove = pid_series.isin(drop_ids) & szvk_m
    out = gdf.loc[~remove].copy()

    stats = OverlapHubDropStats(
        enabled=True,
        options_snapshot=snap,
        n_candidates_hard=n_hard,
        n_candidates_soft=n_soft,
        n_dropped=n_drop,
        dropped_records=dropped_records,
    )
    return out, stats
