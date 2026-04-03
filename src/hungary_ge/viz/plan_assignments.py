"""Attach simulated or enacted district labels to a prepared precinct GeoDataFrame."""

from __future__ import annotations

from collections.abc import Sequence

import geopandas as gpd
import numpy as np
import pandas as pd

VOID_UNIT_KIND = "void"


def merge_simulated_districts(
    gdf: gpd.GeoDataFrame,
    *,
    precinct_id_column: str,
    unit_ids: Sequence[str],
    districts: np.ndarray,
    out_column: str = "sim_district",
) -> gpd.GeoDataFrame:
    """Merge one simulated draw onto ``gdf`` by ``precinct_id_column``.

    Requires the set of precinct ids in ``gdf`` to exactly match ``unit_ids``.
    """
    n = len(unit_ids)
    arr = np.asarray(districts)
    if arr.shape != (n,):
        msg = f"districts shape {arr.shape} != ({n},) for n unit_ids"
        raise ValueError(msg)

    g_ids = set(gdf[precinct_id_column].astype(str).tolist())
    u_ids = {str(u) for u in unit_ids}
    if g_ids != u_ids:
        only_g = sorted(g_ids - u_ids)
        only_u = sorted(u_ids - g_ids)
        msg = (
            "precinct_id set mismatch between gdf and unit_ids: "
            f"only_in_gdf ({len(only_g)})={only_g[:20]}"
            f"{'…' if len(only_g) > 20 else ''}, "
            f"only_in_unit_ids ({len(only_u)})={only_u[:20]}"
            f"{'…' if len(only_u) > 20 else ''}"
        )
        raise ValueError(msg)

    ser = pd.Series(
        arr.astype(np.int32, copy=False),
        index=pd.Index([str(u) for u in unit_ids], name=precinct_id_column),
    )
    merged = gdf.merge(
        ser.rename(out_column).reset_index(),
        on=precinct_id_column,
        how="left",
    )
    if merged[out_column].isna().any():
        msg = f"merge produced null {out_column!r} (internal error)"
        raise ValueError(msg)
    return merged


def merge_enacted_districts(
    gdf: gpd.GeoDataFrame,
    focal_df: pd.DataFrame,
    *,
    precinct_id_column: str,
    out_column: str = "enacted_oevk_full",
    require_all_szvk: bool = True,
) -> gpd.GeoDataFrame:
    """Left-join focal ``oevk_id_full`` onto ``gdf``.

    Void rows (``unit_kind == \"void\"`` when present) may have null ``out_column``.
    When ``require_all_szvk`` (default), every non-void row must have a non-null
    enacted id. Set ``False`` for QA maps when some szvk rows lack focal data.
    """
    if precinct_id_column not in focal_df.columns:
        msg = f"focal_df missing column {precinct_id_column!r}"
        raise ValueError(msg)
    if "oevk_id_full" not in focal_df.columns:
        msg = "focal_df missing column 'oevk_id_full'"
        raise ValueError(msg)

    sub = focal_df[[precinct_id_column, "oevk_id_full"]].copy()
    dup = sub[precinct_id_column].duplicated()
    if dup.any():
        msg = f"focal_df has {int(dup.sum())} duplicate {precinct_id_column}"
        raise ValueError(msg)
    sub[precinct_id_column] = sub[precinct_id_column].astype(str)
    sub = sub.rename(columns={"oevk_id_full": out_column})

    left = gdf.copy()
    left[precinct_id_column] = left[precinct_id_column].astype(str)
    merged = left.merge(sub, on=precinct_id_column, how="left")

    if "unit_kind" in merged.columns:
        need = merged["unit_kind"].astype(str) != VOID_UNIT_KIND
    else:
        need = pd.Series(True, index=merged.index)

    missing = need & merged[out_column].isna()
    if require_all_szvk and missing.any():
        n = int(missing.sum())
        sample = merged.loc[missing, precinct_id_column].head(5).tolist()
        msg = (
            f"{n} non-void row(s) lack focal {out_column!r}; "
            f"example precinct_id={sample!r}"
        )
        raise ValueError(msg)

    # String labels where enacted exists; void / missing stay NA
    has_val = need & merged[out_column].notna()
    merged.loc[has_val, out_column] = merged.loc[has_val, out_column].astype(str)
    return merged
