"""Tests for overlap-hub drop (library, ETL wiring via build script, pipeline flags)."""

from __future__ import annotations

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import box

import hungary_ge.io.precinct_geometry_hub_drop as hub_mod
from hungary_ge.io import (
    OverlapHubDropOptions,
    OverlapHubDropStats,
    drop_overlap_hub_szvk,
    hub_drop_masks_for_precinct_table,
)
from hungary_ge.problem import DEFAULT_PRECINCT_ID_COLUMN


def test_hub_drop_masks_hard_only() -> None:
    work = pd.DataFrame(
        {
            "precinct_id": ["a", "b"],
            "area_m2": [100.0, 100.0],
            "n_overlap_partners": [5, 1],
            "sum_overlap_area_m2": [10.0, 0.0],
        },
    )
    opts = OverlapHubDropOptions(hard_min_partners=3, soft_min_partners=0)
    h, s, c = hub_drop_masks_for_precinct_table(work, options=opts)
    assert h.tolist() == [True, False]
    assert s.tolist() == [False, False]
    assert c.tolist() == [True, False]


def test_hub_drop_masks_soft_requires_mass_ratio() -> None:
    work = pd.DataFrame(
        {
            "precinct_id": ["a", "b"],
            "area_m2": [100.0, 100.0],
            "n_overlap_partners": [2, 2],
            "sum_overlap_area_m2": [50.0, 400.0],
        },
    )
    opts = OverlapHubDropOptions(
        hard_min_partners=0,
        soft_min_partners=2,
        mass_ratio=2.0,
    )
    _h, s, c = hub_drop_masks_for_precinct_table(work, options=opts)
    assert s.tolist() == [False, True]
    assert c.tolist() == [False, True]


def test_hub_drop_masks_soft_skips_nonpositive_area() -> None:
    work = pd.DataFrame(
        {
            "precinct_id": ["a", "b"],
            "area_m2": [0.0, 100.0],
            "n_overlap_partners": [10, 2],
            "sum_overlap_area_m2": [500.0, 400.0],
        },
    )
    opts = OverlapHubDropOptions(
        hard_min_partners=0,
        soft_min_partners=2,
        mass_ratio=1.0,
    )
    _h, s, _c = hub_drop_masks_for_precinct_table(work, options=opts)
    assert s.tolist() == [False, True]


def test_drop_both_tiers_disabled_no_geometry_work() -> None:
    gdf = gpd.GeoDataFrame(
        {
            DEFAULT_PRECINCT_ID_COLUMN: ["x"],
            "maz": ["01"],
            "geometry": [box(0, 0, 1, 1)],
        },
        crs="EPSG:32633",
    )
    opts = OverlapHubDropOptions(hard_min_partners=0, soft_min_partners=0)
    out, stats = drop_overlap_hub_szvk(gdf, options=opts)
    assert len(out) == 1
    assert stats.enabled is False
    assert stats.n_dropped == 0


def test_drop_hub_only_hard_integration() -> None:
    # Hub overlaps four satellites; satellites do not overlap each other.
    hub = box(0, 0, 100, 100)
    s1 = box(1, 1, 30, 30)
    s2 = box(70, 1, 99, 30)
    s3 = box(1, 70, 30, 99)
    s4 = box(70, 70, 99, 99)
    gdf = gpd.GeoDataFrame(
        {
            DEFAULT_PRECINCT_ID_COLUMN: [
                "00-000-001",
                "00-000-002",
                "00-000-003",
                "00-000-004",
                "00-000-005",
            ],
            "maz": ["01"] * 5,
            "geometry": [hub, s1, s2, s3, s4],
        },
        crs="EPSG:32633",
    )
    opts = OverlapHubDropOptions(
        hard_min_partners=4,
        soft_min_partners=0,
        overlap_min_overlap_m2=1.0,
    )
    out, stats = drop_overlap_hub_szvk(gdf, options=opts)
    assert len(out) == 4
    kept = set(out[DEFAULT_PRECINCT_ID_COLUMN].astype(str))
    assert kept == {"00-000-002", "00-000-003", "00-000-004", "00-000-005"}
    assert stats.n_dropped == 1
    assert stats.dropped_records[0]["precinct_id"] == "00-000-001"
    assert stats.dropped_records[0]["reason"] == "hard"


def test_max_drop_exceeded_raises() -> None:
    # Four precincts in one maz with pairwise material overlaps (grid).
    a = box(0, 0, 15, 15)
    b = box(10, 0, 25, 15)
    c = box(0, 10, 15, 25)
    d = box(10, 10, 25, 25)
    gdf = gpd.GeoDataFrame(
        {
            DEFAULT_PRECINCT_ID_COLUMN: ["a", "b", "c", "d"],
            "maz": ["01"] * 4,
            "geometry": [a, b, c, d],
        },
        crs="EPSG:32633",
    )
    opts = OverlapHubDropOptions(
        hard_min_partners=2,
        soft_min_partners=0,
        overlap_min_overlap_m2=1.0,
        max_drop_rows=1,
        allow_exceed_max=False,
    )
    with pytest.raises(ValueError, match="overlap hub drop would remove"):
        drop_overlap_hub_szvk(gdf, options=opts)


def test_preserves_row_order_of_kept_rows() -> None:
    hub = box(0, 0, 100, 100)
    s1 = box(1, 1, 20, 20)
    s2 = box(70, 1, 90, 25)
    gdf = gpd.GeoDataFrame(
        {
            DEFAULT_PRECINCT_ID_COLUMN: ["mid", "first", "last"],
            "maz": ["01"] * 3,
            "geometry": [hub, s1, s2],
        },
        crs="EPSG:32633",
        index=[10, 20, 30],
    )
    opts = OverlapHubDropOptions(
        hard_min_partners=2,
        soft_min_partners=0,
        overlap_min_overlap_m2=1.0,
    )
    out, _stats = drop_overlap_hub_szvk(gdf, options=opts)
    assert out.index.tolist() == [20, 30]


def test_unit_kind_only_drops_szvk() -> None:
    # Same hub pattern as test_drop_hub_only_hard_integration; void row is untouched.
    hub = box(0, 0, 100, 100)
    s1 = box(1, 1, 30, 30)
    s2 = box(70, 1, 99, 30)
    s3 = box(1, 70, 30, 99)
    s4 = box(70, 70, 99, 99)
    void_geom = box(200, 200, 210, 210)
    gdf = gpd.GeoDataFrame(
        {
            DEFAULT_PRECINCT_ID_COLUMN: [
                "00-000-001",
                "00-000-002",
                "00-000-003",
                "00-000-004",
                "00-000-005",
                "gap-01",
            ],
            "maz": ["01"] * 6,
            "unit_kind": ["szvk"] * 5 + ["void"],
            "geometry": [hub, s1, s2, s3, s4, void_geom],
        },
        crs="EPSG:32633",
    )
    opts = OverlapHubDropOptions(
        hard_min_partners=4,
        soft_min_partners=0,
        overlap_min_overlap_m2=1.0,
    )
    out, stats = drop_overlap_hub_szvk(gdf, options=opts)
    assert len(out) == 5
    assert set(out["unit_kind"].astype(str)) == {"szvk", "void"}
    assert stats.n_dropped == 1


def test_manifest_dict_truncates_detail() -> None:
    stats = OverlapHubDropStats(
        enabled=True,
        options_snapshot={"x": 1},
        n_candidates_hard=3,
        n_candidates_soft=0,
        n_dropped=3,
        dropped_records=[{"precinct_id": str(i)} for i in range(10)],
    )
    m = stats.manifest_dict(max_detail=4)
    assert m["dropped_detail_truncated"] is True
    assert len(m["dropped_detail"]) == 4


def test_options_invalid_mass_ratio() -> None:
    with pytest.raises(ValueError, match="mass_ratio"):
        OverlapHubDropOptions(mass_ratio=0.0)


def test_hub_drop_reexports_match_submodule() -> None:
    assert hub_mod.OverlapHubDropOptions is OverlapHubDropOptions
    assert hub_mod.OverlapHubDropStats is OverlapHubDropStats
    assert hub_mod.drop_overlap_hub_szvk is drop_overlap_hub_szvk
    assert (
        hub_mod.hub_drop_masks_for_precinct_table is hub_drop_masks_for_precinct_table
    )
