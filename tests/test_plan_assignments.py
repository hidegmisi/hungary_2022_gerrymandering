"""Tests for Slice 11 plan assignment merges (viz.plan_assignments)."""

from __future__ import annotations

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
from shapely.geometry import Polygon

from hungary_ge.problem import DEFAULT_PRECINCT_ID_COLUMN
from hungary_ge.viz.plan_assignments import (
    merge_enacted_districts,
    merge_simulated_districts,
)


def _box(lon: float, lat: float, d: float = 0.01) -> Polygon:
    return Polygon(
        [(lon, lat), (lon + d, lat), (lon + d, lat + d), (lon, lat + d), (lon, lat)],
    )


def test_merge_simulated_districts_ok() -> None:
    gdf = gpd.GeoDataFrame(
        {
            DEFAULT_PRECINCT_ID_COLUMN: ["01-01-01", "01-01-02"],
            "geometry": [_box(19.0, 47.0), _box(19.1, 47.1)],
        },
        crs="EPSG:4326",
    )
    unit_ids = ("01-01-02", "01-01-01")
    dist = np.array([2, 1], dtype=np.int32)
    out = merge_simulated_districts(
        gdf,
        precinct_id_column=DEFAULT_PRECINCT_ID_COLUMN,
        unit_ids=unit_ids,
        districts=dist,
    )
    by_id = out.set_index(DEFAULT_PRECINCT_ID_COLUMN)["sim_district"]
    assert int(by_id["01-01-01"]) == 1
    assert int(by_id["01-01-02"]) == 2


def test_merge_simulated_districts_set_mismatch() -> None:
    gdf = gpd.GeoDataFrame(
        {
            DEFAULT_PRECINCT_ID_COLUMN: ["a", "b"],
            "geometry": [_box(19.0, 47.0), _box(19.1, 47.1)],
        },
        crs="EPSG:4326",
    )
    with pytest.raises(ValueError, match="mismatch"):
        merge_simulated_districts(
            gdf,
            precinct_id_column=DEFAULT_PRECINCT_ID_COLUMN,
            unit_ids=("a", "c"),
            districts=np.array([1, 2]),
        )


def test_merge_enacted_void_may_be_null() -> None:
    gdf = gpd.GeoDataFrame(
        {
            DEFAULT_PRECINCT_ID_COLUMN: ["01-t-01", "gap-01-0001"],
            "unit_kind": ["szvk", "void"],
            "geometry": [_box(19.0, 47.0), _box(19.2, 47.2)],
        },
        crs="EPSG:4326",
    )
    focal = pd.DataFrame(
        {
            DEFAULT_PRECINCT_ID_COLUMN: ["01-t-01"],
            "oevk_id_full": ["hu-2022-01-001"],
        },
    )
    out = merge_enacted_districts(
        gdf,
        focal,
        precinct_id_column=DEFAULT_PRECINCT_ID_COLUMN,
    )
    assert out.loc[0, "enacted_oevk_full"] == "hu-2022-01-001"
    assert pd.isna(out.loc[1, "enacted_oevk_full"])


def test_merge_enacted_allow_missing_szvk() -> None:
    gdf = gpd.GeoDataFrame(
        {
            DEFAULT_PRECINCT_ID_COLUMN: ["01-a", "01-b"],
            "unit_kind": ["szvk", "szvk"],
            "geometry": [_box(19.0, 47.0), _box(19.1, 47.1)],
        },
        crs="EPSG:4326",
    )
    focal = pd.DataFrame(
        {DEFAULT_PRECINCT_ID_COLUMN: ["01-a"], "oevk_id_full": ["x"]},
    )
    out = merge_enacted_districts(
        gdf,
        focal,
        precinct_id_column=DEFAULT_PRECINCT_ID_COLUMN,
        require_all_szvk=False,
    )
    assert out.loc[0, "enacted_oevk_full"] == "x"
    assert pd.isna(out.loc[1, "enacted_oevk_full"])


def test_merge_enacted_szvk_requires_focal() -> None:
    gdf = gpd.GeoDataFrame(
        {
            DEFAULT_PRECINCT_ID_COLUMN: ["01-a", "01-b"],
            "unit_kind": ["szvk", "szvk"],
            "geometry": [_box(19.0, 47.0), _box(19.1, 47.1)],
        },
        crs="EPSG:4326",
    )
    focal = pd.DataFrame(
        {DEFAULT_PRECINCT_ID_COLUMN: ["01-a"], "oevk_id_full": ["x"]},
    )
    with pytest.raises(ValueError, match="lack focal"):
        merge_enacted_districts(
            gdf,
            focal,
            precinct_id_column=DEFAULT_PRECINCT_ID_COLUMN,
        )


def test_merge_enacted_no_unit_kind_requires_all() -> None:
    gdf = gpd.GeoDataFrame(
        {
            DEFAULT_PRECINCT_ID_COLUMN: ["p1", "p2"],
            "geometry": [_box(19.0, 47.0), _box(19.1, 47.1)],
        },
        crs="EPSG:4326",
    )
    focal = pd.DataFrame(
        {DEFAULT_PRECINCT_ID_COLUMN: ["p1"], "oevk_id_full": ["o1"]},
    )
    with pytest.raises(ValueError, match="lack focal"):
        merge_enacted_districts(
            gdf,
            focal,
            precinct_id_column=DEFAULT_PRECINCT_ID_COLUMN,
        )


def test_merge_enacted_duplicate_focal_precinct() -> None:
    gdf = gpd.GeoDataFrame(
        {
            DEFAULT_PRECINCT_ID_COLUMN: ["p1"],
            "geometry": [_box(19.0, 47.0)],
        },
        crs="EPSG:4326",
    )
    focal = pd.DataFrame(
        {
            DEFAULT_PRECINCT_ID_COLUMN: ["p1", "p1"],
            "oevk_id_full": ["a", "b"],
        },
    )
    with pytest.raises(ValueError, match="duplicate"):
        merge_enacted_districts(
            gdf,
            focal,
            precinct_id_column=DEFAULT_PRECINCT_ID_COLUMN,
        )
