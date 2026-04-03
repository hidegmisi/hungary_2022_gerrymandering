"""Tests for metric precinct geometry repair (ETL consolidation)."""

from __future__ import annotations

import geopandas as gpd
import pytest
from shapely.geometry import Polygon, box

from hungary_ge.io.precinct_geometry_repair import repair_precinct_geometries
from hungary_ge.problem import DEFAULT_PRECINCT_ID_COLUMN


@pytest.fixture
def bowtie_invalid_polygon() -> Polygon:
    # Self-intersecting ring in metric CRS
    return Polygon([(0, 0), (2, 2), (2, 0), (0, 2), (0, 0)])


def test_repair_makes_invalid_polygon_valid_or_empty(
    bowtie_invalid_polygon: Polygon,
) -> None:
    gdf = gpd.GeoDataFrame(
        {
            DEFAULT_PRECINCT_ID_COLUMN: ["x-001"],
            "maz": ["01"],
            "geometry": [bowtie_invalid_polygon],
        },
        crs="EPSG:32633",
    )
    assert not gdf.geometry.iloc[0].is_valid
    out, stats = repair_precinct_geometries(gdf)
    assert stats.n_rows_in_scope == 1
    assert stats.n_invalid_before >= 1
    g = out.geometry.iloc[0]
    assert g.is_valid
    assert not g.is_empty


def test_repair_void_rows_untouched_when_unit_kind_present() -> None:
    sz = box(0, 0, 1, 1)
    vo = box(10, 10, 11, 11)
    gdf = gpd.GeoDataFrame(
        {
            DEFAULT_PRECINCT_ID_COLUMN: ["a", "gap-01-0000"],
            "maz": ["01", "01"],
            "unit_kind": ["szvk", "void"],
            "geometry": [sz, vo],
        },
        crs="EPSG:32633",
    )
    void_before_wkt = vo.wkt
    out, stats = repair_precinct_geometries(gdf)
    assert stats.n_rows_in_scope == 1
    void_row = out[out["unit_kind"].astype(str) == "void"].iloc[0]
    assert void_row.geometry.wkt == void_before_wkt


def test_repair_area_delta_reporting() -> None:
    gdf = gpd.GeoDataFrame(
        {
            DEFAULT_PRECINCT_ID_COLUMN: ["p1", "p2"],
            "maz": ["01", "01"],
            "geometry": [box(0, 0, 100, 100), box(200, 200, 201, 201)],
        },
        crs="EPSG:32633",
    )
    _out, stats = repair_precinct_geometries(
        gdf,
        large_delta_threshold_m2=0.0,
        max_ids_in_manifest=10,
    )
    assert stats.max_abs_area_delta_m2 >= 0.0
    assert isinstance(stats.precinct_ids_large_area_delta, list)


def test_repair_manifest_dict_json_safe_keys(bowtie_invalid_polygon: Polygon) -> None:
    gdf = gpd.GeoDataFrame(
        {DEFAULT_PRECINCT_ID_COLUMN: ["x"], "geometry": [bowtie_invalid_polygon]},
        crs="EPSG:32633",
    )
    _out, stats = repair_precinct_geometries(gdf)
    d = stats.as_manifest_dict()
    assert d["metric_crs"] == "EPSG:32633"
    assert "n_rows_in_scope" in d
