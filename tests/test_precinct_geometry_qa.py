"""Tests for precinct geometry QA (slice 1: scalar metrics)."""

from __future__ import annotations

import math

import geopandas as gpd
import pytest
from shapely.geometry import MultiPolygon, Polygon, box

from hungary_ge.io.precinct_geometry_qa import (
    compute_precinct_metrics,
    filter_szvk_rows,
)
from hungary_ge.problem import DEFAULT_PRECINCT_ID_COLUMN


def test_filter_szvk_rows_no_column_keeps_all() -> None:
    gdf = gpd.GeoDataFrame(
        {
            DEFAULT_PRECINCT_ID_COLUMN: ["01-001-001"],
            "geometry": [box(19, 47, 19.01, 47.01)],
        },
        crs="EPSG:4326",
    )
    out = filter_szvk_rows(gdf)
    assert len(out) == 1


def test_filter_szvk_rows_excludes_void() -> None:
    gdf = gpd.GeoDataFrame(
        {
            DEFAULT_PRECINCT_ID_COLUMN: ["01-001-001", "gap-01-0000"],
            "unit_kind": ["szvk", "void"],
            "geometry": [box(19, 47, 19.01, 47.01), box(19.02, 47, 19.03, 47.01)],
        },
        crs="EPSG:4326",
    )
    out = filter_szvk_rows(gdf)
    assert len(out) == 1
    assert out.iloc[0][DEFAULT_PRECINCT_ID_COLUMN] == "01-001-001"


def test_compute_precinct_metrics_square_polsby() -> None:
    # ~1 km square in EPSG:32633 (Hungary): lon/lat box reprojected is not exact square;
    # build directly in metric CRS for a true square.
    side_m = 1000.0
    square = box(500_000.0, 5_000_000.0, 500_000.0 + side_m, 5_000_000.0 + side_m)
    gdf = gpd.GeoDataFrame(
        {DEFAULT_PRECINCT_ID_COLUMN: ["test-001"], "geometry": [square]},
        crs="EPSG:32633",
    )
    m = compute_precinct_metrics(gdf, metric_crs="EPSG:32633")
    assert len(m) == 1
    row = m.iloc[0]
    assert math.isclose(row["area_m2"], side_m**2, rel_tol=1e-9)
    assert math.isclose(row["perimeter_m"], 4.0 * side_m, rel_tol=1e-9)
    expected_pp = math.pi / 4.0  # Polsby–Popper of a square
    assert math.isclose(row["polsby_popper"], expected_pp, rel_tol=1e-9)
    assert row["n_polygon_parts"] == 1
    assert row["n_holes"] == 0


def test_compute_precinct_metrics_wgs84_reprojects() -> None:
    gdf = gpd.GeoDataFrame(
        {
            DEFAULT_PRECINCT_ID_COLUMN: ["01-001-001"],
            "geometry": [box(19.0, 47.0, 19.01, 47.01)],
        },
        crs="EPSG:4326",
    )
    m = compute_precinct_metrics(gdf)
    row = m.iloc[0]
    assert row["area_m2"] > 0
    assert 0 < row["polsby_popper"] <= 1.0 + 1e-6


def test_compute_precinct_metrics_multipolygon_parts_and_holes() -> None:
    p1 = box(0, 0, 1, 1)
    p2 = box(2, 0, 3, 1)
    # ring with a hole: exterior 0..4, hole 1..3
    outer = [(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0), (0.0, 0.0)]
    inner = [(1.0, 1.0), (3.0, 1.0), (3.0, 3.0), (1.0, 3.0), (1.0, 1.0)]
    with_hole = Polygon(outer, [inner])
    mp = MultiPolygon([p1, p2, with_hole])
    gdf = gpd.GeoDataFrame(
        {DEFAULT_PRECINCT_ID_COLUMN: ["mp-001"], "geometry": [mp]},
        crs="EPSG:32633",
    )
    m = compute_precinct_metrics(gdf, metric_crs="EPSG:32633")
    row = m.iloc[0]
    assert row["n_polygon_parts"] == 3
    assert row["n_holes"] == 1


def test_compute_precinct_metrics_missing_id_column() -> None:
    gdf = gpd.GeoDataFrame({"geometry": [box(0, 0, 1, 1)]}, crs="EPSG:32633")
    with pytest.raises(ValueError, match="missing id column"):
        compute_precinct_metrics(gdf)


def test_compute_precinct_metrics_missing_crs() -> None:
    gdf = gpd.GeoDataFrame(
        {DEFAULT_PRECINCT_ID_COLUMN: ["x"], "geometry": [box(0, 0, 1, 1)]},
        crs=None,
    )
    with pytest.raises(ValueError, match="no CRS"):
        compute_precinct_metrics(gdf)


def test_compute_precinct_metrics_row_order_preserved() -> None:
    gdf = gpd.GeoDataFrame(
        {
            DEFAULT_PRECINCT_ID_COLUMN: ["b", "a"],
            "geometry": [box(0, 0, 1, 1), box(0, 0, 2, 2)],
        },
        crs="EPSG:32633",
    )
    m = compute_precinct_metrics(gdf, metric_crs="EPSG:32633")
    assert m["precinct_id"].tolist() == ["b", "a"]
