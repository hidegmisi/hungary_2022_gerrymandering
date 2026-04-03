"""Tests for precinct geometry QA (slice 1: scalar metrics)."""

from __future__ import annotations

import math

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import MultiPolygon, Polygon, box

from hungary_ge.io.precinct_geometry_qa import (
    GeometryQAOptions,
    apply_qa_flags,
    compute_precinct_metrics,
    compute_precinct_overlaps,
    filter_szvk_rows,
    summarize_qa,
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


def test_compute_precinct_overlaps_two_overlapping_squares() -> None:
    a = box(0.0, 0.0, 1.0, 1.0)
    b = box(0.5, 0.0, 1.5, 1.0)
    gdf = gpd.GeoDataFrame(
        {
            DEFAULT_PRECINCT_ID_COLUMN: ["p-a", "p-b"],
            "maz": ["01", "01"],
            "geometry": [a, b],
        },
        crs="EPSG:32633",
    )
    agg, edges = compute_precinct_overlaps(
        gdf,
        min_overlap_m2=0.01,
        min_overlap_ratio=None,
    )
    assert len(agg) == 2
    assert agg["n_overlap_partners"].tolist() == [1, 1]
    assert math.isclose(agg["sum_overlap_area_m2"].iloc[0], 0.5, rel_tol=1e-9)
    assert math.isclose(agg["max_overlap_area_m2"].iloc[0], 0.5, rel_tol=1e-9)
    assert len(edges) == 1
    assert set(edges.iloc[0][["precinct_id_a", "precinct_id_b"]].tolist()) == {
        "p-a",
        "p-b",
    }
    assert math.isclose(edges["intersection_area_m2"].iloc[0], 0.5, rel_tol=1e-9)


def test_compute_precinct_overlaps_adjacent_touch_no_material() -> None:
    a = box(0.0, 0.0, 1.0, 1.0)
    b = box(1.0, 0.0, 2.0, 1.0)
    gdf = gpd.GeoDataFrame(
        {
            DEFAULT_PRECINCT_ID_COLUMN: ["p-a", "p-b"],
            "maz": ["01", "01"],
            "geometry": [a, b],
        },
        crs="EPSG:32633",
    )
    agg, edges = compute_precinct_overlaps(gdf, min_overlap_m2=1.0)
    assert agg["n_overlap_partners"].tolist() == [0, 0]
    assert len(edges) == 0


def test_compute_precinct_overlaps_hub_covers_many() -> None:
    hub = box(0.0, 0.0, 10.0, 10.0)
    s1 = box(1.0, 1.0, 2.0, 2.0)
    s2 = box(3.0, 1.0, 4.0, 2.0)
    s3 = box(5.0, 1.0, 6.0, 2.0)
    gdf = gpd.GeoDataFrame(
        {
            DEFAULT_PRECINCT_ID_COLUMN: ["hub", "s1", "s2", "s3"],
            "maz": ["01", "01", "01", "01"],
            "geometry": [hub, s1, s2, s3],
        },
        crs="EPSG:32633",
    )
    agg, edges = compute_precinct_overlaps(
        gdf,
        min_overlap_m2=0.01,
        min_overlap_ratio=None,
    )
    hub_row = agg.loc[agg["precinct_id"] == "hub"].iloc[0]
    assert hub_row["n_overlap_partners"] == 3
    assert len(edges) == 3


def test_compute_precinct_overlaps_order_matches_input_gdf() -> None:
    gdf = gpd.GeoDataFrame(
        {
            DEFAULT_PRECINCT_ID_COLUMN: ["second", "first"],
            "maz": ["02", "01"],
            "geometry": [box(100, 0, 101, 1), box(0, 0, 1, 1)],
        },
        crs="EPSG:32633",
    )
    agg, _edges = compute_precinct_overlaps(gdf)
    assert agg["precinct_id"].tolist() == ["second", "first"]


def test_compute_precinct_overlaps_missing_maz_column() -> None:
    gdf = gpd.GeoDataFrame(
        {DEFAULT_PRECINCT_ID_COLUMN: ["x"], "geometry": [box(0, 0, 1, 1)]},
        crs="EPSG:32633",
    )
    with pytest.raises(ValueError, match="missing maz column"):
        compute_precinct_overlaps(gdf)


def test_compute_precinct_overlaps_empty_gdf() -> None:
    gdf = gpd.GeoDataFrame(
        {
            DEFAULT_PRECINCT_ID_COLUMN: pd.Series(dtype=str),
            "maz": pd.Series(dtype=str),
            "geometry": gpd.GeoSeries(dtype="geometry"),
        },
        crs="EPSG:32633",
    )
    agg, edges = compute_precinct_overlaps(gdf)
    assert len(agg) == 0
    assert len(edges) == 0


def _synthetic_metrics_overlap(
    *,
    n: int,
    areas: list[float],
    partners: list[int] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    pids = [f"p{i:03d}" for i in range(n)]
    if partners is None:
        partners = [0] * n
    metrics = pd.DataFrame(
        {
            "precinct_id": pids,
            "area_m2": areas,
            "perimeter_m": [40.0] * n,
            "polsby_popper": [0.7] * n,
            "n_polygon_parts": [1] * n,
            "n_holes": [0] * n,
        }
    )
    overlap = pd.DataFrame(
        {
            "precinct_id": pids,
            "n_overlap_partners": partners,
            "sum_overlap_area_m2": [0.0] * n,
            "max_overlap_area_m2": [0.0] * n,
            "max_overlap_ratio": [0.0] * n,
        }
    )
    return metrics, overlap


def test_apply_qa_flags_tukey_log_area_outlier() -> None:
    n = 22
    # Need spread in area so county IQR > 0; Tukey is skipped when IQR == 0.
    areas = [80.0 + float((i * 7) % 50) for i in range(n - 1)] + [1e12]
    maz = ["01"] * n
    metrics, overlap = _synthetic_metrics_overlap(n=n, areas=areas)
    flags = apply_qa_flags(
        metrics,
        overlap,
        maz=maz,
        options=GeometryQAOptions(min_county_n_for_tukey=20),
    )
    last = flags.loc[flags["precinct_id"] == "p021", "qa_severity"].iloc[0]
    assert last in ("warn", "severe")
    assert (
        "log_area_tukey"
        in flags.loc[flags["precinct_id"] == "p021", "qa_reasons"].iloc[0]
    )


def test_apply_qa_flags_overlap_partner_thresholds() -> None:
    n = 3
    metrics, overlap = _synthetic_metrics_overlap(
        n=n,
        areas=[100.0, 100.0, 100.0],
        partners=[0, 2, 5],
    )
    flags = apply_qa_flags(
        metrics,
        overlap,
        maz=["01", "01", "01"],
        options=GeometryQAOptions(
            warn_if_overlap_partners_ge=2,
            severe_if_overlap_partners_ge=4,
        ),
    )
    assert flags.loc[flags["precinct_id"] == "p000", "qa_severity"].iloc[0] == "ok"
    assert flags.loc[flags["precinct_id"] == "p001", "qa_severity"].iloc[0] == "warn"
    assert flags.loc[flags["precinct_id"] == "p002", "qa_severity"].iloc[0] == "severe"


def test_apply_qa_flags_polsby_warn() -> None:
    n = 2
    metrics, overlap = _synthetic_metrics_overlap(
        n=n,
        areas=[100.0, 100.0],
    )
    metrics.loc[0, "polsby_popper"] = 0.001
    flags = apply_qa_flags(
        metrics,
        overlap,
        maz=["01", "01"],
        options=GeometryQAOptions(warn_polsby_below=0.01),
    )
    r0 = flags.loc[flags["precinct_id"] == "p000", "qa_reasons"].iloc[0]
    assert "polsby_popper" in r0
    assert flags.loc[flags["precinct_id"] == "p001", "qa_severity"].iloc[0] == "ok"


def test_apply_qa_flags_maz_length_mismatch() -> None:
    metrics, overlap = _synthetic_metrics_overlap(n=2, areas=[1.0, 1.0])
    with pytest.raises(ValueError, match="maz length"):
        apply_qa_flags(metrics, overlap, maz=["01"])


def test_apply_qa_flags_empty() -> None:
    metrics = pd.DataFrame(
        {
            "precinct_id": pd.Series(dtype=str),
            "area_m2": pd.Series(dtype=float),
            "perimeter_m": pd.Series(dtype=float),
            "polsby_popper": pd.Series(dtype=float),
            "n_polygon_parts": pd.Series(dtype=int),
            "n_holes": pd.Series(dtype=int),
        }
    )
    overlap = pd.DataFrame(
        {
            "precinct_id": pd.Series(dtype=str),
            "n_overlap_partners": pd.Series(dtype="int64"),
            "sum_overlap_area_m2": pd.Series(dtype=float),
            "max_overlap_area_m2": pd.Series(dtype=float),
            "max_overlap_ratio": pd.Series(dtype=float),
        }
    )
    out = apply_qa_flags(metrics, overlap, maz=[])
    assert len(out) == 0
    assert "qa_severity" in out.columns


def test_summarize_qa_counts_and_hotspots() -> None:
    metrics, overlap = _synthetic_metrics_overlap(
        n=3,
        areas=[100.0, 100.0, 100.0],
        partners=[0, 1, 3],
    )
    flags = apply_qa_flags(
        metrics,
        overlap,
        maz=["01", "01", "01"],
        options=GeometryQAOptions(
            warn_if_overlap_partners_ge=1,
            severe_if_overlap_partners_ge=3,
        ),
    )
    edges = pd.DataFrame(
        {
            "precinct_id_a": ["p000", "p001"],
            "precinct_id_b": ["p001", "p002"],
            "intersection_area_m2": [1.0, 1.0],
        }
    )
    s = summarize_qa(flags, edges_df=edges, top_n_hotspots=2)
    assert s["n_severe"] >= 1
    assert s["n_material_overlap_pairs"] == 2
    assert s["n_precincts_with_overlap"] == 2
    assert len(s["top_overlap_hotspots"]) <= 2
