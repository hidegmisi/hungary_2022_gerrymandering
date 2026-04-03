"""Hex void subdivision (metric CRS)."""

from __future__ import annotations

import math

import geopandas as gpd
import pytest
from shapely.geometry import box

from hungary_ge.io.gaps import (
    GapBuildOptions,
    build_gap_features_for_maz,
    merge_szvk_and_gaps,
)
from hungary_ge.io.gaps_hex import (
    HexVoidOptions,
    circumradius_from_hex_area,
    flat_top_hex_polygon,
    hex_area_from_circumradius,
    resolve_hex_cell_area_m2,
    subdivide_gap_polygons_hex,
    subdivide_one_gap_polygon,
)
from hungary_ge.problem import DEFAULT_PRECINCT_ID_COLUMN


def test_hex_area_matches_polygon() -> None:
    R = 123.45
    poly = flat_top_hex_polygon(0.0, 0.0, R)
    assert math.isclose(poly.area, hex_area_from_circumradius(R), rel_tol=1e-9)


def test_circumradius_from_hex_area_roundtrip() -> None:
    A = 500_000.0
    R = circumradius_from_hex_area(A)
    assert math.isclose(hex_area_from_circumradius(R), A, rel_tol=1e-9)


def test_resolve_hex_cell_area_manual() -> None:
    opts = HexVoidOptions(
        enabled=True,
        hex_cell_area_m2=250_000.0,
        hex_min_cell_area_m2=10_000.0,
        hex_max_cell_area_m2=1_000_000.0,
    )
    a, w = resolve_hex_cell_area_m2(1.0, opts)
    assert a == 250_000.0
    assert not w


def test_resolve_hex_cell_area_auto_clamp() -> None:
    opts = HexVoidOptions(
        enabled=True,
        hex_area_factor=1.0,
        hex_min_cell_area_m2=50_000.0,
        hex_max_cell_area_m2=100_000.0,
    )
    a, _w = resolve_hex_cell_area_m2(10.0, opts)
    assert a == 50_000.0
    a2, _ = resolve_hex_cell_area_m2(200_000.0, opts)
    assert a2 == 100_000.0


def test_subdivide_rectangle_many_cells() -> None:
    gap = box(0, 0, 3000, 2000)
    cell_a = 80_000.0
    cells, trunc = subdivide_one_gap_polygon(
        gap,
        cell_a,
        min_fragment_m2=1.0,
        max_cells=5000,
        hex_opts=HexVoidOptions(min_hex_fragment_width_m=0.0),
    )
    assert not trunc
    assert len(cells) >= 40
    total_a = sum(c.area for c in cells)
    assert math.isclose(total_a, gap.area, rel_tol=0.02)


def test_small_gap_not_subdivided() -> None:
    """Gap below subdivide threshold stays one polygon."""
    shell = gpd.GeoDataFrame(
        {"maz": ["01"], "geometry": [box(0, 0, 60, 60)]},
        crs="EPSG:32633",
    )
    szvk = gpd.GeoDataFrame(
        {
            "maz": ["01"],
            "taz": ["001"],
            "szk": ["01"],
            DEFAULT_PRECINCT_ID_COLUMN: ["01-001-01"],
            "geometry": [box(10, 10, 50, 50)],
        },
        crs="EPSG:32633",
    )
    hex_opts = HexVoidOptions(
        enabled=True,
        hex_cell_area_m2=10_000.0,
        subdivide_min_void_m2=50_000.0,
        min_hex_fragment_width_m=0.0,
    )
    opts = GapBuildOptions(
        min_area_m2=1.0,
        hex_void=hex_opts,
    )
    gaps, st = build_gap_features_for_maz(
        shell,
        szvk,
        "01",
        shell_maz_column="maz",
        options=opts,
    )
    assert st.n_gap_polygons_raw == 1
    assert st.n_gap_polygons == 1
    assert st.hex_cell_area_m2_used == 10_000.0


def test_hex_disabled_raw_count_matches_final() -> None:
    shell = gpd.GeoDataFrame(
        {"maz": ["01"], "geometry": [box(0, 0, 100, 100)]},
        crs="EPSG:32633",
    )
    szvk = gpd.GeoDataFrame(
        {
            "maz": ["01"],
            "taz": ["001"],
            "szk": ["01"],
            DEFAULT_PRECINCT_ID_COLUMN: ["01-001-01"],
            "geometry": [box(0, 0, 10, 10)],
        },
        crs="EPSG:32633",
    )
    gaps, st = build_gap_features_for_maz(
        shell,
        szvk,
        "01",
        shell_maz_column="maz",
        options=GapBuildOptions(min_area_m2=1.0, hex_void=None),
    )
    assert st.n_gap_polygons_raw == st.n_gap_polygons


def test_large_gap_subdivided_more_rows() -> None:
    shell = gpd.GeoDataFrame(
        {"maz": ["01"], "geometry": [box(0, 0, 2000, 2000)]},
        crs="EPSG:32633",
    )
    szvk = gpd.GeoDataFrame(
        {
            "maz": ["01"],
            "taz": ["001"],
            "szk": ["01"],
            DEFAULT_PRECINCT_ID_COLUMN: ["01-001-01"],
            "geometry": [box(0, 0, 100, 100)],
        },
        crs="EPSG:32633",
    )
    no_hex, st0 = build_gap_features_for_maz(
        shell,
        szvk,
        "01",
        shell_maz_column="maz",
        options=GapBuildOptions(min_area_m2=500.0, hex_void=None),
    )
    hex_opts = HexVoidOptions(
        enabled=True,
        hex_cell_area_m2=120_000.0,
        subdivide_min_void_m2=50_000.0,
        max_cells_per_gap=5000,
        min_hex_fragment_width_m=0.0,
    )
    with_hex, st1 = build_gap_features_for_maz(
        shell,
        szvk,
        "01",
        shell_maz_column="maz",
        options=GapBuildOptions(min_area_m2=500.0, hex_void=hex_opts),
    )
    assert st1.n_gap_polygons > st0.n_gap_polygons
    assert st1.n_gap_polygons_raw == st0.n_gap_polygons_raw


def test_subdivide_one_gap_polygon_drops_thin_strip_with_width_filter() -> None:
    """Street-scale slivers fail the erosion test when min_hex_fragment_width_m is set."""
    gap = box(0, 0, 2000, 5)
    cell_a = 50_000.0
    opts = HexVoidOptions(min_hex_fragment_width_m=30.0)
    cells, _trunc = subdivide_one_gap_polygon(
        gap, cell_a, min_fragment_m2=1.0, max_cells=5000, hex_opts=opts
    )
    assert len(cells) == 0


def test_subdivide_one_gap_polygon_keeps_wide_strip_with_width_filter() -> None:
    gap = box(0, 0, 500, 80)
    cell_a = 40_000.0
    opts = HexVoidOptions(min_hex_fragment_width_m=30.0)
    cells, _trunc = subdivide_one_gap_polygon(
        gap, cell_a, min_fragment_m2=1.0, max_cells=5000, hex_opts=opts
    )
    assert len(cells) >= 1


def test_subdivide_one_gap_polygon_area_fraction_drops_small_clips() -> None:
    gap = box(0, 0, 3000, 2000)
    cell_a = 80_000.0
    opts = HexVoidOptions(
        min_hex_fragment_width_m=0.0,
        min_hex_fragment_area_fraction=0.99,
    )
    cells_strict, _ = subdivide_one_gap_polygon(
        gap, cell_a, min_fragment_m2=1.0, max_cells=5000, hex_opts=opts
    )
    cells_loose, _ = subdivide_one_gap_polygon(
        gap,
        cell_a,
        min_fragment_m2=1.0,
        max_cells=5000,
        hex_opts=HexVoidOptions(
            min_hex_fragment_width_m=0.0,
            min_hex_fragment_area_fraction=0.01,
        ),
    )
    assert len(cells_strict) < len(cells_loose)


def test_subdivide_gap_polygons_hex_drops_undivided_when_fraction_requires() -> None:
    """Gaps below ``subdivide_min_void`` used to bypass area-fraction; post-filter fixes."""
    polys = [box(0, 0, 10, 10)]
    out, meta = subdivide_gap_polygons_hex(
        polys,
        median_szvk_area_m2=1_000_000.0,
        hex_opts=HexVoidOptions(
            hex_cell_area_m2=100_000.0,
            subdivide_min_void_m2=500_000.0,
            min_hex_fragment_width_m=0.0,
            min_hex_fragment_area_fraction=0.15,
        ),
        min_fragment_m2=50.0,
    )
    assert len(out) == 0
    assert meta.get("n_void_polygons_dropped_post_quality") == 1


def test_subdivide_gap_polygons_hex_keeps_undivided_when_quality_off() -> None:
    polys = [box(0, 0, 10, 10)]
    out, meta = subdivide_gap_polygons_hex(
        polys,
        median_szvk_area_m2=1_000_000.0,
        hex_opts=HexVoidOptions(
            hex_cell_area_m2=100_000.0,
            subdivide_min_void_m2=500_000.0,
            min_hex_fragment_width_m=0.0,
            min_hex_fragment_area_fraction=None,
        ),
        min_fragment_m2=50.0,
    )
    assert len(out) == 1
    assert meta.get("n_void_polygons_dropped_post_quality") == 0


def test_subdivide_gap_polygons_hex_skipped_invalid_median() -> None:
    polys = [box(0, 0, 100, 100)]
    meta: dict
    out, meta = subdivide_gap_polygons_hex(
        polys,
        median_szvk_area_m2=float("nan"),
        hex_opts=HexVoidOptions(
            enabled=True,
            auto_size=True,
            min_hex_fragment_width_m=0.0,
        ),
        min_fragment_m2=1.0,
    )
    assert meta["skipped_hex"]
    assert len(out) == 1


@pytest.mark.filterwarnings(
    "ignore:The weights matrix is not fully connected:UserWarning"
)
def test_fuzzy_connects_hex_void_corridor() -> None:
    """Hex void cells can have hairline gaps vs queen; fuzzy buffering links the corridor."""
    from hungary_ge.graph import AdjacencyBuildOptions, build_adjacency
    from hungary_ge.problem import OevkProblem
    from hungary_ge.problem.precinct_index_map import prepare_precinct_layer

    shell = gpd.GeoDataFrame(
        {"maz": ["01"], "geometry": [box(0, 0, 1000, 1000)]},
        crs="EPSG:32633",
    )
    s1 = gpd.GeoDataFrame(
        {
            "maz": ["01", "01"],
            "taz": ["001", "002"],
            "szk": ["01", "01"],
            DEFAULT_PRECINCT_ID_COLUMN: ["01-001-01", "01-002-01"],
            "geometry": [
                box(0, 400, 300, 600),
                box(700, 400, 1000, 600),
            ],
        },
        crs="EPSG:32633",
    )
    hex_opts = HexVoidOptions(
        enabled=True,
        hex_cell_area_m2=25_000.0,
        subdivide_min_void_m2=1.0,
        max_cells_per_gap=500,
        min_hex_fragment_width_m=0.0,
    )
    gaps, _st = build_gap_features_for_maz(
        shell,
        s1,
        "01",
        shell_maz_column="maz",
        options=GapBuildOptions(min_area_m2=1.0, hex_void=hex_opts),
    )
    assert len(gaps) > 5
    merged = merge_szvk_and_gaps(s1, gaps)
    prob = OevkProblem(county_column=None, pop_column=None, crs="EPSG:32633")
    prepared, pmap = prepare_precinct_layer(merged, prob)
    graph = build_adjacency(
        prepared,
        prob,
        pmap,
        options=AdjacencyBuildOptions(
            fuzzy=True,
            fuzzy_buffering=True,
            fuzzy_buffer_m=3.0,
        ),
    )
    assert graph.n_components == 1
