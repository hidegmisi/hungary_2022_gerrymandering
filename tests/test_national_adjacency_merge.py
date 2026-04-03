"""County-merge national adjacency (fuzzy) + bicounty cross edges."""

from __future__ import annotations

import json
from pathlib import Path

import geopandas as gpd
import pytest
from shapely.geometry import box

from hungary_ge.graph import AdjacencyBuildOptions, build_adjacency
from hungary_ge.graph.national_adjacency import (
    build_national_adjacency_merged,
    county_adjacent_maz_pairs,
)
from hungary_ge.io.geoio import write_processed_geoparquet
from hungary_ge.pipeline.runner import main
from hungary_ge.problem import (
    DEFAULT_PRECINCT_ID_COLUMN,
    OevkProblem,
    prepare_precinct_layer,
)


def test_county_adjacent_maz_pairs_two_touching_counties() -> None:
    gdf = gpd.GeoDataFrame(
        {
            "maz": ["01", "01", "02", "02"],
            "geometry": [
                box(0, 0, 1, 1),
                box(0, 1, 1, 2),
                box(1, 0, 2, 1),
                box(1, 1, 2, 2),
            ],
        },
        crs="EPSG:4326",
    )
    assert county_adjacent_maz_pairs(gdf) == {("01", "02")}


def test_county_pairs_skips_void_for_dissolve() -> None:
    gdf = gpd.GeoDataFrame(
        {
            "maz": ["01", "01", "02", "02", "02"],
            "unit_kind": ["szvk", "szvk", "szvk", "szvk", "void"],
            "geometry": [
                box(0, 0, 1, 1),
                box(0, 1, 1, 2),
                box(1, 0, 2, 1),
                box(1, 1, 2, 2),
                box(10, 10, 11, 11),
            ],
        },
        crs="EPSG:4326",
    )
    assert county_adjacent_maz_pairs(gdf) == {("01", "02")}


def test_merged_national_includes_cross_maz_edges() -> None:
    """Cross-border adjacency appears only when bicounty edges are merged."""
    gdf = gpd.GeoDataFrame(
        {
            "maz": ["01", "02"],
            DEFAULT_PRECINCT_ID_COLUMN: ["01-x", "02-x"],
            "geometry": [box(0, 0, 1, 1), box(1, 0, 2, 1)],
        },
        crs="EPSG:4326",
    )
    prob = OevkProblem(county_column=None, pop_column=None, crs="EPSG:4326")
    opts = AdjacencyBuildOptions(fuzzy=True, fuzzy_buffering=True, fuzzy_buffer_m=3.0)
    merged = build_national_adjacency_merged(gdf, prob, opts)

    only_01 = gdf[gdf["maz"] == "01"].copy()
    g1, p1 = prepare_precinct_layer(only_01, prob)
    g01 = build_adjacency(g1, prob, p1, options=opts)
    only_02 = gdf[gdf["maz"] == "02"].copy()
    g2, p2 = prepare_precinct_layer(only_02, prob)
    g02 = build_adjacency(g2, prob, p2, options=opts)

    assert g01.n_edges == 0 and g02.n_edges == 0
    assert merged.n_edges == 1
    i1 = merged.order.index_of("01-x")
    i2 = merged.order.index_of("02-x")
    assert i2 in merged.neighbor_lists[i1]


def test_merged_national_topological_includes_cross_maz_edges() -> None:
    """Same geometry as fuzzy cross test: queen shares an edge between counties."""
    gdf = gpd.GeoDataFrame(
        {
            "maz": ["01", "02"],
            DEFAULT_PRECINCT_ID_COLUMN: ["01-x", "02-x"],
            "geometry": [box(0, 0, 1, 1), box(1, 0, 2, 1)],
        },
        crs="EPSG:4326",
    )
    prob = OevkProblem(county_column=None, pop_column=None, crs="EPSG:4326")
    opts = AdjacencyBuildOptions(contiguity="queen")
    merged = build_national_adjacency_merged(gdf, prob, opts)
    assert merged.contiguity == "queen:county_merged"
    assert merged.n_edges == 1
    i1 = merged.order.index_of("01-x")
    i2 = merged.order.index_of("02-x")
    assert i2 in merged.neighbor_lists[i1]


def test_build_national_adjacency_requires_maz_column() -> None:
    gdf = gpd.GeoDataFrame(
        {DEFAULT_PRECINCT_ID_COLUMN: ["a"], "geometry": [box(0, 0, 1, 1)]},
        crs="EPSG:4326",
    )
    prob = OevkProblem(county_column=None, pop_column=None, crs="EPSG:4326")
    opts = AdjacencyBuildOptions(fuzzy=True, fuzzy_buffering=True, fuzzy_buffer_m=1.0)
    with pytest.raises(ValueError, match="maz"):
        build_national_adjacency_merged(gdf, prob, opts)


def test_pilot_national_graph_uses_county_merge_when_maz_present(
    tmp_path: Path,
) -> None:
    repo = tmp_path
    proc = repo / "data" / "processed"
    proc.mkdir(parents=True)
    pq = proc / "precincts.parquet"
    gdf = _grid_gdf()
    write_processed_geoparquet(gdf, pq)
    code = main(
        [
            "--repo-root",
            str(repo),
            "--only",
            "graph",
            "--parquet",
            str(pq),
        ],
    )
    assert code == 0
    meta_path = repo / "data" / "processed" / "graph" / "adjacency_edges.meta.json"
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    assert meta.get("national_county_merge") is True
    assert "fuzzy:buffered:county_merged" in meta.get("contiguity", "")


def _grid_gdf() -> gpd.GeoDataFrame:
    polys = [
        box(0, 0, 1, 1),
        box(1, 0, 2, 1),
        box(0, 1, 1, 2),
        box(1, 1, 2, 2),
    ]
    return gpd.GeoDataFrame(
        {
            DEFAULT_PRECINCT_ID_COLUMN: ["d", "c", "b", "a"],
            "maz": ["01", "01", "01", "02"],
            "geometry": polys,
        },
        crs="EPSG:4326",
    )
