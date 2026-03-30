"""Adjacency graph tests (Slice 3)."""

from __future__ import annotations

import json
from pathlib import Path

import geopandas as gpd
import pytest
from shapely.geometry import Polygon, box

from hungary_ge.graph import (
    AdjacencyBuildOptions,
    AdjacencyPatch,
    apply_adjacency_patch,
    build_adjacency,
    load_adjacency,
    save_adjacency,
)
from hungary_ge.graph.adjacency_graph import adjacency_summary
from hungary_ge.problem import DEFAULT_PRECINCT_ID_COLUMN, OevkProblem
from hungary_ge.problem.precinct_index_map import prepare_precinct_layer


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
            "geometry": polys,
        },
        crs="EPSG:4326",
    )


def test_queen_vs_rook_edge_counts() -> None:
    prob = OevkProblem(county_column=None, pop_column=None, crs="EPSG:4326")
    gdf = _grid_gdf()
    gdf2, pmap = prepare_precinct_layer(gdf, prob)
    gq = build_adjacency(
        gdf2,
        prob,
        pmap,
        options=AdjacencyBuildOptions(contiguity="queen"),
    )
    gr = build_adjacency(
        gdf2,
        prob,
        pmap,
        options=AdjacencyBuildOptions(contiguity="rook"),
    )
    assert gq.n_edges == 6
    assert gr.n_edges == 4
    assert gq.n_components == 1
    assert gq.largest_component_size == 4


def test_neighbors_and_degree() -> None:
    prob = OevkProblem(county_column=None, pop_column=None, crs="EPSG:4326")
    gdf = _grid_gdf()
    gdf2, pmap = prepare_precinct_layer(gdf, prob)
    g = build_adjacency(
        gdf2, prob, pmap, options=AdjacencyBuildOptions(contiguity="queen")
    )
    # sorted ids a,b,c,d -> indices 0,1,2,3
    assert g.degree(0) == len(g.neighbors(0))
    s = adjacency_summary(g)
    assert s["n_edges"] == g.n_edges


def test_order_mismatch_raises() -> None:
    prob = OevkProblem(county_column=None, pop_column=None, crs="EPSG:4326")
    gdf = _grid_gdf()
    gdf2, pmap = prepare_precinct_layer(gdf, prob)
    bad = gdf2.copy()
    # swap first two labels so order no longer matches pmap
    col = DEFAULT_PRECINCT_ID_COLUMN
    ids = list(bad[col])
    ids[0], ids[1] = ids[1], ids[0]
    bad[col] = ids
    with pytest.raises(ValueError, match="does not match"):
        build_adjacency(bad, prob, pmap)


@pytest.mark.filterwarnings("ignore:The weights matrix is not fully connected:UserWarning")
def test_island_two_components() -> None:
    prob = OevkProblem(county_column=None, pop_column=None, crs="EPSG:4326")
    gdf = gpd.GeoDataFrame(
        {
            DEFAULT_PRECINCT_ID_COLUMN: ["b", "a"],
            "geometry": [box(0, 0, 1, 1), box(10, 10, 11, 11)],
        },
        crs="EPSG:4326",
    )
    gdf2, pmap = prepare_precinct_layer(gdf, prob)
    g = build_adjacency(gdf2, prob, pmap)
    assert g.n_components == 2
    assert len(g.island_nodes) == 2  # both degree 0
    assert g.largest_component_size == 1


def test_save_load_roundtrip(tmp_path: Path) -> None:
    prob = OevkProblem(county_column=None, pop_column=None, crs="EPSG:4326")
    gdf = _grid_gdf()
    gdf2, pmap = prepare_precinct_layer(gdf, prob)
    g = build_adjacency(gdf2, prob, pmap)
    p = tmp_path / "e.parquet"
    save_adjacency(g, p)
    g2 = load_adjacency(p)
    assert g2.n_edges == g.n_edges
    assert g2.order.ids == g.order.ids


def test_patch_remove_edge() -> None:
    prob = OevkProblem(county_column=None, pop_column=None, crs="EPSG:4326")
    gdf = _grid_gdf()
    gdf2, pmap = prepare_precinct_layer(gdf, prob)
    g = build_adjacency(
        gdf2, prob, pmap, options=AdjacencyBuildOptions(contiguity="rook")
    )
    e0 = g.n_edges
    patch = AdjacencyPatch(remove=((0, 1),))
    g2, stats = apply_adjacency_patch(g, patch)
    assert stats.n_remove_applied == 1
    assert g2.n_edges == e0 - 1


@pytest.mark.filterwarnings("ignore:The weights matrix is not fully connected:UserWarning")
def test_fuzzy_buffering_closes_near_miss_gap() -> None:
    """Hairline gap: queen is disconnected; fuzzy_contiguity with buffering links."""
    prob = OevkProblem(county_column=None, pop_column=None, crs="EPSG:32633")
    p0 = Polygon([(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)])
    p1 = Polygon([(10.02, 1), (15, 1), (15, 5), (10.02, 5), (10.02, 1)])
    gdf = gpd.GeoDataFrame(
        {
            DEFAULT_PRECINCT_ID_COLUMN: ["a", "b"],
            "geometry": [p0, p1],
        },
        crs="EPSG:32633",
    )
    gdf2, pmap = prepare_precinct_layer(gdf, prob)
    gq = build_adjacency(
        gdf2,
        prob,
        pmap,
        options=AdjacencyBuildOptions(contiguity="queen"),
    )
    assert gq.n_components == 2
    assert gq.n_edges == 0

    opts = AdjacencyBuildOptions(
        fuzzy=True,
        fuzzy_buffering=True,
        fuzzy_buffer_m=1.0,
        fuzzy_metric_crs="EPSG:32633",
    )
    gf = build_adjacency(gdf2, prob, pmap, options=opts)
    assert gf.contiguity == "fuzzy:buffered"
    assert gf.n_components == 1
    assert gf.n_edges == 1


def test_save_adjacency_fuzzy_meta(tmp_path: Path) -> None:
    prob = OevkProblem(county_column=None, pop_column=None, crs="EPSG:32633")
    p0 = Polygon([(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)])
    p1 = Polygon([(10.02, 1), (15, 1), (15, 5), (10.02, 5), (10.02, 1)])
    gdf = gpd.GeoDataFrame(
        {
            DEFAULT_PRECINCT_ID_COLUMN: ["a", "b"],
            "geometry": [p0, p1],
        },
        crs="EPSG:32633",
    )
    gdf2, pmap = prepare_precinct_layer(gdf, prob)
    opts = AdjacencyBuildOptions(
        fuzzy=True,
        fuzzy_buffering=True,
        fuzzy_buffer_m=1.0,
    )
    g = build_adjacency(gdf2, prob, pmap, options=opts)
    p = tmp_path / "e.parquet"
    mj = tmp_path / "e.meta.json"
    save_adjacency(g, p, mj, build_options=opts)
    meta = json.loads(mj.read_text(encoding="utf-8"))
    assert meta["fuzzy_buffering"] is True
    assert meta["fuzzy_buffer_m"] == 1.0
    assert meta["fuzzy_metric_crs"] == "EPSG:32633"
    assert meta["contiguity"] == "fuzzy:buffered"


def test_patch_add_edge_idempotent() -> None:
    prob = OevkProblem(county_column=None, pop_column=None, crs="EPSG:4326")
    gdf = _grid_gdf()
    gdf2, pmap = prepare_precinct_layer(gdf, prob)
    g = build_adjacency(
        gdf2, prob, pmap, options=AdjacencyBuildOptions(contiguity="rook")
    )
    e0 = g.n_edges
    patch = AdjacencyPatch(add=((0, 3),))
    g2, stats = apply_adjacency_patch(g, patch)
    assert stats.n_add_applied == 1
    assert g2.n_edges == e0 + 1
