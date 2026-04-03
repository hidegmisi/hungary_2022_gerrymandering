"""redist bundle export (Slice 6, no R)."""

from __future__ import annotations

import json
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import box

from hungary_ge.graph import AdjacencyBuildOptions, build_adjacency
from hungary_ge.problem import DEFAULT_PRECINCT_ID_COLUMN, OevkProblem
from hungary_ge.problem.precinct_index_map import prepare_precinct_layer
from hungary_ge.sampling.redist_export import (
    RUN_SCHEMA_VERSION,
    export_redist_bundle,
    load_run_manifest,
    precinct_layer_from_bundle,
)
from hungary_ge.sampling.sampler_config import SamplerConfig


def _grid_with_pop() -> gpd.GeoDataFrame:
    polys = [
        box(0, 0, 1, 1),
        box(1, 0, 2, 1),
        box(0, 1, 1, 2),
        box(1, 1, 2, 2),
    ]
    return gpd.GeoDataFrame(
        {
            DEFAULT_PRECINCT_ID_COLUMN: ["d", "c", "b", "a"],
            "pop": [10.0, 20.0, 15.0, 0.0],
            "geometry": polys,
        },
        crs="EPSG:4326",
    )


def test_export_redist_bundle_writes_files(tmp_path: Path) -> None:
    prob = OevkProblem(
        county_column=None,
        pop_column="pop",
        crs="EPSG:4326",
        ndists=2,
    )
    gdf = _grid_with_pop()
    gdf2, pmap = prepare_precinct_layer(gdf, prob)
    g = build_adjacency(
        gdf2, prob, pmap, options=AdjacencyBuildOptions(contiguity="queen")
    )
    cfg = SamplerConfig(n_sims=5, n_runs=1, seed=7, work_dir=tmp_path, pop_tol=0.2)
    run_dir = tmp_path / "bundle"
    export_redist_bundle(
        gdf2,
        g,
        config=cfg,
        run_dir=run_dir,
        ndists=2,
        precinct_id_column=prob.precinct_id_column,
        pop_column="pop",
    )
    assert (run_dir / "precincts.gpkg").is_file()
    assert (run_dir / "edges.csv").is_file()
    assert (run_dir / "run.json").is_file()
    meta = load_run_manifest(run_dir)
    assert meta["schema_version"] == RUN_SCHEMA_VERSION
    assert meta["ndists"] == 2
    assert meta["n_nodes"] == 4
    assert meta["n_sims"] == 5
    assert meta["seed"] == 7
    assert meta["smc_silent"] is False
    assert meta["smc_verbose"] is True
    edges = pd.read_csv(run_dir / "edges.csv")
    assert list(edges.columns) == ["i", "j"]
    assert edges["i"].max() < 4 and edges["j"].max() < 4
    back = precinct_layer_from_bundle(run_dir)
    # Lexicographic precinct_id order: a, b, c, d
    assert list(back["pop"]) == [0.0, 15.0, 20.0, 10.0]


def test_export_validates_row_order(tmp_path: Path) -> None:
    prob = OevkProblem(county_column=None, pop_column="pop", crs="EPSG:4326", ndists=2)
    gdf = _grid_with_pop()
    gdf2, pmap = prepare_precinct_layer(gdf, prob)
    g = build_adjacency(gdf2, prob, pmap)
    bad = gdf2.copy()
    col = DEFAULT_PRECINCT_ID_COLUMN
    bad.iloc[0, bad.columns.get_loc(col)] = "z"
    cfg = SamplerConfig(n_sims=1, n_runs=1, seed=1, work_dir=Path("."), pop_tol=0.2)
    with pytest.raises(ValueError, match="row order"):
        export_redist_bundle(
            bad,
            g,
            config=cfg,
            run_dir=tmp_path / "misaligned",
            ndists=2,
            precinct_id_column=prob.precinct_id_column,
            pop_column="pop",
        )


def test_export_redist_bundle_quiet_smc_flags(tmp_path: Path) -> None:
    prob = OevkProblem(
        county_column=None,
        pop_column="pop",
        crs="EPSG:4326",
        ndists=2,
    )
    gdf2, pmap = prepare_precinct_layer(_grid_with_pop(), prob)
    g = build_adjacency(gdf2, prob, pmap)
    cfg = SamplerConfig(
        n_sims=1,
        n_runs=1,
        work_dir=tmp_path,
        redist_progress=False,
    )
    run_dir = tmp_path / "quiet"
    export_redist_bundle(
        gdf2,
        g,
        config=cfg,
        run_dir=run_dir,
        ndists=2,
        precinct_id_column=prob.precinct_id_column,
        pop_column="pop",
    )
    meta = load_run_manifest(run_dir)
    assert meta["smc_silent"] is True
    assert meta["smc_verbose"] is False


def test_run_json_is_valid_utf8(tmp_path: Path) -> None:
    prob = OevkProblem(county_column=None, pop_column="pop", crs="EPSG:4326", ndists=3)
    gdf2, pmap = prepare_precinct_layer(_grid_with_pop(), prob)
    g = build_adjacency(gdf2, prob, pmap)
    cfg = SamplerConfig(n_sims=1, n_runs=1, work_dir=tmp_path)
    run_dir = tmp_path / "b"
    export_redist_bundle(
        gdf2,
        g,
        config=cfg,
        run_dir=run_dir,
        ndists=3,
        precinct_id_column=prob.precinct_id_column,
        pop_column="pop",
    )
    json.loads((run_dir / "run.json").read_text(encoding="utf-8"))
