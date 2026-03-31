"""Optional end-to-end test with Rscript + redist."""

from __future__ import annotations

import shutil
from pathlib import Path

import geopandas as gpd
import pytest
from shapely.geometry import box

from hungary_ge.graph import AdjacencyBuildOptions, build_adjacency
from hungary_ge.problem import DEFAULT_PRECINCT_ID_COLUMN, OevkProblem
from hungary_ge.problem.precinct_index_map import prepare_precinct_layer
from hungary_ge.sampling import sample_plans
from hungary_ge.sampling.redist_adapter import RedistBackendError, default_run_smc_path

pytestmark = [
    pytest.mark.requires_r,
    pytest.mark.skipif(
        shutil.which("Rscript") is None,
        reason="Rscript not on PATH",
    ),
]


def test_redist_driver_script_exists() -> None:
    assert default_run_smc_path().is_file()


def test_sample_plans_redist_tiny_grid(tmp_path: Path) -> None:
    polys = [
        box(0, 0, 1, 1),
        box(1, 0, 2, 1),
        box(0, 1, 1, 2),
        box(1, 1, 2, 2),
    ]
    gdf = gpd.GeoDataFrame(
        {
            DEFAULT_PRECINCT_ID_COLUMN: ["d", "c", "b", "a"],
            "pop": [100.0, 100.0, 100.0, 100.0],
            "geometry": polys,
        },
        crs="EPSG:4326",
    )
    prob = OevkProblem(
        county_column=None,
        pop_column="pop",
        crs="EPSG:4326",
        ndists=2,
    )
    gdf2, pmap = prepare_precinct_layer(gdf, prob)
    g = build_adjacency(
        gdf2, prob, pmap, options=AdjacencyBuildOptions(contiguity="queen")
    )
    try:
        ens = sample_plans(
            prob,
            g,
            backend="redist",
            gdf=gdf2,
            n_draws=4,
            n_runs=1,
            seed=42,
            work_dir=tmp_path,
            compactness=1.0,
            pop_tol=0.25,
        )
    except RedistBackendError as e:
        pytest.skip(f"redist / R setup incomplete: {e}")
    assert ens.n_units == 4
    assert ens.n_draws == 4
