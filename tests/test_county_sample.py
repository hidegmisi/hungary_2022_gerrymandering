"""County-scoped ensemble generation (Slice D)."""

from __future__ import annotations

import json
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import box

from hungary_ge.config import ProcessedPaths
from hungary_ge.ensemble.plan_ensemble import PlanEnsemble
from hungary_ge.graph import AdjacencyBuildOptions
from hungary_ge.io.geoio import write_processed_geoparquet
from hungary_ge.pipeline.county_sample import (
    county_ndists_by_maz,
    run_county_redist_sample,
)
from hungary_ge.problem import DEFAULT_PRECINCT_ID_COLUMN


def test_county_ndists_by_maz_roundtrip(tmp_path: Path) -> None:
    p = tmp_path / "c.parquet"
    pd.DataFrame({"maz": ["01", "09"], "n_oevk": [6, 8]}).to_parquet(p, index=False)
    d = county_ndists_by_maz(p)
    assert d["01"] == 6 and d["09"] == 8


def test_run_county_redist_sample_writes_ensemble(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = tmp_path
    proc = repo / "data" / "processed"
    proc.mkdir(parents=True)
    run_id = "r-samp"
    run = proc / "runs" / run_id
    run.mkdir(parents=True)
    pd.DataFrame({"maz": ["01"], "n_oevk": [2]}).to_parquet(
        run / "county_oevk_counts.parquet",
        index=False,
    )

    pq = proc / "precincts.parquet"
    gdf = gpd.GeoDataFrame(
        {
            DEFAULT_PRECINCT_ID_COLUMN: ["01-001-001", "01-001-002"],
            "maz": ["01", "01"],
            "voters": [100.0, 100.0],
            "geometry": [box(0, 0, 1, 1), box(1, 0, 2, 1)],
        },
        crs="EPSG:4326",
    )
    write_processed_geoparquet(gdf, pq)

    def fake_sample_plans(_prob, _graph, **_kwargs: object) -> PlanEnsemble:
        return PlanEnsemble.from_columns(
            ("01-001-001", "01-001-002"),
            ([1, 1], [1, 2], [2, 2], [2, 1]),
            draw_ids=(1, 2, 3, 4),
            metadata={"sampler": "redist_smc", "bundle_dir": str(tmp_path)},
        )

    monkeypatch.setattr(
        "hungary_ge.pipeline.county_sample.sample_plans",
        fake_sample_plans,
    )

    paths = ProcessedPaths(repo)
    run_county_redist_sample(
        precinct_parquet=pq,
        paths=paths,
        run_id=run_id,
        maz="01",
        ndists=2,
        pop_column="voters",
        adj_opts=AdjacencyBuildOptions(contiguity="queen"),
        n_draws=4,
        n_runs=1,
        seed=1,
        pop_tol=0.25,
        compactness=1.0,
        rscript_path=None,
        strict_county_connectivity=True,
    )

    ens_dir = paths.county_ensemble_dir(run_id, "01")
    parq = ens_dir / "ensemble_assignments.parquet"
    meta = parq.with_suffix(".meta.json")
    assert parq.is_file()
    assert meta.is_file()
    raw = json.loads(meta.read_text(encoding="utf-8"))
    assert raw["metadata"]["county_maz"] == "01"
    assert raw["metadata"]["county_ndists"] == 2
    assert raw["metadata"]["county_run_id"] == run_id
