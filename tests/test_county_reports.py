"""County diagnostics and partisan reports (Slice E)."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from hungary_ge.config import (
    COUNTY_DIAGNOSTICS_JSON,
    COUNTY_PARTISAN_REPORT_JSON,
    ENSEMBLE_ASSIGNMENTS_PARQUET,
    ProcessedPaths,
)
from hungary_ge.ensemble.persistence import save_plan_ensemble
from hungary_ge.ensemble.plan_ensemble import PlanEnsemble
from hungary_ge.metrics.party_coding import default_partisan_party_coding_path
from hungary_ge.pipeline.county_reports import (
    populations_aligned_to_units,
    run_county_reports,
)
from hungary_ge.problem import DEFAULT_PRECINCT_ID_COLUMN


def test_populations_aligned_to_units() -> None:
    votes = pd.DataFrame(
        {
            DEFAULT_PRECINCT_ID_COLUMN: ["a", "b"],
            "voters": [10.0, 20.0],
        },
    )
    assert populations_aligned_to_units(("a", "b", "c"), votes, pop_column="voters") == [
        10.0,
        20.0,
        0.0,
    ]


def test_run_county_reports_writes_json(tmp_path: Path) -> None:
    repo = tmp_path
    proc = repo / "data" / "processed"
    proc.mkdir(parents=True)
    run_id = "r-rep"
    run = proc / "runs" / run_id
    run.mkdir(parents=True)
    pd.DataFrame({"maz": ["01"], "n_oevk": [2]}).to_parquet(
        run / "county_oevk_counts.parquet",
        index=False,
    )

    ensemble = PlanEnsemble.from_columns(
        ("01-001-001", "01-001-002"),
        ([1, 2], [2, 1]),
        draw_ids=(1, 2),
        metadata={"sampler": "test", "county_ndists": 2},
    )
    ens_dir = ProcessedPaths(repo).county_ensemble_dir(run_id, "01")
    ens_dir.mkdir(parents=True, exist_ok=True)
    save_plan_ensemble(ensemble, ens_dir / ENSEMBLE_ASSIGNMENTS_PARQUET)

    votes = pd.DataFrame(
        {
            DEFAULT_PRECINCT_ID_COLUMN: ["01-001-001", "01-001-002"],
            "voters": [100.0, 100.0],
            "votes_list_952": [60.0, 40.0],
            "votes_list_942": [25.0, 35.0],
            "votes_list_950": [10.0, 15.0],
            "votes_list_951": [5.0, 10.0],
        },
    )
    votes.to_parquet(proc / "precinct_votes.parquet", index=False)

    focal = pd.DataFrame(
        {
            DEFAULT_PRECINCT_ID_COLUMN: ["01-001-001", "01-001-002"],
            "oevk_id_full": ["0101", "0102"],
        },
    )
    focal.to_parquet(proc / "focal_oevk_assignments.parquet", index=False)

    paths = ProcessedPaths(repo)
    run_county_reports(
        paths=paths,
        run_id=run_id,
        maz="01",
        votes_parquet=proc / "precinct_votes.parquet",
        focal_parquet=proc / "focal_oevk_assignments.parquet",
        pop_column="voters",
        pop_tol=0.25,
        party_coding=None,
        party_coding_path=default_partisan_party_coding_path(),
        strict_focal_for_voting_units=True,
        include_smc_log_scan=False,
    )

    rep = paths.county_reports_dir(run_id, "01")
    dpath = rep / COUNTY_DIAGNOSTICS_JSON
    ppath = rep / COUNTY_PARTISAN_REPORT_JSON
    assert dpath.is_file()
    assert ppath.is_file()
    diag = json.loads(dpath.read_text(encoding="utf-8"))
    assert diag["extra"]["county_maz"] == "01"
    assert diag["n_units"] == 2
    part = json.loads(ppath.read_text(encoding="utf-8"))
    assert part["extra"]["county_maz"] == "01"
    assert "coverage" in part


def test_run_county_reports_raises_without_ensemble(tmp_path: Path) -> None:
    repo = tmp_path
    proc = repo / "data" / "processed"
    proc.mkdir(parents=True)
    run_id = "r-x"
    (proc / "runs" / run_id).mkdir(parents=True)
    pd.DataFrame({"maz": ["01"], "n_oevk": [1]}).to_parquet(
        proc / "runs" / run_id / "county_oevk_counts.parquet",
        index=False,
    )
    paths = ProcessedPaths(repo)
    with pytest.raises(FileNotFoundError, match="ensemble"):
        run_county_reports(
            paths=paths,
            run_id=run_id,
            maz="01",
            votes_parquet=proc / "v.parquet",
            focal_parquet=proc / "f.parquet",
            pop_column="voters",
            pop_tol=0.25,
            party_coding=None,
            party_coding_path=default_partisan_party_coding_path(),
            strict_focal_for_voting_units=True,
            include_smc_log_scan=False,
        )
