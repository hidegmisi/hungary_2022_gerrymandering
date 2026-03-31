"""Slice 8 ensemble diagnostics."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pytest

from hungary_ge.diagnostics import (
    gelman_rubin_rhat_univariate,
    summarize_ensemble,
    write_diagnostics_json,
)
from hungary_ge.diagnostics.population import max_abs_relative_pop_deviation_one_draw
from hungary_ge.ensemble import PlanEnsemble, save_plan_ensemble


def test_max_abs_relative_pop_deviation_balanced() -> None:
    pops = [10.0, 10.0, 30.0, 30.0]
    labels = [1, 1, 2, 2]
    # ideal 40; pops 20 and 60 -> rel dev 0.5 each
    assert max_abs_relative_pop_deviation_one_draw(labels, pops, 2) == pytest.approx(
        0.5
    )


def test_max_abs_relative_pop_deviation_perfect() -> None:
    pops = [25.0, 25.0, 25.0, 25.0]
    labels = [1, 1, 2, 2]
    assert max_abs_relative_pop_deviation_one_draw(labels, pops, 2) == pytest.approx(
        0.0
    )


def test_summarize_ensemble_county_splits() -> None:
    uid = ("a", "b", "c", "d")
    e = PlanEnsemble.from_columns(uid, [[1, 1, 1, 1], [1, 2, 1, 2]])
    counties = ["X", "X", "Y", "Y"]
    rep = summarize_ensemble(
        e,
        populations=[1.0, 1.0, 1.0, 1.0],
        ndists=2,
        county_ids=counties,
        include_smc_log_scan=False,
    )
    assert rep.county_splits is not None
    assert rep.county_splits.per_draw_n_split_counties == (0, 2)


def test_summarize_ensemble_pop_tol_flags() -> None:
    uid = ("a", "b", "c", "d")
    e = PlanEnsemble.from_columns(uid, [[1, 1, 2, 2], [1, 1, 2, 2]])
    pops = [10.0, 10.0, 30.0, 30.0]
    rep = summarize_ensemble(
        e,
        populations=pops,
        ndists=2,
        pop_tol=0.4,
        include_smc_log_scan=False,
    )
    assert rep.population is not None
    assert rep.population.draws_exceeding_pop_tol is not None
    assert rep.population.draws_exceeding_pop_tol[0] is True


def test_summarize_ensemble_duplicate_columns() -> None:
    uid = ("a", "b")
    e = PlanEnsemble.from_columns(uid, [[1, 2], [1, 2], [1, 1]])
    rep = summarize_ensemble(
        e,
        populations=[1.0, 1.0],
        ndists=2,
        include_smc_log_scan=False,
    )
    assert rep.ensemble is not None
    assert rep.ensemble.n_unique_assignment_columns == 2
    assert rep.ensemble.n_duplicate_assignment_columns == 1


def test_gelman_rubin_identical_chains() -> None:
    r = gelman_rubin_rhat_univariate(
        [np.array([1.0, 1.1, 0.9]), np.array([0.95, 1.05, 1.0])]
    )
    assert r == pytest.approx(1.0, abs=0.05)


def test_gelman_rubin_separated_chains() -> None:
    r = gelman_rubin_rhat_univariate([np.array([0.01, 0.02]), np.array([2.0, 2.1])])
    assert r > 1.2


def test_summarize_ensemble_rhat_multichain() -> None:
    uid = ("a", "b", "c", "d")
    # Three draws per chain so within-chain variance of scalar summaries is positive.
    cols: list[list[int]] = [
        [1, 1, 2, 2],
        [1, 1, 2, 2],
        [1, 2, 1, 2],
        [1, 1, 1, 1],
        [1, 1, 1, 1],
        [1, 1, 2, 2],
    ]
    e = PlanEnsemble.from_columns(
        uid,
        cols,
        chain_or_run=(1, 1, 1, 2, 2, 2),
    )
    pops = [10.0, 10.0, 30.0, 30.0]
    rep = summarize_ensemble(
        e,
        populations=pops,
        ndists=2,
        county_ids=["C", "C", "C", "C"],
        include_smc_log_scan=False,
    )
    assert rep.chains is not None
    assert rep.chains.n_chains == 2
    assert rep.chains.r_hat_max_abs_rel_pop_deviation is not None
    assert rep.chains.r_hat_max_abs_rel_pop_deviation > 1.0
    assert rep.chains.r_hat_n_split_counties is not None
    assert rep.chains.r_hat_n_split_counties > 1.0


def test_write_diagnostics_json_roundtrip(tmp_path: Path) -> None:
    uid = ("a", "b")
    e = PlanEnsemble.from_columns(uid, [[1, 2], [1, 2]])
    rep = summarize_ensemble(
        e,
        populations=[1.0, 1.0],
        ndists=2,
        include_smc_log_scan=False,
    )
    out = tmp_path / "d.json"
    write_diagnostics_json(out, rep)
    data = json.loads(out.read_text(encoding="utf-8"))
    assert data["schema_version"] == "hungary_ge.diagnostics/v1"
    assert data["n_draws"] == 2
    assert "population" in data


def test_save_plan_ensemble_writes_diagnostics(tmp_path: Path) -> None:
    uid = ("a", "b")
    e = PlanEnsemble.from_columns(uid, [[1, 2], [1, 2]])
    rep = summarize_ensemble(
        e,
        populations=[1.0, 3.0],
        ndists=2,
        include_smc_log_scan=False,
    )
    p = tmp_path / "assignments.parquet"
    save_plan_ensemble(e, p, write_sha256=False, diagnostics_report=rep)
    diag = tmp_path / "assignments_diagnostics.json"
    assert diag.is_file()
    meta = json.loads(p.with_suffix(".meta.json").read_text(encoding="utf-8"))
    assert meta.get("diagnostics_file") == "assignments_diagnostics.json"


def test_smc_log_scrape_ess(tmp_path: Path) -> None:
    err = tmp_path / "e.log"
    err.write_text(
        "Some preamble\nEffective sample size: 42%\nmore\n", encoding="utf-8"
    )
    uid = ("a", "b")
    e = PlanEnsemble.from_columns(
        uid,
        [[1, 2], [1, 2]],
        metadata={"redist_stderr_path": str(err)},
    )
    rep = summarize_ensemble(
        e,
        populations=[1.0, 1.0],
        ndists=2,
        include_smc_log_scan=True,
    )
    assert rep.smc_log is not None
    assert rep.smc_log.parse_status == "scanned"
    assert rep.smc_log.ess_line_hits >= 1
