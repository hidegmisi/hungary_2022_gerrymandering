"""National rollup from county reports (Slice F)."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from hungary_ge.config import (
    COUNTY_DIAGNOSTICS_JSON,
    COUNTY_PARTISAN_REPORT_JSON,
    NATIONAL_REPORT_JSON,
    ProcessedPaths,
)
from hungary_ge.pipeline.national_rollup import (
    NATIONAL_REPORT_SCHEMA_V1,
    build_national_report_payload,
    write_national_report,
)


def _minimal_diagnostics() -> dict:
    return {
        "n_units": 2,
        "n_draws": 4,
        "ndists": 2,
        "population": {"mean_of_max_abs_rel_deviation": 0.08},
        "ensemble": {
            "n_unique_assignment_columns": 3,
            "n_duplicate_assignment_columns": 1,
        },
        "extra": {},
    }


def _minimal_partisan() -> dict:
    return {
        "party_label_a": "A",
        "party_label_b": "B",
        "metrics": {
            "efficiency_gap": {
                "focal_value": 0.1,
                "ensemble_mean": 0.0,
                "percentile_rank": 70.0,
            },
            "vote_share_a": {
                "focal_value": 0.55,
                "ensemble_mean": 0.52,
                "percentile_rank": 40.0,
            },
        },
        "coverage": {},
        "extra": {},
    }


def _write_county_reports(
    base: Path,
    run_id: str,
    maz: str,
    *,
    partisan_payload: dict | None = None,
) -> None:
    rd = base / "runs" / run_id / "counties" / maz / "reports"
    rd.mkdir(parents=True, exist_ok=True)
    (rd / COUNTY_DIAGNOSTICS_JSON).write_text(
        json.dumps(_minimal_diagnostics(), indent=2) + "\n",
        encoding="utf-8",
    )
    (rd / COUNTY_PARTISAN_REPORT_JSON).write_text(
        json.dumps(partisan_payload or _minimal_partisan(), indent=2) + "\n",
        encoding="utf-8",
    )


def test_write_national_report_full(tmp_path: Path) -> None:
    repo = tmp_path
    proc = repo / "data" / "processed"
    run_id = "nr1"
    run = proc / "runs" / run_id
    run.mkdir(parents=True)
    pd.DataFrame({"maz": ["01", "02"], "n_oevk": [6, 5]}).to_parquet(
        run / "county_oevk_counts.parquet",
        index=False,
    )
    p01 = _minimal_partisan()
    p01["metrics"]["efficiency_gap"] = {
        "focal_value": 0.1,
        "ensemble_mean": 0.0,
        "percentile_rank": 70.0,
    }
    p02 = _minimal_partisan()
    p02["metrics"]["efficiency_gap"] = {
        "focal_value": -0.2,
        "ensemble_mean": -0.1,
        "percentile_rank": 20.0,
    }
    _write_county_reports(proc, run_id, "01", partisan_payload=p01)
    _write_county_reports(proc, run_id, "02", partisan_payload=p02)

    paths = ProcessedPaths(repo)
    out = write_national_report(paths, run_id, allow_partial=False)
    assert out.name == NATIONAL_REPORT_JSON
    assert out.is_file()
    pay = json.loads(out.read_text(encoding="utf-8"))
    assert pay["schema_version"] == NATIONAL_REPORT_SCHEMA_V1
    assert pay["completeness"]["partial"] is False
    assert pay["completeness"]["missing_counties"] == []
    w = pay["weighting"]["weights_by_maz"]
    assert abs(w["01"] - 6 / 11) < 1e-9
    assert abs(w["02"] - 5 / 11) < 1e-9
    assert "vote_share_a" in pay["partisan"]["metrics"]
    eg = pay["partisan"]["metrics"]["efficiency_gap"]
    assert eg["weighted_mean_focal"] == pytest.approx(((6 * 0.1) + (5 * -0.2)) / 11.0)
    assert eg["weighted_mean_ensemble_mean"] == pytest.approx(
        ((6 * 0.0) + (5 * -0.1)) / 11.0
    )
    assert eg["weighted_mean_percentile_rank"] == pytest.approx(
        ((6 * 70.0) + (5 * 20.0)) / 11.0
    )


def test_national_rollup_partial_reweights(tmp_path: Path) -> None:
    repo = tmp_path
    proc = repo / "data" / "processed"
    run_id = "nr2"
    run = proc / "runs" / run_id
    run.mkdir(parents=True)
    pd.DataFrame({"maz": ["01", "02"], "n_oevk": [6, 4]}).to_parquet(
        run / "county_oevk_counts.parquet",
        index=False,
    )
    _write_county_reports(proc, run_id, "01")

    paths = ProcessedPaths(repo)
    pay = build_national_report_payload(paths, run_id, allow_partial=True)
    assert pay["completeness"]["partial"] is True
    assert pay["completeness"]["missing_counties"] == ["02"]
    w = pay["weighting"]["weights_by_maz"]
    assert len(w) == 1
    assert w["01"] == pytest.approx(1.0)
    eg = pay["partisan"]["metrics"]["efficiency_gap"]
    assert eg["weighted_mean_focal"] == pytest.approx(0.1)
    assert eg["weighted_mean_ensemble_mean"] == pytest.approx(0.0)
    assert eg["weighted_mean_percentile_rank"] == pytest.approx(70.0)


def test_national_rollup_strict_missing_raises(tmp_path: Path) -> None:
    repo = tmp_path
    proc = repo / "data" / "processed"
    run_id = "nr3"
    run = proc / "runs" / run_id
    run.mkdir(parents=True)
    pd.DataFrame({"maz": ["01", "02"], "n_oevk": [3, 3]}).to_parquet(
        run / "county_oevk_counts.parquet",
        index=False,
    )
    _write_county_reports(proc, run_id, "01")
    paths = ProcessedPaths(repo)
    with pytest.raises(ValueError, match="missing county reports"):
        build_national_report_payload(paths, run_id, allow_partial=False)
