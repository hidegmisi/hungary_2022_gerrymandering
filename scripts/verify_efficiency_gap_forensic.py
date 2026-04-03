"""Forensic cross-check for efficiency-gap values in a run directory.

Recomputes county focal-vs-ensemble efficiency gap from stored artifacts and
compares against county partisan reports and national rollup aggregation.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from hungary_ge.config import (
    COUNTY_PARTISAN_REPORT_JSON,
    ENSEMBLE_ASSIGNMENTS_PARQUET,
    ProcessedPaths,
)
from hungary_ge.ensemble.persistence import load_plan_ensemble
from hungary_ge.metrics.compare import focal_vs_ensemble_metrics
from hungary_ge.metrics.party_coding import (
    default_partisan_party_coding_path,
    load_partisan_party_coding,
)


def _discover_counties(paths: ProcessedPaths, run_id: str) -> list[str]:
    root = paths.run_dir(run_id) / "counties"
    if not root.is_dir():
        raise FileNotFoundError(f"missing county root: {root}")
    out: list[str] = []
    for d in sorted(root.iterdir()):
        if d.is_dir() and d.name.isdigit():
            p = d / "reports" / COUNTY_PARTISAN_REPORT_JSON
            if p.is_file():
                out.append(d.name)
    if not out:
        raise ValueError(f"no counties with {COUNTY_PARTISAN_REPORT_JSON} under {root}")
    return out


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _fmt(x: float) -> str:
    return f"{x:+.12f}"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", default="main")
    parser.add_argument(
        "--counties",
        default="01,08,14,19",
        help="Comma-separated county codes to recompute; use 'all' to scan every county with reports.",
    )
    parser.add_argument("--strict", action="store_true", help="Fail on any mismatch > tolerance.")
    parser.add_argument("--tol", type=float, default=1e-12, help="Absolute tolerance for checks.")
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    paths = ProcessedPaths(repo_root)
    national_path = paths.national_report_path(args.run_id)
    national = _load_json(national_path)

    if args.counties.strip().lower() == "all":
        counties = _discover_counties(paths, args.run_id)
    else:
        counties = [c.strip() for c in args.counties.split(",") if c.strip()]

    votes = pd.read_parquet(paths.precinct_votes_parquet)
    focal = pd.read_parquet(paths.focal_oevk_assignments_parquet)
    coding = load_partisan_party_coding(default_partisan_party_coding_path())

    print(f"Run: {args.run_id}")
    print(f"Counties checked: {', '.join(counties)}")
    print("")
    print(
        "maz  county_focal      recomputed_focal  delta_focal      "
        "county_ens_mean   recomputed_ens    delta_ens"
    )

    county_failures = 0
    for maz in counties:
        report_path = paths.county_reports_dir(args.run_id, maz) / COUNTY_PARTISAN_REPORT_JSON
        report = _load_json(report_path)
        ens_path = paths.county_ensemble_dir(args.run_id, maz) / ENSEMBLE_ASSIGNMENTS_PARQUET
        ensemble = load_plan_ensemble(ens_path)

        recomputed = focal_vs_ensemble_metrics(
            focal=focal,
            ensemble=ensemble,
            votes=votes,
            party_coding=coding,
            strict_focal_for_voting_units=True,
        )

        county_eg = report["metrics"]["efficiency_gap"]
        cf = float(county_eg["focal_value"])
        ce = float(county_eg["ensemble_mean"])
        rf = float(recomputed.metrics["efficiency_gap"].focal_value)
        re = float(recomputed.metrics["efficiency_gap"].ensemble_mean)
        df = rf - cf
        de = re - ce
        ok = abs(df) <= args.tol and abs(de) <= args.tol
        if not ok:
            county_failures += 1
        print(
            f"{maz:>3}  {_fmt(cf)}  {_fmt(rf)}  {_fmt(df)}  "
            f"{_fmt(ce)}  {_fmt(re)}  {_fmt(de)}"
        )

    # Rollup cross-check from county JSON and national weights.
    by_county = {
        row["maz"]: row
        for row in national["partisan"]["metrics"]["efficiency_gap"]["by_county"]
    }
    num_f = 0.0
    den_f = 0.0
    num_e = 0.0
    den_e = 0.0
    for maz in by_county:
        w = float(by_county[maz]["weight"])
        fv = by_county[maz]["focal_value"]
        em = by_county[maz]["ensemble_mean"]
        if fv is not None:
            num_f += w * float(fv)
            den_f += w
        if em is not None:
            num_e += w * float(em)
            den_e += w
    rollup_f = num_f / den_f
    rollup_e = num_e / den_e
    nat_f = float(national["partisan"]["metrics"]["efficiency_gap"]["weighted_mean_focal"])
    nat_e = float(
        national["partisan"]["metrics"]["efficiency_gap"]["weighted_mean_ensemble_mean"]
    )
    rollup_df = rollup_f - nat_f
    rollup_de = rollup_e - nat_e
    rollup_ok = abs(rollup_df) <= args.tol and abs(rollup_de) <= args.tol

    print("")
    print("National rollup cross-check (from national by_county rows):")
    print(f"weighted_mean_focal      reported={_fmt(nat_f)} recomputed={_fmt(rollup_f)} delta={_fmt(rollup_df)}")
    print(
        f"weighted_mean_ensemble   reported={_fmt(nat_e)} recomputed={_fmt(rollup_e)} delta={_fmt(rollup_de)}"
    )

    total_failures = county_failures + (0 if rollup_ok else 1)
    if total_failures == 0:
        print("")
        print("Result: all forensic efficiency-gap checks passed.")
        return 0

    print("")
    print(f"Result: {total_failures} mismatch block(s) exceeded tolerance {args.tol:g}.")
    return 2 if args.strict else 0


if __name__ == "__main__":
    raise SystemExit(main())
