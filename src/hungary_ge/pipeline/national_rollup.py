"""Aggregate county-level reports into one national summary (Slice F)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from hungary_ge.config import (
    COUNTY_DIAGNOSTICS_JSON,
    COUNTY_PARTISAN_REPORT_JSON,
    ProcessedPaths,
)
from hungary_ge.pipeline.county_allocation import normalize_maz
from hungary_ge.pipeline.county_sample import county_ndists_by_maz

NATIONAL_REPORT_SCHEMA_V1 = "hungary_ge.national_report/v1"


def _safe_float(x: object) -> float | None:
    if x is None:
        return None
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    if v != v:  # NaN
        return None
    return v


def _relative_paths(run_root: Path, abs_paths: list[Path]) -> list[str]:
    out: list[str] = []
    rr = run_root.resolve()
    for p in abs_paths:
        try:
            out.append(str(p.resolve().relative_to(rr)))
        except ValueError:
            out.append(str(p))
    return out


def build_national_report_payload(
    paths: ProcessedPaths,
    run_id: str,
    *,
    allow_partial: bool = False,
) -> dict[str, Any]:
    """Assemble dict written to ``paths.national_report_path`` (no I/O).

    Base weights use enacted ``n_oevk`` per county. **Full run:** weights are
    ``n_oevk_m / national_total_ndists``. **Partial run** (``allow_partial`` and
    some counties missing report pairs): weights are renormalized over counties
    that supplied both JSON files so they sum to 1 over the contributing subset.
    """
    run_root = paths.run_dir(run_id).resolve()
    counts_pq = paths.county_oevk_counts_parquet(run_id)
    if not counts_pq.is_file():
        msg = f"missing {counts_pq}"
        raise FileNotFoundError(msg)
    ndists_by_maz = county_ndists_by_maz(counts_pq)
    df = pd.read_parquet(counts_pq)
    expected = sorted({normalize_maz(m) for m in df["maz"].tolist()})
    national_total = int(df["n_oevk"].sum())

    contributed: list[str] = []
    missing: list[str] = []
    diag_paths: list[Path] = []
    part_paths: list[Path] = []
    for maz in expected:
        dpath = paths.county_reports_dir(run_id, maz) / COUNTY_DIAGNOSTICS_JSON
        ppath = paths.county_reports_dir(run_id, maz) / COUNTY_PARTISAN_REPORT_JSON
        if dpath.is_file() and ppath.is_file():
            contributed.append(maz)
            diag_paths.append(dpath.resolve())
            part_paths.append(ppath.resolve())
        else:
            missing.append(maz)

    if missing and not allow_partial:
        msg = (
            "national rollup: missing county reports for "
            f"{missing!r}; pass allow_partial=True to renormalize over "
            f"contributing counties only ({len(contributed)}/{len(expected)})"
        )
        raise ValueError(msg)
    if not contributed:
        msg = "national rollup: no county has both diagnostics.json and partisan_report.json"
        raise ValueError(msg)

    raw_weights = {m: float(ndists_by_maz[m]) for m in contributed}
    w_sum = sum(raw_weights.values())
    if w_sum <= 0:
        msg = "national rollup: sum of n_oevk over contributing counties is zero"
        raise ValueError(msg)

    if missing:
        norm_weights = {m: raw_weights[m] / w_sum for m in contributed}
        weight_note = (
            "renormalized_over_contributing_counties_only; "
            f"missing_megye={sorted(missing)!r}"
        )
    else:
        norm_weights = {m: raw_weights[m] / float(national_total) for m in contributed}
        weight_note = "weights_are_n_oevk_over_national_sum_from_county_oevk_counts"

    diags: list[dict[str, Any]] = []
    parts: list[dict[str, Any]] = []
    for maz in contributed:
        dpath = paths.county_reports_dir(run_id, maz) / COUNTY_DIAGNOSTICS_JSON
        ppath = paths.county_reports_dir(run_id, maz) / COUNTY_PARTISAN_REPORT_JSON
        diags.append(json.loads(dpath.read_text(encoding="utf-8")))
        parts.append(json.loads(ppath.read_text(encoding="utf-8")))

    diag_summary: dict[str, Any] = {"by_county": []}
    num_pop = 0.0
    den_pop = 0.0
    num_uniq = 0.0
    den_uniq = 0.0

    for maz, d in zip(contributed, diags, strict=True):
        w = norm_weights[maz]
        pop = d.get("population") or {}
        ens = d.get("ensemble") or {}
        pm = _safe_float(pop.get("mean_of_max_abs_rel_deviation"))
        if pm is not None:
            num_pop += w * pm
            den_pop += w
        n_d = d.get("n_draws")
        nu = (ens or {}).get("n_unique_assignment_columns")
        if isinstance(n_d, int) and n_d > 0:
            if isinstance(nu, int):
                num_uniq += w * (nu / n_d)
                den_uniq += w
        row = {
            "maz": maz,
            "weight": w,
            "n_units": d.get("n_units"),
            "n_draws": d.get("n_draws"),
            "ndists": d.get("ndists"),
            "pop_mean_max_abs_rel_dev": pop.get("mean_of_max_abs_rel_deviation"),
            "ensemble_n_unique_draws": ens.get("n_unique_assignment_columns"),
            "ensemble_n_duplicate_draws": ens.get("n_duplicate_assignment_columns"),
        }
        diag_summary["by_county"].append(row)

    if den_pop > 0:
        diag_summary["weighted_mean_pop_mean_max_abs_rel_deviation"] = num_pop / den_pop
    if den_uniq > 0:
        diag_summary["weighted_mean_unique_draw_fraction"] = num_uniq / den_uniq

    party_a = next(
        (p.get("party_label_a") for p in parts if p.get("party_label_a")), ""
    )
    party_b = next(
        (p.get("party_label_b") for p in parts if p.get("party_label_b")), ""
    )
    metric_names: set[str] = set()
    for p in parts:
        mets = p.get("metrics") or {}
        if isinstance(mets, dict):
            metric_names.update(mets.keys())

    partisan_agg: dict[str, Any] = {
        "party_label_a": party_a,
        "party_label_b": party_b,
        "metrics": {},
    }
    for name in sorted(metric_names):
        by_c: list[dict[str, Any]] = []
        num_f = num_e = num_pr = 0.0
        den_f = den_e = den_pr = 0.0
        for maz, p in zip(contributed, parts, strict=True):
            mets = p.get("metrics") or {}
            block = mets.get(name) if isinstance(mets, dict) else None
            fv = em = pr = None
            if isinstance(block, dict):
                fv = _safe_float(block.get("focal_value"))
                em = _safe_float(block.get("ensemble_mean"))
                pr = _safe_float(block.get("percentile_rank"))
            w = norm_weights[maz]
            if fv is not None:
                num_f += w * fv
                den_f += w
            if em is not None:
                num_e += w * em
                den_e += w
            if pr is not None:
                num_pr += w * pr
                den_pr += w
            by_c.append(
                {
                    "maz": maz,
                    "weight": w,
                    "focal_value": fv,
                    "ensemble_mean": em,
                    "percentile_rank": pr,
                },
            )
        met_out: dict[str, Any] = {"by_county": by_c}
        if den_f > 0:
            met_out["weighted_mean_focal"] = num_f / den_f
        if den_e > 0:
            met_out["weighted_mean_ensemble_mean"] = num_e / den_e
        if den_pr > 0:
            met_out["weighted_mean_percentile_rank"] = num_pr / den_pr
        partisan_agg["metrics"][name] = met_out

    sources = [
        {
            "maz": m,
            "diagnostics": f"counties/{m}/reports/{COUNTY_DIAGNOSTICS_JSON}",
            "partisan": f"counties/{m}/reports/{COUNTY_PARTISAN_REPORT_JSON}",
        }
        for m in contributed
    ]

    return {
        "schema_version": NATIONAL_REPORT_SCHEMA_V1,
        "run_id": run_id,
        "weighting": {
            "rule": "district_count",
            "description": weight_note,
            "national_total_ndists_declared": national_total,
            "weights_by_maz": {m: norm_weights[m] for m in contributed},
        },
        "completeness": {
            "expected_counties": len(expected),
            "counties_with_pair_of_reports": len(contributed),
            "missing_counties": sorted(missing) if missing else [],
            "partial": bool(missing),
        },
        "diagnostics_summary": diag_summary,
        "partisan": partisan_agg,
        "sources": sources,
        "source_paths_resolved": {
            "run_dir": str(run_root),
            "diagnostics": _relative_paths(run_root, diag_paths),
            "partisan": _relative_paths(run_root, part_paths),
        },
    }


def write_national_report(
    paths: ProcessedPaths,
    run_id: str,
    *,
    allow_partial: bool = False,
) -> Path:
    """Write ``national_report.json`` under ``runs/<run_id>/``."""
    payload = build_national_report_payload(
        paths,
        run_id,
        allow_partial=allow_partial,
    )
    out = paths.national_report_path(run_id)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(
        json.dumps(payload, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    return out
