"""County-level diagnostics and partisan comparison reports (Slice E)."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pandas as pd

from hungary_ge.config import (
    COUNTY_DIAGNOSTICS_JSON,
    COUNTY_PARTISAN_REPORT_JSON,
    ENSEMBLE_ASSIGNMENTS_PARQUET,
    ProcessedPaths,
)
from hungary_ge.diagnostics import summarize_ensemble, write_diagnostics_json
from hungary_ge.ensemble.persistence import load_plan_ensemble
from hungary_ge.io.electoral_etl import load_focal_assignments, load_votes_table
from hungary_ge.metrics import partisan_metrics
from hungary_ge.metrics.party_coding import (
    PartisanPartyCoding,
    default_partisan_party_coding_path,
    load_partisan_party_coding,
)
from hungary_ge.pipeline.county_allocation import normalize_maz
from hungary_ge.pipeline.county_sample import county_ndists_by_maz
from hungary_ge.problem import DEFAULT_PRECINCT_ID_COLUMN


def populations_aligned_to_units(
    unit_ids: tuple[str, ...],
    votes: pd.DataFrame,
    *,
    pop_column: str,
    precinct_col: str = DEFAULT_PRECINCT_ID_COLUMN,
) -> list[float]:
    """Weights for diagnostics (0 if unit missing from ``votes``)."""
    if pop_column not in votes.columns:
        msg = f"votes table missing population/weight column {pop_column!r}"
        raise ValueError(msg)
    dedup = votes.drop_duplicates(subset=[precinct_col])
    lookup = dedup.set_index(dedup[precinct_col].astype(str))[pop_column]
    out: list[float] = []
    for pid in unit_ids:
        sp = str(pid)
        if sp not in lookup.index:
            out.append(0.0)
            continue
        v = lookup.loc[sp]
        out.append(float(v) if pd.notna(v) else 0.0)
    return out


def run_county_reports(
    *,
    paths: ProcessedPaths,
    run_id: str,
    maz: str,
    votes_parquet: Path,
    focal_parquet: Path,
    pop_column: str,
    pop_tol: float | None,
    party_coding: PartisanPartyCoding | None,
    party_coding_path: Path | None,
    strict_focal_for_voting_units: bool,
    include_smc_log_scan: bool = True,
) -> None:
    """Write ``diagnostics.json`` and ``partisan_report.json`` under county ``reports/``."""
    maz_n = normalize_maz(maz)
    ens_path = paths.county_ensemble_dir(run_id, maz_n) / ENSEMBLE_ASSIGNMENTS_PARQUET
    if not ens_path.is_file():
        msg = f"ensemble assignments not found: {ens_path}"
        raise FileNotFoundError(msg)

    ensemble = load_plan_ensemble(ens_path)
    uid = ensemble.unit_ids
    uid_set = frozenset(str(x) for x in uid)

    votes_all = load_votes_table(votes_parquet)
    votes = votes_all[
        votes_all[DEFAULT_PRECINCT_ID_COLUMN].astype(str).isin(uid_set)
    ].copy()

    focal_all = load_focal_assignments(focal_parquet)
    focal = focal_all[
        focal_all[DEFAULT_PRECINCT_ID_COLUMN].astype(str).isin(uid_set)
    ].copy()

    meta_nd = ensemble.metadata.get("county_ndists")
    if meta_nd is not None:
        ndists = int(meta_nd)
    else:
        nd_map = county_ndists_by_maz(paths.county_oevk_counts_parquet(run_id))
        ndists = nd_map[maz_n]

    pops = populations_aligned_to_units(uid, votes, pop_column=pop_column)
    county_lbls = tuple(maz_n for _ in uid)

    diag = summarize_ensemble(
        ensemble,
        populations=pops,
        ndists=ndists,
        pop_tol=pop_tol,
        county_ids=county_lbls,
        include_smc_log_scan=include_smc_log_scan,
    )
    diag_out = replace(
        diag,
        extra={
            **diag.extra,
            "county_maz": maz_n,
            "county_run_id": run_id,
            "pop_column": pop_column,
        },
    )

    coding = party_coding
    if coding is None:
        pcp = party_coding_path or default_partisan_party_coding_path()
        coding = load_partisan_party_coding(pcp)

    partisan = partisan_metrics(
        ensemble,
        votes,
        focal=focal,
        party_coding=coding,
        strict_focal_for_voting_units=strict_focal_for_voting_units,
    )
    partisan_out = replace(
        partisan,
        extra={
            **partisan.extra,
            "county_maz": maz_n,
            "county_run_id": run_id,
        },
    )

    rep_dir = paths.county_reports_dir(run_id, maz_n)
    rep_dir.mkdir(parents=True, exist_ok=True)
    write_diagnostics_json(rep_dir / COUNTY_DIAGNOSTICS_JSON, diag_out)
    partisan_out.write_json(rep_dir / COUNTY_PARTISAN_REPORT_JSON)
