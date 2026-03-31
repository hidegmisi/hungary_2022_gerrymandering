"""Focal plan vs simulated ensemble on partisan metrics (Slice 9)."""

from __future__ import annotations

from collections.abc import Hashable, Sequence

import numpy as np
import pandas as pd

from hungary_ge.ensemble.plan_ensemble import PlanEnsemble
from hungary_ge.metrics.party_coding import PartisanPartyCoding
from hungary_ge.metrics.report import (
    PARTISAN_COMPARISON_SCHEMA_V1,
    CoverageBlock,
    PartisanComparisonReport,
    PartisanMetricResult,
    percentile_rank_inclusive_upper,
    summarize_draws,
)
from hungary_ge.metrics.two_party import (
    district_two_party_totals,
    efficiency_gap_two_party,
    mean_median_district_a_share,
    national_two_party_shares,
    seat_share_a_rate,
)
from hungary_ge.problem import DEFAULT_PRECINCT_ID_COLUMN


def _vote_columns_present(votes: pd.DataFrame, coding: PartisanPartyCoding) -> None:
    missing = [c for c in coding.all_vote_columns if c not in votes.columns]
    if missing:
        msg = f"votes table missing columns {missing}"
        raise ValueError(msg)


def _aligned_vote_arrays(
    unit_ids: tuple[str, ...],
    votes: pd.DataFrame,
    coding: PartisanPartyCoding,
    *,
    precinct_col: str = DEFAULT_PRECINCT_ID_COLUMN,
) -> tuple[np.ndarray, np.ndarray]:
    """Rows aligned to ``unit_ids``; NaN/ missing cells -> 0."""
    sub = votes.set_index(precinct_col, drop=False)
    na = len(unit_ids)
    va = np.zeros(na, dtype=np.float64)
    vb = np.zeros(na, dtype=np.float64)
    for i, pid in enumerate(unit_ids):
        if str(pid) not in sub.index.astype(str):
            continue
        row = sub.loc[pid]
        if isinstance(row, pd.DataFrame):
            row = row.iloc[0]
        a_sum = 0.0
        for c in coding.party_a_columns:
            v = row.get(c)
            if v is not None and pd.notna(v):
                a_sum += float(v)
        b_sum = 0.0
        for c in coding.party_b_columns:
            v = row.get(c)
            if v is not None and pd.notna(v):
                b_sum += float(v)
        va[i] = a_sum
        vb[i] = b_sum
    return va, vb


def _focal_labels_for_units(
    unit_ids: tuple[str, ...],
    focal: pd.DataFrame,
    *,
    precinct_col: str = DEFAULT_PRECINCT_ID_COLUMN,
    focal_district_col: str = "oevk_id_full",
) -> tuple[list[Hashable | None], int]:
    """Parallel to ``unit_ids``; return labels and count of missing focal rows."""
    if precinct_col not in focal.columns:
        msg = f"focal missing {precinct_col!r}"
        raise ValueError(msg)
    if focal_district_col not in focal.columns:
        msg = f"focal missing {focal_district_col!r}"
        raise ValueError(msg)
    fsub = focal.drop_duplicates(subset=[precinct_col]).copy()
    fsub["_i"] = fsub[precinct_col].astype(str)
    m = fsub.set_index("_i", drop=True)[focal_district_col]
    labels: list[Hashable | None] = []
    n_miss = 0
    for pid in unit_ids:
        sp = str(pid)
        if sp not in m.index:
            labels.append(None)
            n_miss += 1
            continue
        val = m.loc[sp]
        if pd.isna(val):
            labels.append(None)
            n_miss += 1
        else:
            labels.append(val)
    return labels, n_miss


def metrics_for_assignment(
    district_per_unit: Sequence[Hashable],
    votes_a: np.ndarray,
    votes_b: np.ndarray,
) -> dict[str, float]:
    """Scalar metrics for one partition (one draw or focal)."""
    tot = district_two_party_totals(district_per_unit, votes_a, votes_b)
    va, vb, t_nat = national_two_party_shares(tot)
    vshare = (va / t_nat) if t_nat > 0 else 0.0
    sshare = seat_share_a_rate(tot)
    eg, _wa, _wb, _t = efficiency_gap_two_party(tot)
    mmd = mean_median_district_a_share(tot)
    out = {
        "vote_share_a": float(vshare),
        "seat_share_a": float(sshare),
        "efficiency_gap": float(eg),
    }
    if mmd is not None:
        out["mean_median_a_share_diff"] = float(mmd)
    else:
        out["mean_median_a_share_diff"] = float("nan")
    return out


def focal_vs_ensemble_metrics(
    focal: pd.DataFrame,
    ensemble: PlanEnsemble,
    votes: pd.DataFrame,
    *,
    party_coding: PartisanPartyCoding,
    strict_focal_for_voting_units: bool = True,
    precinct_col: str = DEFAULT_PRECINCT_ID_COLUMN,
    focal_district_col: str = "oevk_id_full",
) -> PartisanComparisonReport:
    """Compare enacted (focal) districting to ensemble draws on two-bloc metrics.

    Args:
        focal: DataFrame with ``precinct_col`` and ``focal_district_col`` (e.g. ``oevk_id_full``).
        ensemble: Simulated assignments aligned to the same ``unit_ids`` order as votes join.
        votes: Precinct vote table (Slice 4 schema).
        party_coding: Which columns sum into bloc A vs B.
        strict_focal_for_voting_units: If True, raise when any unit with positive
            two-party votes lacks a focal district label.
    """
    _vote_columns_present(votes, party_coding)
    uid = ensemble.unit_ids
    va, vb = _aligned_vote_arrays(uid, votes, party_coding, precinct_col=precinct_col)
    focal_lbl, n_miss_focal = _focal_labels_for_units(
        uid, focal, precinct_col=precinct_col, focal_district_col=focal_district_col
    )

    vote_idx = votes[precinct_col].astype(str)
    in_votes = set(vote_idx.tolist())

    n_vote_pos = 0
    n_vote_miss_focal = 0
    n_missing_vote_row = 0
    for i, pid in enumerate(uid):
        sp = str(pid)
        t = va[i] + vb[i]
        if sp not in in_votes:
            n_missing_vote_row += 1
        if t > 0.0:
            n_vote_pos += 1
            if focal_lbl[i] is None:
                n_vote_miss_focal += 1

    if strict_focal_for_voting_units and n_vote_miss_focal > 0:
        msg = (
            f"{n_vote_miss_focal} units have positive two-party votes but missing "
            f"focal {focal_district_col}; fix focal table or set strict_focal_for_voting_units=False"
        )
        raise ValueError(msg)

    draw_metrics: dict[str, list[float]] = {
        "vote_share_a": [],
        "seat_share_a": [],
        "efficiency_gap": [],
        "mean_median_a_share_diff": [],
    }
    for j in range(ensemble.n_draws):
        dist_col = [ensemble.assignments[i][j] for i in range(ensemble.n_units)]
        m = metrics_for_assignment(dist_col, va, vb)
        for k in draw_metrics:
            draw_metrics[k].append(m[k])

    focal_idx = [i for i in range(ensemble.n_units) if focal_lbl[i] is not None]
    focal_dist = [focal_lbl[i] for i in focal_idx]
    focal_va = va[focal_idx]
    focal_vb = vb[focal_idx]
    focal_m = metrics_for_assignment(focal_dist, focal_va, focal_vb)

    results: dict[str, PartisanMetricResult] = {}
    for name, draws in draw_metrics.items():
        fv = focal_m[name]
        if not np.isfinite(fv) and name != "mean_median_a_share_diff":
            fv = 0.0
        mean, p05, p95 = summarize_draws(draws)
        pr = percentile_rank_inclusive_upper(draws, float(fv))
        results[name] = PartisanMetricResult(
            name=name,
            focal_value=float(fv),
            ensemble_mean=mean,
            ensemble_p05=p05,
            ensemble_p95=p95,
            percentile_rank=pr,
        )

    cov = CoverageBlock(
        n_units=len(uid),
        n_units_with_positive_two_party_votes=n_vote_pos,
        n_units_missing_vote_row=n_missing_vote_row,
        n_units_missing_focal_district=n_miss_focal,
        n_voting_units_missing_focal=n_vote_miss_focal,
    )

    return PartisanComparisonReport(
        schema_version=PARTISAN_COMPARISON_SCHEMA_V1,
        party_label_a=party_coding.label_a,
        party_label_b=party_coding.label_b,
        metrics=results,
        coverage=cov,
        extra={
            "precinct_id_column": precinct_col,
            "focal_district_column": focal_district_col,
            "n_units_in_focal_aggregate": len(focal_idx),
        },
    )
