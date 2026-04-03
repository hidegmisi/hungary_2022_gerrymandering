"""Partisan and representational outcome metrics (post-ensemble comparison).

Prefer efficiency gap, seats–votes summaries, and symmetry-style measures over
raw compactness as evidence; see docs/methodology.md and docs/partisan-metrics.md.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from hungary_ge.ensemble.plan_ensemble import PlanEnsemble
from hungary_ge.metrics.balance import apply_two_bloc_vote_balance
from hungary_ge.metrics.compare import focal_vs_ensemble_metrics
from hungary_ge.metrics.party_coding import (
    PARTISAN_PARTY_CODING_SCHEMA_V1,
    PartisanPartyCoding,
    default_partisan_party_coding_path,
    list_map_vote_columns,
    load_partisan_party_coding,
    partisan_party_coding_from_dict,
)
from hungary_ge.metrics.policy import (
    DEFAULT_METRIC_COMPUTATION_POLICY,
    BalancePolicy,
    MetricComputationPolicy,
    NumericalSafetyPolicy,
)
from hungary_ge.metrics.report import (
    PARTISAN_COMPARISON_SCHEMA_V1,
    CoverageBlock,
    PartisanComparisonReport,
    PartisanMetricResult,
)

VotesTable = pd.DataFrame


def partisan_metrics(
    ensemble: PlanEnsemble,
    votes: VotesTable,
    *,
    focal: pd.DataFrame,
    party_coding: PartisanPartyCoding | None = None,
    party_coding_path: str | Path | None = None,
    **kwargs: Any,
) -> PartisanComparisonReport:
    """Compare **focal** and **ensemble** on two-bloc partisan metrics.

    Thin wrapper around :func:`focal_vs_ensemble_metrics` with argument order
    ``(ensemble, votes)`` for pipeline ergonomics.

    If ``party_coding`` is omitted, loads JSON from ``party_coding_path`` or the
    packaged example under ``metrics/data/partisan_party_coding.json``.
    """
    coding = party_coding
    if coding is None:
        pth = party_coding_path or default_partisan_party_coding_path()
        coding = load_partisan_party_coding(pth)
    return focal_vs_ensemble_metrics(
        focal, ensemble, votes, party_coding=coding, **kwargs
    )


__all__ = [
    "apply_two_bloc_vote_balance",
    "DEFAULT_METRIC_COMPUTATION_POLICY",
    "BalancePolicy",
    "MetricComputationPolicy",
    "NumericalSafetyPolicy",
    "PARTISAN_COMPARISON_SCHEMA_V1",
    "PARTISAN_PARTY_CODING_SCHEMA_V1",
    "CoverageBlock",
    "PartisanComparisonReport",
    "PartisanMetricResult",
    "PartisanPartyCoding",
    "default_partisan_party_coding_path",
    "focal_vs_ensemble_metrics",
    "list_map_vote_columns",
    "load_partisan_party_coding",
    "partisan_metrics",
    "partisan_party_coding_from_dict",
]
