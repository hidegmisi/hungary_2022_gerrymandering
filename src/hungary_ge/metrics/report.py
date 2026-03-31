"""Typed report for focal vs ensemble partisan metrics (Slice 9)."""

from __future__ import annotations

import json
import math
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import numpy as np

PARTISAN_COMPARISON_SCHEMA_V1 = "hungary_ge.metrics.comparison/v1"


def _json_sanitize(x: Any) -> Any:
    if isinstance(x, dict):
        return {k: _json_sanitize(v) for k, v in x.items()}
    if isinstance(x, list):
        return [_json_sanitize(v) for v in x]
    if isinstance(x, float) and not math.isfinite(x):
        return None
    return x


@dataclass(frozen=True)
class CoverageBlock:
    """How well focal / votes align to ensemble ``unit_ids``."""

    n_units: int
    n_units_with_positive_two_party_votes: int
    n_units_missing_vote_row: int
    n_units_missing_focal_district: int
    n_voting_units_missing_focal: int


@dataclass(frozen=True)
class PartisanMetricResult:
    """One scalar metric: focal value vs ensemble distribution."""

    name: str
    focal_value: float
    ensemble_mean: float
    ensemble_p05: float
    ensemble_p95: float
    percentile_rank: float
    """
    Empirical percentile of focal in the ensemble draw distribution:
    ``100 * mean(draw <= focal)`` (inclusive upper tail).
    """


@dataclass(frozen=True)
class PartisanComparisonReport:
    """Full comparison table for one election coding and ensemble."""

    schema_version: str = PARTISAN_COMPARISON_SCHEMA_V1
    party_label_a: str = ""
    party_label_b: str = ""
    metrics: dict[str, PartisanMetricResult] = field(default_factory=dict)
    coverage: CoverageBlock | None = None
    extra: dict[str, Any] = field(default_factory=dict)

    def to_json_dict(self) -> dict[str, Any]:
        return asdict(self)

    def write_json(self, path: str | Path) -> None:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        payload = _json_sanitize(self.to_json_dict())
        p.write_text(
            json.dumps(payload, indent=2, sort_keys=True, allow_nan=False) + "\n",
            encoding="utf-8",
        )


def percentile_rank_inclusive_upper(draws: list[float], focal: float) -> float:
    """Percent of draws with value ``<= focal`` (percentile if focal were a draw)."""
    if not draws:
        return float("nan")
    arr = sorted(draws)
    # inclusive: count draws <= focal
    le = sum(1 for x in arr if x <= focal)
    return 100.0 * le / len(arr)


def summarize_draws(draws: list[float]) -> tuple[float, float, float]:
    """Return (mean, p05, p95)."""
    if not draws:
        return float("nan"), float("nan"), float("nan")
    a = np.array(draws, dtype=np.float64)
    return float(np.mean(a)), float(np.percentile(a, 5)), float(np.percentile(a, 95))
