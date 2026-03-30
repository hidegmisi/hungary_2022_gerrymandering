"""Partisan and representational outcome metrics (post-ensemble comparison).

Prefer efficiency gap, seats–votes summaries, and symmetry-style measures over
raw compactness as evidence; see docs/methodology.md.
"""

from __future__ import annotations

from typing import Any

from hungary_ge.ensemble import PlanEnsemble

VotesTable = Any


def partisan_metrics(
    ensemble: PlanEnsemble,
    votes: VotesTable,
    *,
    focal_assignments: dict[str, int] | None = None,
) -> dict[str, float]:
    """Compare ensemble and optional focal plan on partisan outcome measures."""
    raise NotImplementedError("partisan_metrics: implement once vote schema is fixed")


__all__ = ["partisan_metrics"]
