"""Per-draw district population balance summaries."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence

import numpy as np

from hungary_ge.diagnostics.report import PopulationSummaryBlock
from hungary_ge.ensemble.plan_ensemble import PlanEnsemble


def max_abs_relative_pop_deviation_one_draw(
    district_labels: Sequence[int],
    populations: Sequence[float],
    ndists: int,
) -> float:
    """Max over districts ``d`` of ``|sum(pop in d) - ideal| / ideal`` with ``ideal = sum(pop)/ndists``."""
    pops = np.asarray(populations, dtype=np.float64)
    if len(district_labels) != len(pops):
        msg = (
            f"district_labels length {len(district_labels)} != populations {len(pops)}"
        )
        raise ValueError(msg)
    total = float(pops.sum())
    if ndists < 1:
        raise ValueError("ndists must be >= 1")
    ideal = total / ndists
    if ideal <= 0.0:
        return 0.0

    totals: dict[int, float] = defaultdict(float)
    for d, w in zip(district_labels, pops, strict=True):
        totals[int(d)] += float(w)

    worst = 0.0
    for d in range(1, ndists + 1):
        p_d = totals.get(d, 0.0)
        worst = max(worst, abs(p_d - ideal) / ideal)
    return float(worst)


def per_draw_max_abs_rel_pop_deviation(
    ensemble: PlanEnsemble,
    populations: Sequence[float],
    ndists: int,
) -> np.ndarray:
    """Shape ``(n_draws,)`` — one value per simulated column."""
    n_d = ensemble.n_draws
    out = np.empty(n_d, dtype=np.float64)
    pops = list(populations)
    for j in range(n_d):
        col = [ensemble.assignments[i][j] for i in range(ensemble.n_units)]
        out[j] = max_abs_relative_pop_deviation_one_draw(col, pops, ndists)
    return out


def build_population_summary_block(
    ensemble: PlanEnsemble,
    populations: Sequence[float],
    ndists: int,
    pop_tol: float | None,
) -> PopulationSummaryBlock:
    """Aggregate population-balance diagnostics across draws."""
    pops = np.asarray(populations, dtype=np.float64)
    total = float(pops.sum())
    ideal = total / ndists if ndists > 0 else 0.0
    per = per_draw_max_abs_rel_pop_deviation(ensemble, populations, ndists)
    t_per = tuple(float(x) for x in per)
    mean = float(np.mean(per)) if len(per) else 0.0
    max_v = float(np.max(per)) if len(per) else 0.0
    p95 = float(np.percentile(per, 95)) if len(per) else 0.0

    exceeds: tuple[bool, ...] | None = None
    if pop_tol is not None:
        exceeds = tuple(bool(x > pop_tol) for x in per)

    return PopulationSummaryBlock(
        ideal_per_district=ideal,
        total_population=total,
        per_draw_max_abs_rel_deviation=t_per,
        draws_exceeding_pop_tol=exceeds,
        mean_of_max_abs_rel_deviation=mean,
        max_of_max_abs_rel_deviation=max_v,
        p95_of_max_abs_rel_deviation=p95,
    )
