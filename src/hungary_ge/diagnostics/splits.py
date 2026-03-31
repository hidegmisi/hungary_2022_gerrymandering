"""County split counts per simulated draw."""

from __future__ import annotations

from collections.abc import Sequence

import numpy as np

from hungary_ge.diagnostics.report import CountySplitsBlock
from hungary_ge.ensemble.plan_ensemble import PlanEnsemble


def n_split_counties_one_draw(
    district_labels: Sequence[int],
    county_ids: Sequence[str],
) -> int:
    """Count counties that map to **more than one** distinct district label."""
    by_county: dict[str, set[int]] = {}
    for cty, d in zip(county_ids, district_labels, strict=True):
        if cty not in by_county:
            by_county[cty] = set()
        by_county[cty].add(int(d))
    return sum(1 for dists in by_county.values() if len(dists) > 1)


def per_draw_n_split_counties(
    ensemble: PlanEnsemble,
    county_ids: Sequence[str],
) -> np.ndarray:
    """Shape ``(n_draws,)``."""
    if len(county_ids) != ensemble.n_units:
        msg = f"county_ids length {len(county_ids)} != n_units {ensemble.n_units}"
        raise ValueError(msg)
    n_d = ensemble.n_draws
    out = np.empty(n_d, dtype=np.int64)
    for j in range(n_d):
        col = [ensemble.assignments[i][j] for i in range(ensemble.n_units)]
        out[j] = n_split_counties_one_draw(col, county_ids)
    return out


def build_county_splits_block(
    ensemble: PlanEnsemble,
    county_ids: Sequence[str],
) -> CountySplitsBlock:
    per = per_draw_n_split_counties(ensemble, county_ids)
    t = tuple(int(x) for x in per)
    n_cty = len(set(county_ids))
    mean = float(np.mean(per)) if len(per) else 0.0
    max_v = int(np.max(per)) if len(per) else 0
    return CountySplitsBlock(
        n_counties_in_frame=n_cty,
        per_draw_n_split_counties=t,
        mean_n_split_counties=mean,
        max_n_split_counties=max_v,
    )
