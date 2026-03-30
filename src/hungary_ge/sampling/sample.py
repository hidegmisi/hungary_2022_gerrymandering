"""Plan sampling adapter (R ``redist`` / SMC / MCMC or Python implementation)."""

from __future__ import annotations

from typing import Any

from hungary_ge.ensemble import PlanEnsemble
from hungary_ge.problem import OevkProblem

GraphT = Any


def sample_plans(
    problem: OevkProblem,
    adjacency: GraphT,
    *,
    n_draws: int = 5000,
    n_runs: int = 1,
    seed: int | None = None,
    **kwargs: object,
) -> PlanEnsemble:
    """Draw an ensemble of redistricting plans under encoded constraints.

    Intended to delegate to **redist** (``redist_smc``, ``redist_flip``, etc.)
    or a Python sampler once Hungarian OEVK rules are implemented.

    Args:
        problem: OEVK problem specification.
        adjacency: Dual graph from :func:`hungary_ge.graph.build_adjacency`.
        n_draws: Number of plans per run.
        n_runs: Parallel SMC runs (for simulation SE / diagnostics).
        seed: Optional RNG seed.
        **kwargs: Sampler-specific options (compactness, constraints, …).
    """
    raise NotImplementedError(
        "sample_plans: connect R redist or implement Python sampler"
    )
