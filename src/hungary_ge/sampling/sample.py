"""Plan sampling adapter (R ``redist`` / SMC / MCMC or Python implementation)."""

from __future__ import annotations

import tempfile
from dataclasses import replace
from pathlib import Path
from typing import Any

from geopandas import GeoDataFrame

from hungary_ge.constraints.constraint_spec import (
    ConstraintSpec,
    ElectorBalanceConstraint,
)
from hungary_ge.ensemble import PlanEnsemble
from hungary_ge.graph.adjacency_graph import AdjacencyGraph
from hungary_ge.problem import OevkProblem
from hungary_ge.sampling.redist_adapter import run_redist_pipeline
from hungary_ge.sampling.sampler_config import SamplerConfig

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

    Args:
        problem: OEVK problem specification.
        adjacency: Dual graph from :func:`hungary_ge.graph.build_adjacency`.
        n_draws: Number of plans per run (``nsims`` for ``redist_smc``).
        n_runs: Parallel SMC runs.
        seed: Optional RNG seed.
        **kwargs: Use ``backend="redist"`` with ``gdf=GeoDataFrame``, optional
            ``constraint_spec``, ``work_dir``, ``bundle_dir``, ``rscript_path``,
            ``pop_tol``, ``compactness``, ``redist_extras`` dict, or a
            pre-built :class:`SamplerConfig` as ``sampler_config``.

    Raises:
        NotImplementedError: Default Python sampler is not implemented.
        TypeError: Missing or wrong types for ``backend="redist"``.
    """
    backend = kwargs.get("backend", "python")
    if backend == "redist":
        gdf = kwargs.get("gdf")
        if not isinstance(gdf, GeoDataFrame):
            msg = (
                "sample_plans(..., backend='redist') requires keyword gdf: GeoDataFrame"
            )
            raise TypeError(msg)
        if not isinstance(adjacency, AdjacencyGraph):
            msg = "sample_plans(..., backend='redist') requires AdjacencyGraph as adjacency"
            raise TypeError(msg)

        work_raw = kwargs.get("work_dir")
        cfg_in = kwargs.get("sampler_config")
        if isinstance(cfg_in, SamplerConfig):
            base = cfg_in
        else:
            base = SamplerConfig(
                n_sims=n_draws,
                n_runs=n_runs,
                seed=seed,
                work_dir=Path("."),
                pop_tol=float(
                    kwargs.get(
                        "pop_tol",
                        ElectorBalanceConstraint().max_relative_deviation,
                    )
                ),
                compactness=float(kwargs.get("compactness", 1.0)),
                redist_extras=dict(kwargs.get("redist_extras") or {}),
            )

        if work_raw is not None:
            work_path = Path(work_raw).resolve()
        elif isinstance(cfg_in, SamplerConfig) and base.work_dir != Path("."):
            work_path = Path(base.work_dir).resolve()
        else:
            work_path = Path(tempfile.mkdtemp(prefix="hungary_ge_sample_"))
        work_path.mkdir(parents=True, exist_ok=True)

        bundle_raw = kwargs.get("bundle_dir")
        bundle_path = (
            Path(bundle_raw).resolve() if bundle_raw is not None else work_path
        )
        bundle_path.mkdir(parents=True, exist_ok=True)

        cfg = replace(
            base,
            n_sims=n_draws,
            n_runs=n_runs,
            seed=seed if seed is not None else base.seed,
            work_dir=bundle_path,
        )
        if kwargs.get("pop_tol") is not None:
            cfg = replace(cfg, pop_tol=float(kwargs["pop_tol"]))
        if kwargs.get("compactness") is not None:
            cfg = replace(cfg, compactness=float(kwargs["compactness"]))
        rex = kwargs.get("redist_extras")
        if rex is not None:
            if not isinstance(rex, dict):
                msg = "redist_extras must be a dict[str, object]"
                raise TypeError(msg)
            cfg = replace(cfg, redist_extras=dict(rex))

        cs = kwargs.get("constraint_spec")
        if cs is not None and not isinstance(cs, ConstraintSpec):
            msg = "constraint_spec must be a ConstraintSpec or None"
            raise TypeError(msg)

        rp = kwargs.get("rscript_path")
        return run_redist_pipeline(
            gdf,
            adjacency,
            problem,
            cfg,
            constraint_spec=cs if isinstance(cs, ConstraintSpec) else None,
            bundle_dir=bundle_path,
            rscript_path=Path(rp) if rp is not None else None,
        )

    if backend != "python":
        msg = f"unknown sample_plans backend {backend!r}"
        raise ValueError(msg)
    raise NotImplementedError(
        "sample_plans: Python backend not implemented; use backend='redist' or install R"
    )
