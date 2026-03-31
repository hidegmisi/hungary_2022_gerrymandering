"""Frozen sampler configuration and subprocess result (Slice 6)."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from hungary_ge.constraints.constraint_spec import ElectorBalanceConstraint
from hungary_ge.ensemble.plan_ensemble import PlanEnsemble

_DEFAULT_ELECTOR_BALANCE = ElectorBalanceConstraint()


@dataclass(frozen=True)
class SamplerConfig:
    """Parameters for one sampling call (R `redist` or future backends).

    Attributes:
        n_sims: Draws per run passed to the backend (e.g. ``redist_smc(..., nsims=)``).
        n_runs: Independent SMC runs (``runs=`` in `redist`).
        seed: RNG seed (optional).
        work_dir: Directory for logs and exported bundles.
        pop_tol: Max fractional deviation from equal population per district for `redist`.
        compactness: `redist_smc` compactness argument (default 1).
        redist_extras: Optional pass-through for advanced `redist` arguments
            (serialized in ``run.json`` where relevant).
    """

    n_sims: int
    n_runs: int = 1
    seed: int | None = None
    work_dir: Path = Path(".")
    pop_tol: float = _DEFAULT_ELECTOR_BALANCE.max_relative_deviation
    compactness: float = 1.0
    redist_extras: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.n_sims < 1:
            msg = f"n_sims must be >= 1, got {self.n_sims}"
            raise ValueError(msg)
        if self.n_runs < 1:
            msg = f"n_runs must be >= 1, got {self.n_runs}"
            raise ValueError(msg)
        if self.pop_tol <= 0:
            msg = f"pop_tol must be positive, got {self.pop_tol}"
            raise ValueError(msg)
        if self.compactness < 0:
            msg = f"compactness must be non-negative, got {self.compactness}"
            raise ValueError(msg)


@dataclass
class SamplerResult:
    """Outcome of a sampler invocation (subprocess or in-process)."""

    exit_code: int
    stdout_path: Path | None
    stderr_path: Path | None
    assignments_csv: Path | None
    ensemble: PlanEnsemble | None
    diagnostics: dict[str, Any] = field(default_factory=dict)
