"""Ensemble diagnostics (effective sample size, split acceptance, R-hat-style summaries).

Mirrors ALARM / redist emphasis on verifying exploration of the constrained
plan space. Implementations may wrap R ``summary.redist_plans`` or replicate
in Python.
"""

from __future__ import annotations

from hungary_ge.ensemble import PlanEnsemble


def summarize_ensemble(ensemble: PlanEnsemble) -> dict[str, object]:
    """Return diagnostic summaries for simulated plan draws."""
    raise NotImplementedError(
        "summarize_ensemble: implement once sampler outputs metadata columns"
    )


__all__ = ["summarize_ensemble"]
