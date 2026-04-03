"""Policy objects for partisan metric computation (balancing, numerical safety).

Slice 1 domain model: configuration only. Transform and EG math wire up in later slices.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

BalanceMode = Literal["symmetric"]
SmallValuesMode = Literal["raise", "skip_balance", "clip"]


@dataclass(frozen=True)
class BalancePolicy:
    """Statewide two-bloc vote balancing (e.g. symmetric multiplicative scaling)."""

    enabled: bool = True
    mode: BalanceMode = "symmetric"


@dataclass(frozen=True)
class NumericalSafetyPolicy:
    """Guards for near-zero national totals and unstable scale factors.

    ``eps_bloc``: treat ``sum(A)`` or ``sum(B)`` at or below this as too small to
    balance safely (unless ``on_small_values`` chooses another path).

    ``eps_total``: treat ``sum(A+B)`` at or below this as degenerate turnout for
    normalized metrics.

    ``on_small_values``:
    - ``raise``: fail fast with a clear error (default for production reports).
    - ``skip_balance``: leave votes unscaled when balancing would be unsafe.
    - ``clip``: reserved for future use (e.g. clamp scale factor); same as skip until implemented.
    """

    eps_total: float = 1e-12
    eps_bloc: float = 1e-9
    on_small_values: SmallValuesMode = "raise"

    def __post_init__(self) -> None:
        if self.eps_total <= 0.0:
            msg = f"eps_total must be positive, got {self.eps_total!r}"
            raise ValueError(msg)
        if self.eps_bloc <= 0.0:
            msg = f"eps_bloc must be positive, got {self.eps_bloc!r}"
            raise ValueError(msg)


@dataclass(frozen=True)
class MetricComputationPolicy:
    """Composed policy passed through reports and draw-level recomputation."""

    balance: BalancePolicy = BalancePolicy()
    safety: NumericalSafetyPolicy = NumericalSafetyPolicy()


DEFAULT_METRIC_COMPUTATION_POLICY = MetricComputationPolicy()

__all__ = [
    "BalanceMode",
    "BalancePolicy",
    "DEFAULT_METRIC_COMPUTATION_POLICY",
    "MetricComputationPolicy",
    "NumericalSafetyPolicy",
    "SmallValuesMode",
]
