"""Slice 1: metric computation policy dataclasses."""

from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from hungary_ge.metrics.policy import (
    DEFAULT_METRIC_COMPUTATION_POLICY,
    BalancePolicy,
    MetricComputationPolicy,
    NumericalSafetyPolicy,
)


def test_default_metric_computation_policy() -> None:
    p = DEFAULT_METRIC_COMPUTATION_POLICY
    assert p.balance.enabled is True
    assert p.balance.mode == "symmetric"
    assert p.safety.on_small_values == "raise"
    assert p.safety.eps_total > 0.0
    assert p.safety.eps_bloc > 0.0


def test_metric_computation_policy_composition() -> None:
    p = MetricComputationPolicy(
        balance=BalancePolicy(enabled=False),
        safety=NumericalSafetyPolicy(
            eps_total=1e-6,
            eps_bloc=1e-3,
            on_small_values="skip_balance",
        ),
    )
    assert p.balance.enabled is False
    assert p.safety.eps_total == pytest.approx(1e-6)
    assert p.safety.on_small_values == "skip_balance"


def test_numerical_safety_policy_rejects_nonpositive_eps() -> None:
    with pytest.raises(ValueError, match="eps_total"):
        NumericalSafetyPolicy(eps_total=0.0)
    with pytest.raises(ValueError, match="eps_bloc"):
        NumericalSafetyPolicy(eps_bloc=-1.0)


def test_policies_are_frozen() -> None:
    p = BalancePolicy()
    with pytest.raises(FrozenInstanceError):
        p.enabled = False  # type: ignore[misc]
