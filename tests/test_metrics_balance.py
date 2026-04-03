"""Symmetric vote balance helper (slice 2)."""

from __future__ import annotations

import numpy as np
import pytest

from hungary_ge.metrics.balance import apply_two_bloc_vote_balance
from hungary_ge.metrics.policy import (
    MetricComputationPolicy,
    NumericalSafetyPolicy,
)


def test_symmetric_balance_equalizes_national_totals() -> None:
    va = np.array([60.0, 40.0], dtype=np.float64)
    vb = np.array([40.0, 60.0], dtype=np.float64)
    policy = MetricComputationPolicy()
    va_o, vb_o, meta = apply_two_bloc_vote_balance(va, vb, policy)
    assert meta["balance_applied"] is True
    assert meta["sum_a_after"] == pytest.approx(meta["sum_b_after"], rel=0.0, abs=1e-6)
    assert float(np.sum(va_o)) == pytest.approx(float(np.sum(vb_o)), rel=0.0, abs=1e-6)


def test_balance_disabled_returns_unscaled() -> None:
    from hungary_ge.metrics.policy import BalancePolicy

    p = MetricComputationPolicy(balance=BalancePolicy(enabled=False))
    va = np.array([1.0, 2.0])
    vb = np.array([3.0, 4.0])
    va_o, vb_o, meta = apply_two_bloc_vote_balance(va, vb, p)
    assert meta["balance_applied"] is False
    assert np.allclose(va_o, va)
    assert np.allclose(vb_o, vb)


def test_balance_raises_when_one_bloc_zero_by_default() -> None:
    p = MetricComputationPolicy()
    with pytest.raises(ValueError, match="symmetric vote balance unsafe"):
        apply_two_bloc_vote_balance(np.array([10.0]), np.array([0.0]), p)


def test_balance_skip_when_bloc_small() -> None:
    p = MetricComputationPolicy(
        safety=NumericalSafetyPolicy(on_small_values="skip_balance"),
    )
    va_o, vb_o, meta = apply_two_bloc_vote_balance(np.array([10.0]), np.array([0.0]), p)
    assert meta["balance_applied"] is False
    assert meta.get("skip_reason") == "bloc_total_below_eps_bloc"


def test_balance_skips_when_total_below_eps_total() -> None:
    p = MetricComputationPolicy(safety=NumericalSafetyPolicy(eps_total=1e6))
    va_o, vb_o, meta = apply_two_bloc_vote_balance(np.array([1.0]), np.array([2.0]), p)
    assert meta["balance_applied"] is False
    assert meta.get("degenerate_two_party_turnout") is True
