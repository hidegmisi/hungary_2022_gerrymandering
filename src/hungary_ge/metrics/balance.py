"""Symmetric statewide two-bloc vote balancing for partisan metrics."""

from __future__ import annotations

from typing import Any

import numpy as np

from hungary_ge.metrics.policy import MetricComputationPolicy

BalanceMetadata = dict[str, Any]


def apply_two_bloc_vote_balance(
    votes_a: np.ndarray,
    votes_b: np.ndarray,
    policy: MetricComputationPolicy,
) -> tuple[np.ndarray, np.ndarray, BalanceMetadata]:
    """Scale ``A' = A * x``, ``B' = B / x`` with ``x = sqrt(sum(B)/sum(A))`` when safe.

    Returns copies of the input arrays when balancing is disabled or skipped.
    Metadata is JSON-friendly (bool, float, str, None).
    """
    va = np.asarray(votes_a, dtype=np.float64).copy()
    vb = np.asarray(votes_b, dtype=np.float64).copy()
    np.nan_to_num(va, copy=False, nan=0.0)
    np.nan_to_num(vb, copy=False, nan=0.0)

    s_a = float(np.sum(va))
    s_b = float(np.sum(vb))
    s_tot = s_a + s_b
    safety = policy.safety

    base_meta: BalanceMetadata = {
        "balance_enabled_requested": bool(policy.balance.enabled),
        "balance_mode": policy.balance.mode,
        "sum_a_before": s_a,
        "sum_b_before": s_b,
        "sum_two_party_before": s_tot,
    }

    if not policy.balance.enabled:
        return va, vb, {**base_meta, "balance_applied": False, "skip_reason": "balance_disabled"}

    if s_tot <= safety.eps_total:
        return va, vb, {
            **base_meta,
            "balance_applied": False,
            "degenerate_two_party_turnout": True,
            "skip_reason": "total_turnout_below_eps_total",
        }

    if s_a <= safety.eps_bloc or s_b <= safety.eps_bloc:
        if safety.on_small_values == "raise":
            msg = (
                "symmetric vote balance unsafe: need positive bloc totals above "
                f"eps_bloc={safety.eps_bloc}; sum(A)={s_a}, sum(B)={s_b}. "
                "Use NumericalSafetyPolicy(on_small_values='skip_balance') to leave votes raw."
            )
            raise ValueError(msg)
        return va, vb, {
            **base_meta,
            "balance_applied": False,
            "skip_reason": "bloc_total_below_eps_bloc",
        }

    if policy.balance.mode != "symmetric":
        msg = f"unsupported balance mode {policy.balance.mode!r}"
        raise ValueError(msg)

    x = float(np.sqrt(s_b / s_a))
    if not np.isfinite(x) or x <= 0.0:
        if safety.on_small_values == "raise":
            msg = f"symmetric balance produced non-finite scale factor x={x!r}"
            raise ValueError(msg)
        return va, vb, {
            **base_meta,
            "balance_applied": False,
            "skip_reason": "non_finite_scale_factor",
        }

    va_out = va * x
    vb_out = vb / x
    if not np.all(np.isfinite(va_out)) or not np.all(np.isfinite(vb_out)):
        if safety.on_small_values == "raise":
            msg = "symmetric balance produced non-finite adjusted vote arrays"
            raise ValueError(msg)
        return va, vb, {
            **base_meta,
            "balance_applied": False,
            "skip_reason": "non_finite_adjusted_votes",
        }

    s_a_out = float(np.sum(va_out))
    s_b_out = float(np.sum(vb_out))
    return va_out, vb_out, {
        **base_meta,
        "balance_applied": True,
        "symmetric_scale_factor_x": x,
        "sum_a_after": s_a_out,
        "sum_b_after": s_b_out,
        "sum_two_party_after": s_a_out + s_b_out,
    }


__all__ = ["apply_two_bloc_vote_balance"]
