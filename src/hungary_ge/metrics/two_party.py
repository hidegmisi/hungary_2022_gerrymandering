"""Two-party district aggregates, SMD seats, efficiency gap (Slice 9).

Seat rule: plurality in each district; ties credit **0.5** seats each.

Efficiency gap uses **winner surplus** on bloc totals: the winner wastes votes equal
to the margin ``|A-B|``; the loser wastes all of its votes. Normalized by total
two-party turnout across districts with positive turnout.
"""

from __future__ import annotations

import math
from collections.abc import Hashable, Sequence

import numpy as np


def _district_tie(a: float, b: float, *, turnout: float) -> bool:
    """Treat near-equal bloc totals as a tie (stabilizes symmetric balance + float error)."""
    if turnout <= 0.0:
        return True
    return math.isclose(a, b, rel_tol=1e-12, abs_tol=max(1e-9, 1e-12 * turnout))


def district_two_party_totals(
    district_labels: Sequence[Hashable],
    votes_a: np.ndarray,
    votes_b: np.ndarray,
) -> dict[Hashable, tuple[float, float]]:
    """Sum ``votes_a`` and ``votes_b`` by district label.

    Arrays must be length ``len(district_labels)``. Missing/NaN treated as 0.
    """
    if len(district_labels) != len(votes_a) or len(votes_a) != len(votes_b):
        msg = "district_labels, votes_a, votes_b must have equal length"
        raise ValueError(msg)
    va = np.asarray(votes_a, dtype=np.float64)
    vb = np.asarray(votes_b, dtype=np.float64)
    np.nan_to_num(va, copy=False, nan=0.0)
    np.nan_to_num(vb, copy=False, nan=0.0)
    out: dict[Hashable, tuple[float, float]] = {}
    for d, a, b in zip(district_labels, va, vb, strict=True):
        t_a, t_b = out.get(d, (0.0, 0.0))
        out[d] = (t_a + float(a), t_b + float(b))
    return out


def seat_share_a_smd(
    totals_by_district: dict[Hashable, tuple[float, float]],
) -> tuple[float, float]:
    """Return ``(seats_a, n_effective_districts)`` for two-party SMD.

    Ties use 0.5 seats each. Only districts with **positive** two-party turnout
    ``T = A+B`` count toward ``n_effective_districts`` for seat share denominator.
    """
    seats_a = 0.0
    n_eff = 0
    for a, b in totals_by_district.values():
        t = a + b
        if t <= 0.0:
            continue
        n_eff += 1
        if _district_tie(a, b, turnout=t):
            seats_a += 0.5
        elif a > b:
            seats_a += 1.0
        else:
            seats_a += 0.0
    return seats_a, float(n_eff)


def seat_share_a_rate(
    totals_by_district: dict[Hashable, tuple[float, float]],
) -> float:
    """``seats_a / n_effective_districts``, or 0 if none."""
    sa, n_e = seat_share_a_smd(totals_by_district)
    if n_e <= 0.0:
        return 0.0
    return sa / n_e


def national_two_party_shares(
    totals_by_district: dict[Hashable, tuple[float, float]],
) -> tuple[float, float, float]:
    """Return ``(V_A, V_B, V_A+V_B)`` summed over districts."""
    va = 0.0
    vb = 0.0
    for a, b in totals_by_district.values():
        va += a
        vb += b
    return va, vb, va + vb


def efficiency_gap_two_party(
    totals_by_district: dict[Hashable, tuple[float, float]],
) -> tuple[float, float, float, float]:
    """Efficiency-style gap from winner-surplus wasted votes on district totals.

    Returns ``(eg, wasted_a, wasted_b, total_two_party)`` where
    ``eg = (wasted_a - wasted_b) / total_two_party`` when ``total_two_party > 0``.

    Per district with positive turnout: if ``A > B``, wasted A is ``A - B`` and wasted B
    is ``B``; if ``B > A``, wasted A is ``A`` and wasted B is ``B - A``; if ``A == B``,
    both wasted totals are zero for that district.
    """
    w_a = 0.0
    w_b = 0.0
    tot = 0.0
    for a, b in totals_by_district.values():
        t = a + b
        if t <= 0.0:
            continue
        tot += t
        if _district_tie(a, b, turnout=t):
            pass
        elif a > b:
            w_a += a - b
            w_b += b
        else:
            w_a += a
            w_b += b - a
    if tot <= 0.0:
        return 0.0, w_a, w_b, tot
    return (w_a - w_b) / tot, w_a, w_b, tot


def mean_median_district_a_share(
    totals_by_district: dict[Hashable, tuple[float, float]],
) -> float | None:
    """Mean minus median of **district-level** ``A / (A+B)`` where ``A+B > 0``."""
    shares: list[float] = []
    for a, b in totals_by_district.values():
        t = a + b
        if t <= 0.0:
            continue
        shares.append(a / t)
    if not shares:
        return None
    arr = np.array(shares, dtype=np.float64)
    return float(np.mean(arr) - np.median(arr))
