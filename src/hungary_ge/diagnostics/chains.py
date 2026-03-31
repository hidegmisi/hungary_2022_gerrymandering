"""Multi-chain scalar summaries and univariate Gelman–Rubin R-hat."""

from __future__ import annotations

import numpy as np


def gelman_rubin_rhat_univariate(chain_samples: list[np.ndarray]) -> float:
    """Gelman–Rubin ``\\hat{R}`` for one scalar trajectory per chain.

    Each array is a 1D sequence of per-draw statistics from one independent
    chain. Chains are truncated to a common minimum length. Returns
    ``nan`` if fewer than two chains or fewer than two retained iterations per
    chain. Returns ``1.0`` if within-chain variance is zero and between is
    zero; ``inf`` if within is zero but between is positive.
    """
    if len(chain_samples) < 2:
        return float("nan")
    chains = [np.asarray(c, dtype=np.float64).ravel() for c in chain_samples]
    ms = [len(c) for c in chains]
    m = min(ms)
    if m < 2:
        return float("nan")
    chains = [c[:m] for c in chains]
    J = len(chains)
    psi_bar_j = np.array([float(np.mean(c)) for c in chains])
    psi_bar = float(np.mean(psi_bar_j))
    B = (m / (J - 1.0)) * float(np.sum((psi_bar_j - psi_bar) ** 2))
    s2 = np.array([float(np.var(c, ddof=1)) for c in chains])
    W = float(np.mean(s2))
    if W == 0.0:
        return 1.0 if B == 0.0 else float("inf")
    var_hat = (m - 1.0) / m * W + B / m
    # Plain sqrt(var_hat/W) can fall slightly below 1 with noise when B≈0; clamp for reporting.
    return float(max(1.0, np.sqrt(var_hat / W)))


def split_draw_indices_by_chain(
    chain_per_draw: tuple[int, ...],
) -> dict[int, list[int]]:
    """Map chain id -> sorted draw column indices (0-based)."""
    out: dict[int, list[int]] = {}
    for j, ch in enumerate(chain_per_draw):
        out.setdefault(int(ch), []).append(j)
    for ch in out:
        out[ch].sort()
    return out


def per_chain_scalar_sequences(
    per_draw_values: np.ndarray,
    chain_per_draw: tuple[int, ...],
) -> list[np.ndarray]:
    """Collect ``per_draw_values[j]`` into one array per chain (column order preserved)."""
    buckets = split_draw_indices_by_chain(chain_per_draw)
    chains_ids = sorted(buckets.keys())
    return [
        np.array([per_draw_values[j] for j in buckets[c]], dtype=np.float64)
        for c in chains_ids
    ]
