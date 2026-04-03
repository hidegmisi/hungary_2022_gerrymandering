"""CLI flags and Namespace → :class:`MetricComputationPolicy` for partisan metrics."""

from __future__ import annotations

import argparse

from hungary_ge.metrics.policy import (
    BalancePolicy,
    MetricComputationPolicy,
    NumericalSafetyPolicy,
    SmallValuesMode,
)


def add_arguments(parser: argparse.ArgumentParser) -> None:
    """Register flags used by ``reports`` and ``policy_figures`` (single global parser)."""
    g = parser.add_argument_group(
        "partisan_metrics",
        "Symmetric vote balance and safety for county partisan JSON and policy-figure draw metrics.",
    )
    g.add_argument(
        "--no-partisan-balance",
        dest="partisan_balance",
        action="store_false",
        help="Disable symmetric statewide two-bloc balance before partisan metrics.",
    )
    g.add_argument(
        "--partisan-balance",
        dest="partisan_balance",
        action="store_true",
        help="Enable symmetric statewide two-bloc balance (default).",
    )
    g.add_argument(
        "--partisan-balance-eps-bloc",
        type=float,
        default=None,
        metavar="EPS",
        help=(
            "Minimum bloc national total to allow balancing; below this uses "
            "--partisan-small-values (default: built-in policy eps_bloc)."
        ),
    )
    g.add_argument(
        "--partisan-balance-eps-total",
        type=float,
        default=None,
        metavar="EPS",
        help=(
            "National two-party total at or below this is degenerate for normalized metrics "
            "(default: built-in policy eps_total)."
        ),
    )
    g.add_argument(
        "--partisan-small-values",
        choices=("raise", "skip_balance", "clip"),
        default="raise",
        help="Behavior when balancing is unsafe (e.g. near-zero bloc totals).",
    )
    parser.set_defaults(partisan_balance=True)


def metric_computation_policy_from_namespace(
    ns: argparse.Namespace,
) -> MetricComputationPolicy:
    """Build policy from parsed pipeline args (see :func:`add_arguments`)."""
    d = NumericalSafetyPolicy()
    eps_total = getattr(ns, "partisan_balance_eps_total", None)
    eps_bloc = getattr(ns, "partisan_balance_eps_bloc", None)
    small: SmallValuesMode = getattr(ns, "partisan_small_values", "raise")
    return MetricComputationPolicy(
        balance=BalancePolicy(
            enabled=bool(getattr(ns, "partisan_balance", True)),
            mode="symmetric",
        ),
        safety=NumericalSafetyPolicy(
            eps_total=d.eps_total if eps_total is None else float(eps_total),
            eps_bloc=d.eps_bloc if eps_bloc is None else float(eps_bloc),
            on_small_values=small,
        ),
    )


__all__ = [
    "add_arguments",
    "metric_computation_policy_from_namespace",
]
