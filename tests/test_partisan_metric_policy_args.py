"""CLI → MetricComputationPolicy wiring (slice 3)."""

from __future__ import annotations

from hungary_ge.metrics.policy import NumericalSafetyPolicy
from hungary_ge.pipeline.cli import build_parser
from hungary_ge.pipeline.partisan_metric_policy_args import (
    metric_computation_policy_from_namespace,
)


def test_parser_partisan_balance_defaults_on() -> None:
    parser = build_parser()
    args = parser.parse_args(["--mode", "national"])
    assert args.partisan_balance is True
    assert args.partisan_small_values == "raise"
    assert args.partisan_balance_eps_bloc is None
    assert args.partisan_balance_eps_total is None


def test_parser_no_partisan_balance() -> None:
    parser = build_parser()
    args = parser.parse_args(["--mode", "national", "--no-partisan-balance"])
    assert args.partisan_balance is False


def test_metric_computation_policy_from_namespace_defaults() -> None:
    parser = build_parser()
    args = parser.parse_args(["--mode", "national"])
    p = metric_computation_policy_from_namespace(args)
    assert p.balance.enabled is True
    d = NumericalSafetyPolicy()
    assert p.safety.eps_bloc == d.eps_bloc
    assert p.safety.eps_total == d.eps_total


def test_metric_computation_policy_from_namespace_overrides() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "--mode",
            "national",
            "--no-partisan-balance",
            "--partisan-balance-eps-bloc",
            "1e-6",
            "--partisan-balance-eps-total",
            "1e-8",
            "--partisan-small-values",
            "skip_balance",
        ]
    )
    p = metric_computation_policy_from_namespace(args)
    assert p.balance.enabled is False
    assert p.safety.eps_bloc == 1e-6
    assert p.safety.eps_total == 1e-8
    assert p.safety.on_small_values == "skip_balance"
