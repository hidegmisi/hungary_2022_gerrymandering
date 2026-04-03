"""National rollup from per-county reports."""

from __future__ import annotations

import argparse
import sys

from hungary_ge.pipeline.context import PipelineContext
from hungary_ge.pipeline.national_rollup import write_national_report

NAME = "rollup"


def add_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--rollup-allow-partial",
        action="store_true",
        help=(
            "rollup stage: allow missing county report pairs; renormalize weights over "
            "counties that contributed both JSON files"
        ),
    )


def run(ctx: PipelineContext) -> int:
    args = ctx.args
    paths = ctx.paths
    run_id = ctx.run_id

    if args.mode != "county":
        print("rollup stage requires --mode county", file=sys.stderr)
        return 2
    assert run_id is not None
    prefix_rb = f"[run {run_id}] "
    print(f"{prefix_rb}stage rollup: national_report.json")  # noqa: T201
    try:
        out_nat = write_national_report(
            paths,
            run_id,
            allow_partial=args.rollup_allow_partial,
        )
    except (FileNotFoundError, ValueError) as exc:
        print(f"{prefix_rb}rollup failed: {exc}", file=sys.stderr)
        return 1
    print(f"{prefix_rb}wrote {out_nat.name}")  # noqa: T201
    return 0
