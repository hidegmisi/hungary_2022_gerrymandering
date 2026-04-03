"""Generate memo-ready policy figures from rollup and county reports."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from hungary_ge.pipeline.context import PipelineContext
from hungary_ge.pipeline.partisan_metric_policy_args import (
    metric_computation_policy_from_namespace,
)
from hungary_ge.pipeline.policy_figures import (
    DEFAULT_STYLE,
    STYLE_CHOICES,
    generate_policy_figures,
)

NAME = "policy_figures"


def add_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--policy-figures-outdir",
        type=Path,
        default=None,
        help="policy_figures stage: output directory (default runs/<run_id>/policy_figures)",
    )
    parser.add_argument(
        "--policy-figures-votes",
        type=Path,
        default=Path("data/processed/precinct_votes.parquet"),
        help="policy_figures stage: precinct votes parquet",
    )
    parser.add_argument(
        "--policy-figures-focal",
        type=Path,
        default=Path("data/processed/focal_oevk_assignments.parquet"),
        help="policy_figures stage: focal OEVK assignments parquet",
    )
    parser.add_argument(
        "--policy-figures-party-coding",
        type=Path,
        default=None,
        help="policy_figures stage: optional party coding JSON override",
    )
    parser.add_argument(
        "--policy-figures-skip-draw-level",
        action="store_true",
        help="policy_figures stage: skip draw-level histograms (plots 05/06)",
    )
    parser.add_argument(
        "--policy-figures-style",
        choices=STYLE_CHOICES,
        default=DEFAULT_STYLE,
        help="policy_figures stage: visual preset for memo output",
    )


def run(ctx: PipelineContext) -> int:
    args = ctx.args
    paths = ctx.paths
    repo_root = ctx.repo_root
    run_id = ctx.run_id

    if args.mode != "county":
        print("policy_figures stage requires --mode county", file=sys.stderr)
        return 2
    assert run_id is not None

    out_dir = args.policy_figures_outdir
    if out_dir is None:
        out_dir = paths.run_dir(run_id) / "policy_figures"
    elif not out_dir.is_absolute():
        out_dir = (repo_root / out_dir).resolve()

    votes = args.policy_figures_votes
    if not votes.is_absolute():
        votes = (repo_root / votes).resolve()
    focal = args.policy_figures_focal
    if not focal.is_absolute():
        focal = (repo_root / focal).resolve()
    pcp = args.policy_figures_party_coding
    if pcp is not None and not pcp.is_absolute():
        pcp = (repo_root / pcp).resolve()

    prefix = f"[run {run_id}] "
    print(f"{prefix}stage policy_figures: generating memo charts")  # noqa: T201
    metric_policy = metric_computation_policy_from_namespace(args)
    try:
        outputs = generate_policy_figures(
            paths=paths,
            run_id=run_id,
            out_dir=out_dir,
            votes_parquet=votes,
            focal_parquet=focal,
            party_coding_path=pcp,
            style_name=args.policy_figures_style,
            skip_draw_level=bool(args.policy_figures_skip_draw_level),
            no_progress=bool(args.no_progress),
            log_prefix=prefix,
            metric_policy=metric_policy,
        )
    except (FileNotFoundError, OSError, ValueError) as exc:
        print(f"{prefix}policy_figures failed: {exc}", file=sys.stderr)
        return 1

    print(f"{prefix}wrote {len(outputs)} policy figure artifact(s) to {out_dir}")  # noqa: T201
    return 0

