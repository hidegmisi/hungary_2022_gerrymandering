"""Electoral votes / focal assignments ETL stage."""

from __future__ import annotations

import argparse

from hungary_ge.pipeline.context import PipelineContext
from hungary_ge.pipeline.votes_etl import run_precinct_votes_etl

NAME = "votes"


def add_arguments(_parser: argparse.ArgumentParser) -> None:
    """No stage-specific flags (uses ``--szavkor-root`` from core)."""


def run(ctx: PipelineContext) -> int:
    args = ctx.args
    run_id = ctx.run_id
    prefix = f"[run {run_id}] " if args.mode == "county" and run_id else ""
    print(f"{prefix}stage votes: votes_etl (build_precinct_votes)")  # noqa: T201
    return run_precinct_votes_etl(
        [
            "--repo-root",
            str(ctx.repo_root),
            "--szavkor-root",
            str(ctx.szavkor),
        ],
    )
