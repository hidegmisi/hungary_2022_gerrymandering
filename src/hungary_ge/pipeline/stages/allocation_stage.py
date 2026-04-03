"""County OEVK count allocation from focal assignments."""

from __future__ import annotations

import argparse
import sys

from hungary_ge.pipeline.context import PipelineContext
from hungary_ge.pipeline.county_allocation import write_county_oevk_counts

NAME = "allocation"


def add_arguments(_parser: argparse.ArgumentParser) -> None:
    """Uses ``--run-id`` from core."""


def run(ctx: PipelineContext) -> int:
    paths = ctx.paths
    run_id = ctx.run_id
    assert run_id is not None
    prefix = f"[run {run_id}] "
    print(f"{prefix}stage allocation: county OEVK counts from focal")  # noqa: T201
    focal = paths.focal_oevk_assignments_parquet
    if not focal.is_file():
        print(f"{prefix}missing focal assignments: {focal}", file=sys.stderr)
        return 1
    try:
        pq_out, meta_out = write_county_oevk_counts(paths.run_dir(run_id), focal)
    except ValueError as exc:
        print(f"{prefix}allocation failed: {exc}", file=sys.stderr)
        return 1
    print(f"{prefix}wrote {pq_out.name} and {meta_out.name}")  # noqa: T201
    return 0
