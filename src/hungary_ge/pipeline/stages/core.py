"""Core CLI flags shared by all pipeline stages."""

from __future__ import annotations

import argparse
from pathlib import Path

from hungary_ge.pipeline.profiles import PROFILE_CHOICES

DEFAULT_STAGES: tuple[str, ...] = ("etl", "votes", "graph")

STAGE_CHOICES = (
    "etl",
    "votes",
    "allocation",
    "graph",
    "viz",
    "sample",
    "reports",
    "rollup",
    "policy_figures",
)


def add_core_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=Path.cwd(),
        help="Repository root (default: cwd)",
    )
    parser.add_argument(
        "--pipeline-profile",
        choices=PROFILE_CHOICES,
        default=None,
        metavar="PROFILE",
        help=(
            "Apply a named flag bundle: "
            "'plain' → precincts.parquet + queen (no fuzzy); "
            "'void_hex_fuzzy_latest' → void-hex ETL output + fuzzy graph flags + matching --parquet"
        ),
    )
    parser.add_argument(
        "--szavkor-root",
        type=Path,
        default=Path("data/raw/szavkor_topo"),
        help="Raw szavkor_topo root relative to repo unless absolute",
    )
    parser.add_argument(
        "--mode",
        choices=("national", "county"),
        default="national",
        help=(
            "national: single graph under data/processed/graph/; "
            "county: per-county graph/viz under runs/<run-id>/counties/<maz>/graph/"
        ),
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="Run folder name under data/processed/runs/ (required for --mode county or allocation stage)",
    )
    parser.add_argument(
        "--maz",
        type=str,
        default=None,
        help="County mode only: run county-scoped stages for this megye only (e.g. 01)",
    )
    parser.add_argument(
        "--exclude-maz",
        action="append",
        default=None,
        metavar="MAZ",
        help=(
            "County mode only: skip these megye codes (repeatable), e.g. "
            "--exclude-maz 01 to omit Budapest. When --maz is set, it must not be excluded."
        ),
    )
    parser.add_argument(
        "--only",
        nargs="+",
        choices=STAGE_CHOICES,
        default=None,
        help=f"Stages to run (default: {' '.join(DEFAULT_STAGES)})",
    )
    parser.add_argument(
        "--parquet",
        type=Path,
        default=Path("data/processed/precincts.parquet"),
        help="Precinct GeoParquet for graph and viz stages",
    )
