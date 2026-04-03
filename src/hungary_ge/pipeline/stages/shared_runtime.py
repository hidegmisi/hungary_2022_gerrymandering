"""CLI flags shared by county-mode stages (progress / UX)."""

from __future__ import annotations

import argparse


def add_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--no-progress",
        action="store_true",
        help=(
            "County mode: disable tqdm bars and suppress live redist SMC stderr "
            "(also disabled when stderr is not a TTY; set TQDM_DISABLE=1 to force tqdm off)"
        ),
    )
