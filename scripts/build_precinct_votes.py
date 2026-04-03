#!/usr/bin/env python3
"""Build ``precinct_votes.parquet`` and ``focal_oevk_assignments.parquet`` from ``szavkor_topo``.

Run from the repository root::

    uv run python scripts/build_precinct_votes.py

Thin wrapper around :mod:`hungary_ge.pipeline.votes_etl`.
"""

from __future__ import annotations

import sys

from hungary_ge.pipeline.votes_etl import run_precinct_votes_etl

if __name__ == "__main__":
    raise SystemExit(run_precinct_votes_etl(sys.argv[1:]))
