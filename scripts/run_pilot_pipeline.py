#!/usr/bin/env python3
"""Slice 10 pilot orchestration (ETL, votes, adjacency Parquet, optional Folium map).

From the repository root::

    uv run python scripts/run_pilot_pipeline.py

Or::

    uv run python -m hungary_ge.pipeline

See ``REPRODUCIBILITY.md`` and ``docs/master-plan.md`` (Slice 10).
"""

from __future__ import annotations

from hungary_ge.pipeline.runner import main

if __name__ == "__main__":
    raise SystemExit(main())
