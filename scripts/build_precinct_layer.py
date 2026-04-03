#!/usr/bin/env python3
"""Build canonical precinct GeoParquet (and optional GeoJSON) from ``szavkor_topo``.

Run from the repository root::

    uv run python scripts/build_precinct_layer.py

Thin wrapper around :mod:`hungary_ge.pipeline.precinct_etl` (also invoked in-process
by ``hungary-ge-pipeline``). See module docstring in ``precinct_etl`` for options.
"""

from __future__ import annotations

import sys

from hungary_ge.pipeline.precinct_etl import run_precinct_layer_etl

if __name__ == "__main__":
    raise SystemExit(run_precinct_layer_etl(sys.argv[1:]))
