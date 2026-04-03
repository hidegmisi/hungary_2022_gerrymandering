"""Subprocess invocation of ``scripts/map_adjacency.py`` (Folium)."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import Any


def map_adjacency_argv(
    repo_root: Path,
    pq_graph: Path,
    args: Any,
    *,
    maz: str | None,
    out: Path | None,
) -> list[str]:
    """CLI args for ``scripts/map_adjacency.py``."""
    parquet_for_script = (
        pq_graph.relative_to(repo_root)
        if pq_graph.is_relative_to(repo_root)
        else pq_graph
    )
    argv: list[str] = [
        "--repo-root",
        str(repo_root),
        "--parquet",
        str(parquet_for_script),
        "--max-features",
        str(args.viz_max_features),
        "--max-edges",
        str(args.viz_max_edges),
    ]
    if args.graph_fuzzy:
        argv.append("--fuzzy")
    if args.graph_fuzzy_buffering:
        argv.append("--fuzzy-buffering")
    if args.graph_fuzzy_tolerance != 0.005:
        argv.extend(["--fuzzy-tolerance", str(args.graph_fuzzy_tolerance)])
    if args.graph_fuzzy_buffer_m is not None:
        argv.extend(["--fuzzy-buffer-m", str(args.graph_fuzzy_buffer_m)])
    if args.graph_fuzzy_metric_crs != "EPSG:32633":
        argv.extend(["--fuzzy-metric-crs", str(args.graph_fuzzy_metric_crs)])
    if not args.graph_fuzzy:
        argv.extend(["--contiguity", args.graph_contiguity])
    if maz is not None:
        argv.extend(["--maz", maz])
    if out is not None:
        argv.extend(["--out", str(out)])
    return argv


def run_viz_subprocess(
    repo_root: Path,
    extra: list[str],
    *,
    log_prefix: str = "",
) -> int:
    script = repo_root / "scripts" / "map_adjacency.py"
    if not script.is_file():
        print(f"Missing script: {script}", file=sys.stderr)
        return 2
    proc = subprocess.run(
        [sys.executable, str(script), *extra],
        check=False,
        cwd=str(repo_root),
    )
    code = int(proc.returncode)
    if code != 0:
        print(f"{log_prefix}viz subprocess exited {code}", file=sys.stderr)
    return code
