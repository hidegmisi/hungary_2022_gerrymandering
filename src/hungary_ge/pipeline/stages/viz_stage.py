"""Folium adjacency map (subprocess to ``map_adjacency.py``)."""

from __future__ import annotations

import argparse
from pathlib import Path

from hungary_ge.pipeline.context import PipelineContext
from hungary_ge.pipeline.progress import county_tqdm
from hungary_ge.pipeline.stages.county_sequence import county_maz_sequence
from hungary_ge.pipeline.stages.viz_tools import map_adjacency_argv, run_viz_subprocess

NAME = "viz"


def add_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--viz-maz",
        type=str,
        default=None,
        help="County code for viz stage (recommended)",
    )
    parser.add_argument(
        "--viz-out",
        type=Path,
        default=None,
        help="Output HTML for viz (default: map_adjacency default under graph/)",
    )
    parser.add_argument(
        "--viz-max-features",
        type=int,
        default=5000,
        help="Cap rows for viz stage",
    )
    parser.add_argument(
        "--viz-max-edges",
        type=int,
        default=50000,
        help="Cap edges drawn for viz stage",
    )


def run(ctx: PipelineContext) -> int:
    args = ctx.args
    repo_root = ctx.repo_root
    paths = ctx.paths
    pq_graph = ctx.pq_graph
    run_id = ctx.run_id
    exclude_maz_set = ctx.exclude_maz_set

    if args.mode == "national":
        vo: Path | None = None
        if args.viz_out is not None:
            vo = args.viz_out
            if not vo.is_absolute():
                vo = (repo_root / vo).resolve()
        viz_extra = map_adjacency_argv(
            repo_root,
            pq_graph,
            args,
            maz=args.viz_maz,
            out=vo,
        )
        return run_viz_subprocess(repo_root, viz_extra)

    assert run_id is not None
    counts_pq = paths.county_oevk_counts_parquet(run_id)
    maz_list_v = county_maz_sequence(
        counts_pq,
        args.maz,
        log_prefix="",
        exclude_maz=exclude_maz_set,
    )
    if maz_list_v is None:
        return 1
    with county_tqdm(
        maz_list_v,
        desc="viz",
        no_progress=args.no_progress,
    ) as pbar:
        for maz in pbar:
            prefix = f"[run {run_id} county {maz}] "
            pbar.set_postfix_str(f"{maz} folium", refresh=False)
            print(f"{prefix}stage viz: Folium map")  # noqa: T201
            vo = paths.county_adjacency_map_path(run_id, maz)
            viz_extra = map_adjacency_argv(
                repo_root,
                pq_graph,
                args,
                maz=maz,
                out=vo,
            )
            code = run_viz_subprocess(repo_root, viz_extra, log_prefix=prefix)
            if code != 0:
                return code
    return 0
