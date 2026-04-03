"""Adjacency graph export (national or per-county); optional Folium after each county."""

from __future__ import annotations

import argparse
import sys

from hungary_ge.config import ADJACENCY_EDGES_PARQUET
from hungary_ge.pipeline.context import PipelineContext
from hungary_ge.pipeline.progress import county_tqdm
from hungary_ge.pipeline.stages.county_sequence import county_maz_sequence
from hungary_ge.pipeline.stages.graph_ops import run_graph_export
from hungary_ge.pipeline.stages.viz_tools import map_adjacency_argv, run_viz_subprocess

NAME = "graph"


def add_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--graph-contiguity",
        choices=("queen", "rook"),
        default="queen",
        help="Queen/Rook when --graph-fuzzy is not set",
    )
    parser.add_argument(
        "--graph-fuzzy",
        action="store_true",
        help="Fuzzy contiguity for graph export",
    )
    parser.add_argument(
        "--graph-fuzzy-buffering",
        action="store_true",
        help="With --graph-fuzzy: buffer in metric CRS",
    )
    parser.add_argument(
        "--graph-fuzzy-tolerance",
        type=float,
        default=0.005,
        help="Fuzzy tolerance (libpysal) when buffering distance is derived from bbox",
    )
    parser.add_argument(
        "--graph-fuzzy-buffer-m",
        type=float,
        default=None,
        help="Fixed fuzzy buffer in meters",
    )
    parser.add_argument(
        "--graph-fuzzy-metric-crs",
        type=str,
        default="EPSG:32633",
        help="Metric CRS for fuzzy buffering",
    )
    parser.add_argument(
        "--allow-disconnected-county-graph",
        action="store_true",
        help=(
            "County graph: write Parquet/meta even if the county subgraph has "
            "multiple components or island precincts (default: fail when graph_health.ok is false)"
        ),
    )
    parser.add_argument(
        "--no-county-maps",
        action="store_true",
        help=(
            "County mode: skip automatic Folium adjacency_map.html after each county graph "
            "(use viz stage-only or add viz later)"
        ),
    )


def run(ctx: PipelineContext) -> int:
    args = ctx.args
    repo_root = ctx.repo_root
    paths = ctx.paths
    pq_graph = ctx.pq_graph
    run_id = ctx.run_id
    exclude_maz_set = ctx.exclude_maz_set

    if not pq_graph.is_file():
        print(f"Missing precinct layer for graph: {pq_graph}", file=sys.stderr)
        return 1
    if args.mode == "national":
        return run_graph_export(repo_root, pq_graph, args)

    assert run_id is not None
    counts_pq = paths.county_oevk_counts_parquet(run_id)
    maz_list = county_maz_sequence(
        counts_pq,
        args.maz,
        log_prefix="",
        exclude_maz=exclude_maz_set,
    )
    if maz_list is None:
        return 1
    with county_tqdm(
        maz_list,
        desc="graph",
        no_progress=args.no_progress,
    ) as pbar:
        for maz in pbar:
            prefix = f"[run {run_id} county {maz}] "
            pbar.set_postfix_str(f"{maz} adjacency", refresh=False)
            print(f"{prefix}stage graph: adjacency -> counties/{maz}/graph/")  # noqa: T201
            edges_out = paths.county_graph_dir(run_id, maz) / ADJACENCY_EDGES_PARQUET
            code = run_graph_export(
                repo_root,
                pq_graph,
                args,
                maz_filter=maz,
                edges_parquet=edges_out,
                log_prefix=prefix,
                county_run_id=run_id,
                strict_county_connectivity=not args.allow_disconnected_county_graph,
            )
            if code != 0:
                return code
            if not args.no_county_maps:
                pbar.set_postfix_str(f"{maz} folium", refresh=False)
                print(f"{prefix}stage graph: Folium map -> adjacency_map.html")  # noqa: T201
                html_out = paths.county_adjacency_map_path(run_id, maz)
                vargv = map_adjacency_argv(
                    repo_root,
                    pq_graph,
                    args,
                    maz=maz,
                    out=html_out,
                )
                code = run_viz_subprocess(repo_root, vargv, log_prefix=prefix)
                if code != 0:
                    return code
    return 0
