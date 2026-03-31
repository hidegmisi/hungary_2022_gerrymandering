"""Orchestrate build_precinct_layer, build_precinct_votes, adjacency export, optional Folium map.

Default stages: ``etl``, ``votes``, ``graph``. ``viz`` is opt-in (needs ``viz`` extra).
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

from hungary_ge.config import ProcessedPaths
from hungary_ge.graph import AdjacencyBuildOptions, adjacency_summary, build_adjacency
from hungary_ge.graph.adjacency_io import save_adjacency
from hungary_ge.io import load_processed_geoparquet
from hungary_ge.problem import OevkProblem, prepare_precinct_layer

DEFAULT_STAGES: tuple[str, ...] = ("etl", "votes", "graph")


def _run_script(repo_root: Path, script_name: str, extra: list[str]) -> int:
    script = repo_root / "scripts" / script_name
    if not script.is_file():
        print(f"Missing script: {script}", file=sys.stderr)
        return 2
    cmd = [sys.executable, str(script), *extra]
    proc = subprocess.run(cmd, check=False, cwd=str(repo_root))
    return int(proc.returncode)


def _run_graph(
    repo_root: Path,
    parquet: Path,
    *,
    contiguity: str,
    fuzzy: bool,
    fuzzy_buffering: bool,
    fuzzy_tolerance: float,
    fuzzy_buffer_m: float | None,
    fuzzy_metric_crs: str,
) -> int:
    pq = parquet.resolve()
    if not pq.is_file():
        print(f"Missing precinct layer for graph stage: {pq}", file=sys.stderr)
        return 1
    gdf = load_processed_geoparquet(pq)
    prob = OevkProblem(county_column=None, pop_column=None, crs="EPSG:4326")
    gdf2, pmap = prepare_precinct_layer(gdf, prob)
    if fuzzy:
        adj_opts = AdjacencyBuildOptions(
            fuzzy=True,
            fuzzy_buffering=fuzzy_buffering,
            fuzzy_tolerance=fuzzy_tolerance,
            fuzzy_buffer_m=fuzzy_buffer_m,
            fuzzy_metric_crs=fuzzy_metric_crs,
        )
    else:
        adj_opts = (
            AdjacencyBuildOptions(contiguity="rook")
            if contiguity == "rook"
            else AdjacencyBuildOptions(contiguity="queen")
        )
    graph = build_adjacency(gdf2, prob, pmap, options=adj_opts)
    summ = adjacency_summary(graph)
    print(summ)  # noqa: T201
    paths = ProcessedPaths(repo_root)
    edges_path = paths.adjacency_edges_parquet
    save_adjacency(
        graph,
        edges_path,
        build_options=adj_opts,
    )
    print(f"Wrote {edges_path} and metadata sidecar")  # noqa: T201
    return 0


def _run_viz(
    repo_root: Path,
    extra: list[str],
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
    return int(proc.returncode)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Pilot pipeline: ETL precinct layer, electoral parquets, adjacency Parquet; "
            "optional Folium map (uv sync --extra viz)."
        ),
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=Path.cwd(),
        help="Repository root (default: cwd)",
    )
    parser.add_argument(
        "--szavkor-root",
        type=Path,
        default=Path("data/raw/szavkor_topo"),
        help="Raw szavkor_topo root relative to repo unless absolute",
    )
    parser.add_argument(
        "--only",
        nargs="+",
        choices=("etl", "votes", "graph", "viz"),
        default=None,
        help=f"Stages to run (default: {' '.join(DEFAULT_STAGES)})",
    )
    parser.add_argument(
        "--parquet",
        type=Path,
        default=Path("data/processed/precincts.parquet"),
        help="Precinct GeoParquet for graph and viz stages",
    )
    parser.add_argument(
        "--etl-with-gaps",
        action="store_true",
        help="Pass --with-gaps to build_precinct_layer.py",
    )
    parser.add_argument(
        "--etl-shell",
        type=Path,
        default=None,
        help="Shell geometry file for --with-gaps (required when --etl-with-gaps is set)",
    )
    parser.add_argument(
        "--etl-void-hex",
        action="store_true",
        help="Pass --void-hex to build_precinct_layer.py (requires --etl-with-gaps)",
    )
    parser.add_argument(
        "--etl-out-parquet",
        type=Path,
        default=None,
        help="Optional --out-parquet for ETL (e.g. precincts_void_hex.parquet)",
    )
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
    args = parser.parse_args(argv)
    repo_root = args.repo_root.resolve()
    stages = list(dict.fromkeys(args.only))

    if args.etl_with_gaps and args.etl_shell is None:
        print("--etl-shell is required when using --etl-with-gaps", file=sys.stderr)
        return 2
    if args.etl_void_hex and not args.etl_with_gaps:
        print("--etl-void-hex requires --etl-with-gaps", file=sys.stderr)
        return 2

    szavkor = args.szavkor_root
    if not szavkor.is_absolute():
        szavkor = (repo_root / szavkor).resolve()
    needs_raw = "etl" in stages or "votes" in stages
    if needs_raw and not szavkor.is_dir():
        print(f"Missing szavkor_topo root: {szavkor}", file=sys.stderr)
        return 1

    pq_arg = args.parquet
    if not pq_arg.is_absolute():
        pq_graph = (repo_root / pq_arg).resolve()
    else:
        pq_graph = pq_arg.resolve()

    for stage in stages:
        if stage == "etl":
            extra: list[str] = [
                "--repo-root",
                str(repo_root),
                "--szavkor-root",
                str(szavkor),
            ]
            if args.etl_with_gaps:
                assert args.etl_shell is not None
                shell = args.etl_shell
                if not shell.is_absolute():
                    shell = (repo_root / shell).resolve()
                extra.extend(["--with-gaps", "--shell", str(shell)])
            if args.etl_void_hex:
                extra.append("--void-hex")
            if args.etl_out_parquet is not None:
                out_pq = args.etl_out_parquet
                if not out_pq.is_absolute():
                    out_pq = (repo_root / out_pq).resolve()
                extra.extend(["--out-parquet", str(out_pq)])
            code = _run_script(repo_root, "build_precinct_layer.py", extra)
            if code != 0:
                return code
        elif stage == "votes":
            code = _run_script(
                repo_root,
                "build_precinct_votes.py",
                ["--repo-root", str(repo_root), "--szavkor-root", str(szavkor)],
            )
            if code != 0:
                return code
        elif stage == "graph":
            if not pq_graph.is_file():
                print(f"Missing precinct layer for graph: {pq_graph}", file=sys.stderr)
                return 1
            code = _run_graph(
                repo_root,
                pq_graph,
                contiguity=args.graph_contiguity,
                fuzzy=args.graph_fuzzy,
                fuzzy_buffering=args.graph_fuzzy_buffering,
                fuzzy_tolerance=args.graph_fuzzy_tolerance,
                fuzzy_buffer_m=args.graph_fuzzy_buffer_m,
                fuzzy_metric_crs=args.graph_fuzzy_metric_crs,
            )
            if code != 0:
                return code
        elif stage == "viz":
            parquet_for_script = (
                pq_graph.relative_to(repo_root)
                if pq_graph.is_relative_to(repo_root)
                else pq_graph
            )
            viz_extra: list[str] = [
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
                viz_extra.append("--fuzzy")
            if args.graph_fuzzy_buffering:
                viz_extra.append("--fuzzy-buffering")
            if args.graph_fuzzy_tolerance != 0.005:
                viz_extra.extend(["--fuzzy-tolerance", str(args.graph_fuzzy_tolerance)])
            if args.graph_fuzzy_buffer_m is not None:
                viz_extra.extend(["--fuzzy-buffer-m", str(args.graph_fuzzy_buffer_m)])
            if args.graph_fuzzy_metric_crs != "EPSG:32633":
                viz_extra.extend(
                    ["--fuzzy-metric-crs", str(args.graph_fuzzy_metric_crs)]
                )
            if not args.graph_fuzzy:
                viz_extra.extend(["--contiguity", args.graph_contiguity])
            if args.viz_maz is not None:
                viz_extra.extend(["--maz", args.viz_maz])
            if args.viz_out is not None:
                vo = args.viz_out
                if not vo.is_absolute():
                    vo = (repo_root / vo).resolve()
                viz_extra.extend(["--out", str(vo)])
            code = _run_viz(repo_root, viz_extra)
            if code != 0:
                return code

    return 0


def main_entry() -> None:
    """Setuptools / ``uv run hungary-ge-pipeline`` console_script target."""
    raise SystemExit(main())
