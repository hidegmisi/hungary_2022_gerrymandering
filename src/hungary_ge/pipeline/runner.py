"""Orchestrate build_precinct_layer, build_precinct_votes, adjacency export, optional Folium map.

Default stages: ``etl``, ``votes``, ``graph``. ``viz`` is opt-in (needs ``viz`` extra).

``--mode county`` uses ``--run-id`` and per-county work under
``data/processed/runs/<run-id>/counties/<maz>/`` for ``graph``, ``viz``, and
``sample`` (``redist`` SMC → ``ensemble/ensemble_assignments.parquet``), and
``reports`` (``diagnostics.json`` + ``partisan_report.json`` per county), and
``rollup`` (``national_report.json``).
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path
from typing import Any

import pandas as pd

from hungary_ge.config import (
    ADJACENCY_EDGES_PARQUET,
    ENSEMBLE_ASSIGNMENTS_PARQUET,
    ProcessedPaths,
)
from hungary_ge.graph import AdjacencyBuildOptions, adjacency_summary, build_adjacency
from hungary_ge.graph.adjacency_io import save_adjacency
from hungary_ge.graph.national_adjacency import build_national_adjacency_merged
from hungary_ge.io import load_processed_geoparquet
from hungary_ge.pipeline.county_allocation import (
    normalize_maz,
    write_county_oevk_counts,
)
from hungary_ge.pipeline.county_reports import run_county_reports
from hungary_ge.pipeline.county_sample import (
    county_ndists_by_maz,
    run_county_redist_sample,
)
from hungary_ge.pipeline.national_rollup import write_national_report
from hungary_ge.pipeline.progress import county_tqdm
from hungary_ge.problem import OevkProblem, prepare_precinct_layer
from hungary_ge.sampling.redist_adapter import RedistBackendError

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
)


def _run_script(repo_root: Path, script_name: str, extra: list[str]) -> int:
    script = repo_root / "scripts" / script_name
    if not script.is_file():
        print(f"Missing script: {script}", file=sys.stderr)
        return 2
    cmd = [sys.executable, str(script), *extra]
    proc = subprocess.run(cmd, check=False, cwd=str(repo_root))
    return int(proc.returncode)


def _county_maz_sequence(
    counts_parquet: Path,
    maz_filter: str | None,
    *,
    log_prefix: str = "",
    exclude_maz: frozenset[str] | None = None,
) -> list[str] | None:
    """Return sorted county codes from allocation Parquet, or ``None`` on error."""
    if not counts_parquet.is_file():
        print(
            f"{log_prefix}missing county allocation table: {counts_parquet} "
            "(run the allocation stage first)",
            file=sys.stderr,
        )
        return None
    df = pd.read_parquet(counts_parquet)
    if "maz" not in df.columns:
        print(
            f"{log_prefix}county_oevk_counts.parquet missing 'maz' column",
            file=sys.stderr,
        )
        return None
    ex = exclude_maz or frozenset()
    maz_vals = sorted({normalize_maz(m) for m in df["maz"].tolist()})
    if maz_filter is not None:
        mf = normalize_maz(maz_filter)
        if mf in ex:
            print(
                f"{log_prefix}maz {mf!r} is excluded (--exclude-maz)",
                file=sys.stderr,
            )
            return None
        if mf not in maz_vals:
            print(
                f"{log_prefix}maz {mf!r} not in county allocation table "
                f"(available: {maz_vals!r})",
                file=sys.stderr,
            )
            return None
        return [mf]
    maz_vals = [m for m in maz_vals if m not in ex]
    if not maz_vals:
        print(
            f"{log_prefix}no counties left after --exclude-maz {sorted(ex)!r}",
            file=sys.stderr,
        )
        return None
    return maz_vals


def _adjacency_build_options_from_args(args: Any) -> AdjacencyBuildOptions:
    """Match graph stage / map_adjacency contiguity settings."""
    if args.graph_fuzzy:
        return AdjacencyBuildOptions(
            fuzzy=True,
            fuzzy_buffering=args.graph_fuzzy_buffering,
            fuzzy_tolerance=args.graph_fuzzy_tolerance,
            fuzzy_buffer_m=args.graph_fuzzy_buffer_m,
            fuzzy_metric_crs=args.graph_fuzzy_metric_crs,
        )
    if args.graph_contiguity == "rook":
        return AdjacencyBuildOptions(contiguity="rook")
    return AdjacencyBuildOptions(contiguity="queen")


def _run_allocation(paths: ProcessedPaths, run_id: str, log_prefix: str) -> int:
    focal = paths.focal_oevk_assignments_parquet
    if not focal.is_file():
        print(f"{log_prefix}missing focal assignments: {focal}", file=sys.stderr)
        return 1
    try:
        pq_out, meta_out = write_county_oevk_counts(paths.run_dir(run_id), focal)
    except ValueError as exc:
        print(f"{log_prefix}allocation failed: {exc}", file=sys.stderr)
        return 1
    print(f"{log_prefix}wrote {pq_out.name} and {meta_out.name}")  # noqa: T201
    return 0


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
    maz_filter: str | None = None,
    edges_parquet: Path | None = None,
    log_prefix: str = "",
    county_run_id: str | None = None,
    strict_county_connectivity: bool = True,
    legacy_national_graph: bool = False,
) -> int:
    pq = parquet.resolve()
    if not pq.is_file():
        print(
            f"{log_prefix}Missing precinct layer for graph stage: {pq}", file=sys.stderr
        )
        return 1
    gdf = load_processed_geoparquet(pq)
    if maz_filter is not None:
        if "maz" not in gdf.columns:
            print(
                f"{log_prefix}precinct layer has no 'maz' column; cannot filter by county",
                file=sys.stderr,
            )
            return 1
        mzn = gdf["maz"].map(normalize_maz)
        gdf = gdf[mzn == normalize_maz(maz_filter)].copy()
        if gdf.empty:
            print(
                f"{log_prefix}no precinct rows for maz={normalize_maz(maz_filter)!r}",
                file=sys.stderr,
            )
            return 1
    prob = OevkProblem(county_column=None, pop_column=None, crs="EPSG:4326")
    paths = ProcessedPaths(repo_root)
    edges_path = (
        edges_parquet if edges_parquet is not None else paths.adjacency_edges_parquet
    )
    extra_meta: dict[str, Any] | None = None

    merge_national = (
        maz_filter is None and not legacy_national_graph and "maz" in gdf.columns
    )
    if maz_filter is None and not legacy_national_graph and "maz" not in gdf.columns:
        print(
            f"{log_prefix}national graph: no 'maz' column; using single-pass queen/rook/fuzzy",
            file=sys.stderr,
        )
    if merge_national:
        buf_m = fuzzy_buffer_m if fuzzy_buffer_m is not None else 3.0
        adj_opts = AdjacencyBuildOptions(
            fuzzy=True,
            fuzzy_buffering=True,
            fuzzy_tolerance=fuzzy_tolerance,
            fuzzy_buffer_m=buf_m,
            fuzzy_metric_crs=fuzzy_metric_crs,
        )
        try:
            graph = build_national_adjacency_merged(gdf, prob, adj_opts)
        except ValueError as exc:
            print(
                f"{log_prefix}national county-merge graph failed: {exc}",
                file=sys.stderr,
            )
            return 1
        summ = adjacency_summary(graph)
        print(f"{log_prefix}{summ}")  # noqa: T201
        extra_meta = {"national_county_merge": True}
        save_adjacency(
            graph,
            edges_path,
            build_options=adj_opts,
            extra_meta=extra_meta,
        )
        print(f"{log_prefix}Wrote {edges_path} and metadata sidecar")  # noqa: T201
        return 0

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
    print(f"{log_prefix}{summ}")  # noqa: T201
    if maz_filter is not None:
        maz_n = normalize_maz(maz_filter)
        island_ids = [str(graph.order.ids[i]) for i in graph.island_nodes[:50]]
        ok = graph.n_components == 1 and len(graph.island_nodes) == 0
        warnings: list[str] = []
        if graph.n_components != 1:
            warnings.append(
                f"multiple connected components ({graph.n_components}); "
                f"largest size {graph.largest_component_size}"
            )
        if graph.island_nodes:
            warnings.append(
                f"{len(graph.island_nodes)} island precinct(s) (no contiguity neighbors)"
            )
        gh: dict[str, Any] = {
            "ok": ok,
            "n_components": graph.n_components,
            "n_island_nodes": len(graph.island_nodes),
            "largest_component_size": graph.largest_component_size,
            "island_precinct_ids": island_ids,
            "warnings": warnings,
        }
        extra_meta = {
            "county_maz": maz_n,
            "run_id": county_run_id,
            "graph_health": gh,
        }
        if strict_county_connectivity and not ok:
            print(
                f"{log_prefix}county graph graph_health.ok is false "
                f"(components={graph.n_components}, "
                f"n_islands={len(graph.island_nodes)}). "
                "Use --allow-disconnected-county-graph to save anyway.",
                file=sys.stderr,
            )
            return 1
    save_adjacency(
        graph,
        edges_path,
        build_options=adj_opts,
        extra_meta=extra_meta,
    )
    print(f"{log_prefix}Wrote {edges_path} and metadata sidecar")  # noqa: T201
    return 0


def _map_adjacency_argv(
    repo_root: Path,
    pq_graph: Path,
    args: Any,
    *,
    maz: str | None,
    out: Path | None,
    legacy_national_graph: bool = False,
) -> list[str]:
    """CLI args for ``scripts/map_adjacency.py`` (shared by viz stage and county auto-maps)."""
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
    if legacy_national_graph:
        argv.append("--legacy-national-graph")
    return argv


def _run_viz(
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


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Pilot pipeline: ETL precinct layer, electoral parquets, adjacency Parquet; "
            "optional Folium map (uv sync --extra viz). "
            "County mode graph writes adjacency + graph_health meta per megye and, "
            "by default, adjacency_map.html after each county (see --no-county-maps)."
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
    parser.add_argument(
        "--etl-with-gaps",
        action="store_true",
        help="Pass --with-gaps to build_precinct_layer.py",
    )
    parser.add_argument(
        "--etl-shell",
        type=Path,
        default=None,
        help=(
            "Shell boundary file or admin directory (01.geojson…20.geojson); "
            "default: data/raw/admin when --etl-with-gaps"
        ),
    )
    parser.add_argument(
        "--etl-void-hex",
        action="store_true",
        help="Pass --void-hex to build_precinct_layer.py (requires --etl-with-gaps)",
    )
    parser.add_argument(
        "--etl-void-hex-cell-area-m2",
        type=float,
        default=None,
        help="Pass --void-hex-cell-area-m2 to build_precinct_layer.py",
    )
    parser.add_argument(
        "--etl-void-hex-max-cells-per-gap",
        type=int,
        default=None,
        help="Pass --void-hex-max-cells-per-gap to build_precinct_layer.py",
    )
    parser.add_argument(
        "--etl-void-hex-min-fragment-width-m",
        type=float,
        default=None,
        help="Pass --void-hex-min-fragment-width-m to build_precinct_layer.py",
    )
    parser.add_argument(
        "--etl-void-hex-min-fragment-area-fraction",
        type=float,
        default=None,
        help="Pass --void-hex-min-fragment-area-fraction to build_precinct_layer.py",
    )
    parser.add_argument(
        "--etl-out-parquet",
        type=Path,
        default=None,
        help="Optional --out-parquet for ETL (e.g. precincts_void_hex.parquet)",
    )
    parser.add_argument(
        "--etl-no-geometry-repair",
        action="store_true",
        help="Pass --no-geometry-repair to build_precinct_layer.py",
    )
    parser.add_argument(
        "--etl-hub-drop-enable",
        action="store_true",
        help="Pass --hub-drop-enable to build_precinct_layer.py",
    )
    parser.add_argument(
        "--etl-hub-drop-hard-partners",
        type=int,
        default=100,
        help="Pass --hub-drop-hard-partners (default 100; 0 disables hard tier)",
    )
    parser.add_argument(
        "--etl-hub-drop-soft-partners",
        type=int,
        default=30,
        help="Pass --hub-drop-soft-partners (default 30; 0 disables soft tier)",
    )
    parser.add_argument(
        "--etl-hub-drop-mass-ratio",
        type=float,
        default=1.5,
        help="Pass --hub-drop-mass-ratio to build_precinct_layer.py",
    )
    parser.add_argument(
        "--etl-hub-drop-min-overlap-m2",
        type=float,
        default=5.0,
        help="Pass --hub-drop-min-overlap-m2 to build_precinct_layer.py",
    )
    parser.add_argument(
        "--etl-hub-drop-max-rows",
        type=int,
        default=200,
        help="Pass --hub-drop-max-rows (0 = no limit)",
    )
    parser.add_argument(
        "--etl-hub-drop-allow-exceed-max",
        action="store_true",
        help="Pass --hub-drop-allow-exceed-max to build_precinct_layer.py",
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
        "--graph-legacy-national",
        action="store_true",
        help=(
            "National graph/viz: single-pass adjacency on full layer. "
            "Default is per-county fuzzy merge + bicounty cross edges (needs 'maz')."
        ),
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
    parser.add_argument(
        "--no-progress",
        action="store_true",
        help=(
            "County mode: disable tqdm bars and suppress live redist SMC stderr "
            "(also disabled when stderr is not a TTY; set TQDM_DISABLE=1 to force tqdm off)"
        ),
    )
    parser.add_argument(
        "--sample-n-draws",
        type=int,
        default=4,
        help="sample stage: SMC draws per county (redist nsims)",
    )
    parser.add_argument(
        "--sample-n-runs",
        type=int,
        default=1,
        help="sample stage: parallel SMC runs (redist nruns)",
    )
    parser.add_argument(
        "--sample-seed",
        type=int,
        default=None,
        help="sample stage: optional RNG seed for redist",
    )
    parser.add_argument(
        "--sample-pop-column",
        type=str,
        default="voters",
        help="sample stage: numeric weight column on precinct layer (redist pop)",
    )
    parser.add_argument(
        "--sample-pop-tol",
        type=float,
        default=0.25,
        help="sample stage: max relative population deviation per district",
    )
    parser.add_argument(
        "--sample-compactness",
        type=float,
        default=1.0,
        help="sample stage: redist compactness weight",
    )
    parser.add_argument(
        "--sample-rscript-path",
        type=Path,
        default=None,
        help="sample stage: override path to r/redist/run_smc.R",
    )
    parser.add_argument(
        "--sample-fail-fast",
        action="store_true",
        help="sample stage: exit on first county failure (default: run all counties, then fail if any error)",
    )
    parser.add_argument(
        "--sample-skip-existing",
        action="store_true",
        help=(
            "sample stage: skip a county when ensemble_assignments.parquet already exists "
            "(resume long multi-county runs)"
        ),
    )
    parser.add_argument(
        "--reports-votes",
        type=Path,
        default=Path("data/processed/precinct_votes.parquet"),
        help="reports stage: precinct votes Parquet",
    )
    parser.add_argument(
        "--reports-focal",
        type=Path,
        default=Path("data/processed/focal_oevk_assignments.parquet"),
        help="reports stage: focal OEVK assignments Parquet",
    )
    parser.add_argument(
        "--reports-pop-column",
        type=str,
        default="voters",
        help="reports stage: population/weight column in votes (diagnostics)",
    )
    parser.add_argument(
        "--reports-pop-tol",
        type=float,
        default=0.25,
        help="reports stage: flag draws exceeding this max rel pop deviation",
    )
    parser.add_argument(
        "--reports-party-coding",
        type=Path,
        default=None,
        help="reports stage: partisan party coding JSON (default: packaged stub)",
    )
    parser.add_argument(
        "--reports-loose-focal",
        action="store_true",
        help="reports stage: allow units with votes but missing focal district",
    )
    parser.add_argument(
        "--reports-fail-fast",
        action="store_true",
        help="reports stage: exit on first county failure",
    )
    parser.add_argument(
        "--reports-no-smc-log-scan",
        action="store_true",
        help="reports stage: skip redist log scrape in diagnostics",
    )
    parser.add_argument(
        "--rollup-allow-partial",
        action="store_true",
        help=(
            "rollup stage: allow missing county report pairs; renormalize weights over "
            "counties that contributed both JSON files"
        ),
    )
    args = parser.parse_args(argv)
    repo_root = args.repo_root.resolve()
    stages = list(dict.fromkeys(args.only or DEFAULT_STAGES))

    if args.mode == "county" and args.run_id is None:
        print("--run-id is required when using --mode county", file=sys.stderr)
        return 2
    if "allocation" in stages and args.run_id is None:
        print(
            "--run-id is required when the allocation stage is included",
            file=sys.stderr,
        )
        return 2

    if args.etl_with_gaps:
        if args.etl_shell is None:
            args.etl_shell = Path("data/raw/admin")
        shell_chk = args.etl_shell
        if not shell_chk.is_absolute():
            shell_chk = (repo_root / shell_chk).resolve()
        if not shell_chk.is_file() and not shell_chk.is_dir():
            print(f"--etl-shell not found: {shell_chk}", file=sys.stderr)
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

    paths = ProcessedPaths(repo_root)
    run_id = args.run_id
    exclude_maz_set: frozenset[str] | None = None
    if args.exclude_maz:
        exclude_maz_set = frozenset(
            normalize_maz(x) for x in args.exclude_maz if str(x).strip()
        )

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
            if args.etl_void_hex_cell_area_m2 is not None:
                extra.extend(
                    ["--void-hex-cell-area-m2", str(args.etl_void_hex_cell_area_m2)]
                )
            if args.etl_void_hex_max_cells_per_gap is not None:
                extra.extend(
                    [
                        "--void-hex-max-cells-per-gap",
                        str(args.etl_void_hex_max_cells_per_gap),
                    ]
                )
            if args.etl_void_hex_min_fragment_width_m is not None:
                extra.extend(
                    [
                        "--void-hex-min-fragment-width-m",
                        str(args.etl_void_hex_min_fragment_width_m),
                    ]
                )
            if args.etl_void_hex_min_fragment_area_fraction is not None:
                extra.extend(
                    [
                        "--void-hex-min-fragment-area-fraction",
                        str(args.etl_void_hex_min_fragment_area_fraction),
                    ]
                )
            if args.etl_out_parquet is not None:
                out_pq = args.etl_out_parquet
                if not out_pq.is_absolute():
                    out_pq = (repo_root / out_pq).resolve()
                extra.extend(["--out-parquet", str(out_pq)])
            if args.etl_no_geometry_repair:
                extra.append("--no-geometry-repair")
            if args.etl_hub_drop_enable:
                extra.append("--hub-drop-enable")
                extra.extend(
                    [
                        "--hub-drop-hard-partners",
                        str(args.etl_hub_drop_hard_partners),
                        "--hub-drop-soft-partners",
                        str(args.etl_hub_drop_soft_partners),
                        "--hub-drop-mass-ratio",
                        str(args.etl_hub_drop_mass_ratio),
                        "--hub-drop-min-overlap-m2",
                        str(args.etl_hub_drop_min_overlap_m2),
                        "--hub-drop-max-rows",
                        str(args.etl_hub_drop_max_rows),
                    ],
                )
                if args.etl_hub_drop_allow_exceed_max:
                    extra.append("--hub-drop-allow-exceed-max")
            prefix = f"[run {run_id}] " if args.mode == "county" and run_id else ""
            print(f"{prefix}stage etl: build_precinct_layer.py")  # noqa: T201
            code = _run_script(repo_root, "build_precinct_layer.py", extra)
            if code != 0:
                return code
        elif stage == "votes":
            prefix = f"[run {run_id}] " if args.mode == "county" and run_id else ""
            print(f"{prefix}stage votes: build_precinct_votes.py")  # noqa: T201
            code = _run_script(
                repo_root,
                "build_precinct_votes.py",
                ["--repo-root", str(repo_root), "--szavkor-root", str(szavkor)],
            )
            if code != 0:
                return code
        elif stage == "allocation":
            assert run_id is not None
            prefix = f"[run {run_id}] "
            print(f"{prefix}stage allocation: county OEVK counts from focal")  # noqa: T201
            code = _run_allocation(paths, run_id, prefix)
            if code != 0:
                return code
        elif stage == "graph":
            if not pq_graph.is_file():
                print(f"Missing precinct layer for graph: {pq_graph}", file=sys.stderr)
                return 1
            if args.mode == "national":
                code = _run_graph(
                    repo_root,
                    pq_graph,
                    contiguity=args.graph_contiguity,
                    fuzzy=args.graph_fuzzy,
                    fuzzy_buffering=args.graph_fuzzy_buffering,
                    fuzzy_tolerance=args.graph_fuzzy_tolerance,
                    fuzzy_buffer_m=args.graph_fuzzy_buffer_m,
                    fuzzy_metric_crs=args.graph_fuzzy_metric_crs,
                    legacy_national_graph=args.graph_legacy_national,
                )
                if code != 0:
                    return code
            else:
                assert run_id is not None
                counts_pq = paths.county_oevk_counts_parquet(run_id)
                maz_list = _county_maz_sequence(
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
                        print(f"{prefix}stage graph: adjacency → counties/{maz}/graph/")  # noqa: T201
                        edges_out = (
                            paths.county_graph_dir(run_id, maz)
                            / ADJACENCY_EDGES_PARQUET
                        )
                        code = _run_graph(
                            repo_root,
                            pq_graph,
                            contiguity=args.graph_contiguity,
                            fuzzy=args.graph_fuzzy,
                            fuzzy_buffering=args.graph_fuzzy_buffering,
                            fuzzy_tolerance=args.graph_fuzzy_tolerance,
                            fuzzy_buffer_m=args.graph_fuzzy_buffer_m,
                            fuzzy_metric_crs=args.graph_fuzzy_metric_crs,
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
                            print(
                                f"{prefix}stage graph: Folium map → adjacency_map.html"
                            )  # noqa: T201
                            html_out = paths.county_adjacency_map_path(run_id, maz)
                            vargv = _map_adjacency_argv(
                                repo_root,
                                pq_graph,
                                args,
                                maz=maz,
                                out=html_out,
                            )
                            code = _run_viz(repo_root, vargv, log_prefix=prefix)
                            if code != 0:
                                return code
        elif stage == "sample":
            if args.mode != "county":
                print("sample stage requires --mode county", file=sys.stderr)
                return 2
            if not pq_graph.is_file():
                print(f"Missing precinct layer for sample: {pq_graph}", file=sys.stderr)
                return 1
            assert run_id is not None
            counts_pq = paths.county_oevk_counts_parquet(run_id)
            maz_list_s = _county_maz_sequence(
                counts_pq,
                args.maz,
                log_prefix="",
                exclude_maz=exclude_maz_set,
            )
            if maz_list_s is None:
                return 1
            try:
                nd_map = county_ndists_by_maz(counts_pq)
            except ValueError as exc:
                print(f"sample: {exc}", file=sys.stderr)
                return 1
            adj_sample = _adjacency_build_options_from_args(args)
            rs_path = (
                Path(args.sample_rscript_path).resolve()
                if args.sample_rscript_path is not None
                else None
            )
            failures: list[str] = []
            with county_tqdm(
                maz_list_s,
                desc="sample",
                no_progress=args.no_progress,
            ) as pbar:
                for maz in pbar:
                    prefix = f"[run {run_id} county {maz}] "
                    ens_existing = (
                        paths.county_ensemble_dir(run_id, maz)
                        / ENSEMBLE_ASSIGNMENTS_PARQUET
                    )
                    if args.sample_skip_existing and ens_existing.is_file():
                        pbar.set_postfix_str(f"{maz} skip", refresh=True)
                        print(f"{prefix}sample: skip existing {ens_existing.name}")  # noqa: T201
                        continue
                    ndists_c = nd_map[maz]
                    pbar.set_postfix_str(f"{maz} SMC ndists={ndists_c}", refresh=False)
                    print(f"{prefix}stage sample: redist SMC, ndists={ndists_c}")  # noqa: T201
                    try:
                        run_county_redist_sample(
                            precinct_parquet=pq_graph,
                            paths=paths,
                            run_id=run_id,
                            maz=maz,
                            ndists=ndists_c,
                            pop_column=args.sample_pop_column,
                            adj_opts=adj_sample,
                            n_draws=args.sample_n_draws,
                            n_runs=args.sample_n_runs,
                            seed=args.sample_seed,
                            pop_tol=args.sample_pop_tol,
                            compactness=args.sample_compactness,
                            rscript_path=rs_path,
                            strict_county_connectivity=not args.allow_disconnected_county_graph,
                            log_prefix=prefix,
                            redist_progress=not args.no_progress,
                        )
                    except (ValueError, RedistBackendError) as exc:
                        pbar.set_postfix_str(f"{maz} fail", refresh=True)
                        print(f"{prefix}sample failed: {exc}", file=sys.stderr)
                        failures.append(maz)
                        if args.sample_fail_fast:
                            return 1
                        continue
                    pbar.set_postfix_str(f"{maz} done", refresh=True)
                    print(f"{prefix}wrote ensemble_assignments.parquet")  # noqa: T201
            if failures:
                print(
                    f"[run {run_id}] sample: failed counties: {sorted(failures)!r}",
                    file=sys.stderr,
                )
                return 1
        elif stage == "reports":
            if args.mode != "county":
                print("reports stage requires --mode county", file=sys.stderr)
                return 2
            assert run_id is not None
            counts_pq = paths.county_oevk_counts_parquet(run_id)
            maz_list_rpt = _county_maz_sequence(
                counts_pq,
                args.maz,
                log_prefix="",
                exclude_maz=exclude_maz_set,
            )
            if maz_list_rpt is None:
                return 1
            votes_path = args.reports_votes
            if not votes_path.is_absolute():
                votes_path = (repo_root / votes_path).resolve()
            focal_path = args.reports_focal
            if not focal_path.is_absolute():
                focal_path = (repo_root / focal_path).resolve()
            if not votes_path.is_file():
                print(f"reports: missing votes {votes_path}", file=sys.stderr)
                return 1
            if not focal_path.is_file():
                print(f"reports: missing focal {focal_path}", file=sys.stderr)
                return 1
            party_coding_path = (
                Path(args.reports_party_coding).resolve()
                if args.reports_party_coding is not None
                else None
            )
            if party_coding_path is not None and not party_coding_path.is_file():
                print(
                    f"reports: missing party coding {party_coding_path}",
                    file=sys.stderr,
                )
                return 1
            r_failures: list[str] = []
            with county_tqdm(
                maz_list_rpt,
                desc="reports",
                no_progress=args.no_progress,
            ) as pbar:
                for maz in pbar:
                    prefix = f"[run {run_id} county {maz}] "
                    ens_check = (
                        paths.county_ensemble_dir(run_id, maz)
                        / ENSEMBLE_ASSIGNMENTS_PARQUET
                    )
                    if not ens_check.is_file():
                        pbar.set_postfix_str(f"{maz} skip", refresh=True)
                        print(
                            f"{prefix}reports: skip (no {ens_check.name})",
                            file=sys.stderr,
                        )
                        continue
                    pbar.set_postfix_str(f"{maz} reports", refresh=False)
                    print(f"{prefix}stage reports: diagnostics + partisan")  # noqa: T201
                    try:
                        run_county_reports(
                            paths=paths,
                            run_id=run_id,
                            maz=maz,
                            votes_parquet=votes_path,
                            focal_parquet=focal_path,
                            pop_column=args.reports_pop_column,
                            pop_tol=args.reports_pop_tol,
                            party_coding=None,
                            party_coding_path=party_coding_path,
                            strict_focal_for_voting_units=not args.reports_loose_focal,
                            include_smc_log_scan=not args.reports_no_smc_log_scan,
                        )
                    except (OSError, ValueError) as exc:
                        pbar.set_postfix_str(f"{maz} fail", refresh=True)
                        print(f"{prefix}reports failed: {exc}", file=sys.stderr)
                        r_failures.append(maz)
                        if args.reports_fail_fast:
                            return 1
                        continue
                    pbar.set_postfix_str(f"{maz} done", refresh=True)
                    print(f"{prefix}wrote diagnostics.json and partisan_report.json")  # noqa: T201
            if r_failures:
                print(
                    f"[run {run_id}] reports: failed counties: {sorted(r_failures)!r}",
                    file=sys.stderr,
                )
                return 1
        elif stage == "rollup":
            if args.mode != "county":
                print("rollup stage requires --mode county", file=sys.stderr)
                return 2
            assert run_id is not None
            prefix_rb = f"[run {run_id}] "
            print(f"{prefix_rb}stage rollup: national_report.json")  # noqa: T201
            try:
                out_nat = write_national_report(
                    paths,
                    run_id,
                    allow_partial=args.rollup_allow_partial,
                )
            except (FileNotFoundError, ValueError) as exc:
                print(f"{prefix_rb}rollup failed: {exc}", file=sys.stderr)
                return 1
            print(f"{prefix_rb}wrote {out_nat.name}")  # noqa: T201
        elif stage == "viz":
            if args.mode == "national":
                vo: Path | None = None
                if args.viz_out is not None:
                    vo = args.viz_out
                    if not vo.is_absolute():
                        vo = (repo_root / vo).resolve()
                viz_extra = _map_adjacency_argv(
                    repo_root,
                    pq_graph,
                    args,
                    maz=args.viz_maz,
                    out=vo,
                    legacy_national_graph=args.graph_legacy_national,
                )
                code = _run_viz(repo_root, viz_extra)
                if code != 0:
                    return code
            else:
                assert run_id is not None
                counts_pq = paths.county_oevk_counts_parquet(run_id)
                maz_list_v = _county_maz_sequence(
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
                        viz_extra = _map_adjacency_argv(
                            repo_root,
                            pq_graph,
                            args,
                            maz=maz,
                            out=vo,
                        )
                        code = _run_viz(repo_root, viz_extra, log_prefix=prefix)
                        if code != 0:
                            return code

    return 0


def main_entry() -> None:
    """Setuptools / ``uv run hungary-ge-pipeline`` console_script target."""
    raise SystemExit(main())
