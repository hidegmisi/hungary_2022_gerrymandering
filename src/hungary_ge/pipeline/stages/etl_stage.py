"""Precinct layer ETL stage."""

from __future__ import annotations

import argparse
from pathlib import Path

from hungary_ge.pipeline.context import PipelineContext
from hungary_ge.pipeline.precinct_etl import run_precinct_layer_etl

NAME = "etl"


def add_arguments(parser: argparse.ArgumentParser) -> None:
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


def run(ctx: PipelineContext) -> int:
    args = ctx.args
    repo_root = ctx.repo_root
    szavkor = ctx.szavkor
    run_id = ctx.run_id
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
    print(f"{prefix}stage etl: precinct_etl (build_precinct_layer)")  # noqa: T201
    return run_precinct_layer_etl(extra)
