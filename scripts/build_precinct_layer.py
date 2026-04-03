#!/usr/bin/env python3
"""Build canonical precinct GeoParquet (and optional GeoJSON) from ``szavkor_topo``.

Run from the repository root::

    uv run python scripts/build_precinct_layer.py

With optional void (gap) polygons (county shell minus szvk union)::

    uv run python scripts/build_precinct_layer.py --with-gaps --shell data/raw/admin

``--shell`` may be a single boundary file or a directory of ``01.geojson`` … ``20.geojson``.

By default, szvk geometries are repaired in the gap metric CRS (``make_valid`` /
``buffer(0)``) before gap union and before writing Parquet; see manifest
``geometry_repair``. Use ``--no-geometry-repair`` to skip.

Optional ``--hub-drop-enable`` removes extreme overlap-hub szvk polygons after repair
and before gap union (spatial layer only); see manifest ``overlap_hub_drop``.
Vote extracts are unchanged; dropped ids may still appear in ``precinct_votes``.

See ``docs/data-model.md`` (ETL / provenance, void units) and ``README.md``.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Any

from hungary_ge.config import ProcessedPaths
from hungary_ge.io import (
    GapBuildOptions,
    GapShellSource,
    HexVoidOptions,
    build_gap_features_all_counties,
    build_precinct_gdf,
    compute_shell_source_sha256,
    merge_szvk_and_gaps,
    raw_precinct_list_total,
    read_shell_gdf,
    repair_precinct_geometries,
    write_processed_geojson,
    write_processed_geoparquet,
)
from hungary_ge.io.precinct_geometry_hub_drop import (
    OverlapHubDropOptions,
    drop_overlap_hub_szvk,
)


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _write_manifest(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Convert szavkor_topo settlement JSON to precinct GeoParquet.",
    )
    parser.add_argument(
        "--szavkor-root",
        type=Path,
        default=Path("data/raw/szavkor_topo"),
        help="Root folder with {maz}/{maz}-{taz}.json layout",
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=Path.cwd(),
        help="Repository root for default output paths",
    )
    parser.add_argument(
        "--out-parquet",
        type=Path,
        default=None,
        help="Output GeoParquet (default: <repo-root>/data/processed/precincts.parquet)",
    )
    parser.add_argument(
        "--out-geojson",
        type=Path,
        default=None,
        help="Optional GeoJSON copy",
    )
    parser.add_argument(
        "--manifest",
        type=Path,
        default=None,
        help="Write JSON manifest (stats + SHA-256 of parquet). "
        "Default: <repo-root>/data/processed/manifests/precincts_etl.json",
    )
    parser.add_argument(
        "--no-default-manifest",
        action="store_true",
        help="Skip writing the default manifest path when --manifest is omitted",
    )
    parser.add_argument(
        "--with-gaps",
        action="store_true",
        help="Add void (gap) polygons: county shell minus union(szvk), per county",
    )
    parser.add_argument(
        "--shell",
        type=Path,
        default=None,
        help=(
            "Shell boundary file (GeoJSON/GPKG/Shapefile) or directory of "
            "01.geojson…20.geojson (required with --with-gaps)"
        ),
    )
    parser.add_argument(
        "--shell-maz-column",
        type=str,
        default="maz",
        help="Column in shell layer matching precinct maz (two-digit county)",
    )
    parser.add_argument(
        "--shell-layer",
        type=str,
        default=None,
        help="Optional layer name for multi-layer formats (e.g. GPKG)",
    )
    parser.add_argument(
        "--no-geometry-repair",
        action="store_true",
        help="Skip metric make_valid/buffer(0) on szvk before gap union and parquet write",
    )
    parser.add_argument(
        "--hub-drop-enable",
        action="store_true",
        help="After repair: drop extreme overlap-hub szvk rows before gap union / write",
    )
    parser.add_argument(
        "--hub-drop-hard-partners",
        type=int,
        default=100,
        help="Hard tier: drop if overlap partners >= this (0 disables hard tier); default 100",
    )
    parser.add_argument(
        "--hub-drop-soft-partners",
        type=int,
        default=30,
        help="Soft tier: min partners when mass rule applies (0 disables soft tier); default 30",
    )
    parser.add_argument(
        "--hub-drop-mass-ratio",
        type=float,
        default=1.5,
        help="Soft tier: require sum_overlap_area_m2 >= ratio * area_m2; default 1.5",
    )
    parser.add_argument(
        "--hub-drop-min-overlap-m2",
        type=float,
        default=5.0,
        help="Material overlap threshold (m²) passed to overlap detection; default 5",
    )
    parser.add_argument(
        "--hub-drop-max-rows",
        type=int,
        default=200,
        help="Abort if more than this many precincts would be dropped (0 = no limit)",
    )
    parser.add_argument(
        "--hub-drop-allow-exceed-max",
        action="store_true",
        help="Allow dropping more precincts than --hub-drop-max-rows",
    )
    parser.add_argument(
        "--gap-metric-crs",
        type=str,
        default="EPSG:32633",
        help="Projected CRS for gap difference operations (meters)",
    )
    parser.add_argument(
        "--gap-min-area-m2",
        type=float,
        default=100.0,
        help="Drop gap fragments smaller than this area in metric CRS",
    )
    parser.add_argument(
        "--gap-void-prefix",
        type=str,
        default="gap",
        help="Prefix for synthetic precinct_id on void rows",
    )
    parser.add_argument(
        "--gap-precinct-union-buffer-m",
        type=float,
        default=0.0,
        help="Buffer (meters) applied to precinct union before difference (micro-gaps)",
    )
    parser.add_argument(
        "--void-hex",
        action="store_true",
        help="With --with-gaps: subdivide large voids into hex cells (requires --with-gaps)",
    )
    parser.add_argument(
        "--void-hex-cell-area-m2",
        type=float,
        default=None,
        help="Fixed hex cell area (m²); overrides mean-based auto sizing",
    )
    parser.add_argument(
        "--void-hex-area-factor",
        type=float,
        default=1.5,
        help="Auto cell area = mean_szvk_area * factor (after clamps); default 1.5",
    )
    parser.add_argument(
        "--void-hex-subdivide-min-factor",
        type=float,
        default=4.0,
        help="Subdivide void if area >= mean_szvk_area * this factor (unless --void-hex-subdivide-min-m2)",
    )
    parser.add_argument(
        "--void-hex-subdivide-min-m2",
        type=float,
        default=None,
        help="Subdivide void if area >= this (m²); overrides factor-based threshold",
    )
    parser.add_argument(
        "--void-hex-min-cell-m2",
        type=float,
        default=10_000.0,
        help="Clamp: minimum auto hex cell area (m²)",
    )
    parser.add_argument(
        "--void-hex-max-cell-m2",
        type=float,
        default=5_000_000.0,
        help="Clamp: maximum auto hex cell area (m²)",
    )
    parser.add_argument(
        "--void-hex-max-cells-per-gap",
        type=int,
        default=200_000,
        help="Safety cap on hex cells per raw gap polygon",
    )
    parser.add_argument(
        "--void-hex-no-auto",
        action="store_true",
        help="Disable mean-based sizing (requires --void-hex-cell-area-m2)",
    )
    parser.add_argument(
        "--void-hex-min-fragment-width-m",
        type=float,
        default=30.0,
        help=(
            "Drop clipped hex fragments thinner than this (meters; erosion test). "
            "Use 0 to disable. Default: 30."
        ),
    )
    parser.add_argument(
        "--void-hex-min-fragment-area-fraction",
        type=float,
        default=None,
        help=(
            "Require clipped fragment area >= this fraction of full hex cell area. "
            "Unset = no extra filter."
        ),
    )
    args = parser.parse_args()

    repo_root = args.repo_root.resolve()
    paths = ProcessedPaths(repo_root)
    szavkor = args.szavkor_root
    if not szavkor.is_absolute():
        szavkor = (repo_root / szavkor).resolve()

    out_parquet = args.out_parquet
    if out_parquet is None:
        out_parquet = paths.precincts_parquet
    elif not out_parquet.is_absolute():
        out_parquet = (repo_root / out_parquet).resolve()

    raw_total = raw_precinct_list_total(szavkor)
    gdf, stats = build_precinct_gdf(szavkor)

    geometry_repair_payload: dict[str, object] | None = None
    if not args.no_geometry_repair:
        gdf, repair_stats = repair_precinct_geometries(
            gdf,
            metric_crs=args.gap_metric_crs,
        )
        geometry_repair_payload = repair_stats.as_manifest_dict()

    overlap_hub_drop_payload: dict[str, object] | None = None
    if args.hub_drop_enable:
        hard = args.hub_drop_hard_partners if args.hub_drop_hard_partners > 0 else None
        soft = args.hub_drop_soft_partners if args.hub_drop_soft_partners > 0 else None
        hub_opts = OverlapHubDropOptions(
            hard_min_partners=hard,
            soft_min_partners=soft,
            mass_ratio=float(args.hub_drop_mass_ratio),
            max_drop_rows=int(args.hub_drop_max_rows),
            allow_exceed_max=bool(args.hub_drop_allow_exceed_max),
            overlap_min_overlap_m2=float(args.hub_drop_min_overlap_m2),
            metric_crs=args.gap_metric_crs,
        )
        gdf, hub_stats = drop_overlap_hub_szvk(gdf, options=hub_opts)
        overlap_hub_drop_payload = hub_stats.manifest_dict()

    if args.void_hex and not args.with_gaps:
        print("--void-hex requires --with-gaps", file=sys.stderr)
        return 2

    gap_stats_payload: dict[str, object] | None = None
    shell_sha: str | None = None
    if args.with_gaps:
        if (
            args.void_hex
            and args.void_hex_no_auto
            and args.void_hex_cell_area_m2 is None
        ):
            print(
                "--void-hex-no-auto requires --void-hex-cell-area-m2",
                file=sys.stderr,
            )
            return 2
        if args.shell is None:
            print("--shell is required when using --with-gaps", file=sys.stderr)
            return 2
        shell_path = args.shell
        if not shell_path.is_absolute():
            shell_path = (repo_root / shell_path).resolve()
        if not shell_path.is_file() and not shell_path.is_dir():
            print(f"Shell path not found: {shell_path}", file=sys.stderr)
            return 2
        shell_src = GapShellSource(
            path=shell_path,
            maz_column=args.shell_maz_column,
            layer=args.shell_layer,
        )
        shell_gdf = read_shell_gdf(shell_src)
        hex_void: HexVoidOptions | None = None
        if args.void_hex:
            hex_void = HexVoidOptions(
                enabled=True,
                auto_size=not args.void_hex_no_auto,
                hex_area_factor=args.void_hex_area_factor,
                hex_cell_area_m2=args.void_hex_cell_area_m2,
                subdivide_min_void_m2=args.void_hex_subdivide_min_m2,
                subdivide_min_void_factor=args.void_hex_subdivide_min_factor,
                hex_min_cell_area_m2=args.void_hex_min_cell_m2,
                hex_max_cell_area_m2=args.void_hex_max_cell_m2,
                max_cells_per_gap=args.void_hex_max_cells_per_gap,
                min_hex_fragment_width_m=args.void_hex_min_fragment_width_m,
                min_hex_fragment_area_fraction=args.void_hex_min_fragment_area_fraction,
            )
        gap_opts = GapBuildOptions(
            metric_crs=args.gap_metric_crs,
            min_area_m2=args.gap_min_area_m2,
            void_id_prefix=args.gap_void_prefix,
            precinct_union_buffer_m=args.gap_precinct_union_buffer_m,
            hex_void=hex_void,
        )
        gaps, gap_stats = build_gap_features_all_counties(
            shell_gdf,
            gdf,
            shell_maz_column=shell_src.maz_column,
            options=gap_opts,
        )
        gdf = merge_szvk_and_gaps(gdf, gaps)
        try:
            shell_sha = compute_shell_source_sha256(shell_path)
        except ValueError as exc:
            print(f"Shell fingerprint failed: {exc}", file=sys.stderr)
            return 2
        gap_stats_payload = {
            "n_shell_features_read": gap_stats.n_shell_features_read,
            "n_counties_processed": gap_stats.n_counties_processed,
            "n_gap_polygons": gap_stats.n_gap_polygons,
            "n_gap_polygons_raw": gap_stats.n_gap_polygons_raw,
            "n_void_cells_after_hex": gap_stats.n_void_cells_after_hex,
            "n_dropped_below_min_area": gap_stats.n_dropped_below_min_area,
            "total_gap_area_m2": gap_stats.total_gap_area_m2,
            "mean_szvk_area_m2": gap_stats.mean_szvk_area_m2,
            "hex_cell_area_m2_used": gap_stats.hex_cell_area_m2_used,
            "n_hex_cells_truncated": gap_stats.n_hex_cells_truncated,
            "n_void_polygons_dropped_post_quality": gap_stats.n_void_polygons_dropped_post_quality,
            "per_maz": gap_stats.per_maz,
            "options": {
                "metric_crs": gap_opts.metric_crs,
                "min_area_m2": gap_opts.min_area_m2,
                "void_id_prefix": gap_opts.void_id_prefix,
                "precinct_union_buffer_m": gap_opts.precinct_union_buffer_m,
                "shell_buffer_m": gap_opts.shell_buffer_m,
            },
        }
        if hex_void is not None:
            gap_stats_payload["hex_void"] = asdict(hex_void)
        if gap_stats.warnings:
            gap_stats_payload["warnings"] = gap_stats.warnings[:500]

    write_processed_geoparquet(gdf, out_parquet)

    out_geojson = args.out_geojson
    if out_geojson is not None:
        if not out_geojson.is_absolute():
            out_geojson = (repo_root / out_geojson).resolve()
        write_processed_geojson(gdf, out_geojson)

    manifest_path = args.manifest
    if manifest_path is None and not args.no_default_manifest:
        manifest_path = paths.manifest_json("precincts_etl")
    if manifest_path is not None and not manifest_path.is_absolute():
        manifest_path = (repo_root / manifest_path).resolve()

    if "unit_kind" in gdf.columns:
        n_rows_szvk_manifest = int(
            (gdf["unit_kind"].astype(str) == "szvk").sum(),
        )
    else:
        n_rows_szvk_manifest = len(gdf)

    payload: dict[str, Any] = {
        "szavkor_root": str(szavkor),
        "raw_list_total": raw_total,
        "n_files_read": stats.n_files_read,
        "n_records_in": stats.n_records_in,
        "n_rows_out": len(gdf),
        "n_rows_szvk": n_rows_szvk_manifest,
        "n_dropped_unrepaired": stats.n_dropped_unrepaired,
        "out_parquet": str(out_parquet),
        "out_parquet_sha256": _sha256_file(out_parquet),
        "crs": str(gdf.crs) if gdf.crs is not None else None,
    }
    if out_geojson is not None:
        payload["out_geojson"] = str(out_geojson)
    if stats.warnings:
        payload["warnings"] = stats.warnings[:500]
        if len(stats.warnings) > 500:
            payload["warnings_truncated"] = True

    if geometry_repair_payload is not None:
        payload["geometry_repair"] = geometry_repair_payload

    if overlap_hub_drop_payload is not None:
        payload["overlap_hub_drop"] = overlap_hub_drop_payload

    if args.with_gaps and gap_stats_payload is not None:
        payload["with_gaps"] = True
        payload["shell_path"] = str(shell_path)
        payload["shell_maz_column"] = args.shell_maz_column
        if args.shell_layer:
            payload["shell_layer"] = args.shell_layer
        payload["shell_sha256"] = shell_sha
        payload["gap_build"] = gap_stats_payload

    if manifest_path is not None:
        _write_manifest(manifest_path, payload)

    extra = ""
    if args.with_gaps and gap_stats_payload is not None:
        extra = f", void polygons: {gap_stats_payload['n_gap_polygons']}"
    hub_extra = ""
    if overlap_hub_drop_payload is not None:
        hub_extra = f", hub-drop szvk: {overlap_hub_drop_payload.get('n_dropped', 0)}"
    print(
        f"Wrote {len(gdf)} rows to {out_parquet} "
        f"(szvk rows: {n_rows_szvk_manifest}, raw list rows: {raw_total}, "
        f"dropped szvk (unrepaired): {stats.n_dropped_unrepaired}{hub_extra}{extra})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
