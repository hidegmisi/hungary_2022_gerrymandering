#!/usr/bin/env python3
"""Build canonical precinct GeoParquet (and optional GeoJSON) from ``szavkor_topo``.

Run from the repository root::

    uv run python scripts/build_precinct_layer.py

With optional void (gap) polygons (county shell minus szvk union)::

    uv run python scripts/build_precinct_layer.py --with-gaps --shell path/to/megye.geojson

See ``docs/data-model.md`` (ETL / provenance, void units) and ``README.md``.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any

from hungary_ge.config import ProcessedPaths
from hungary_ge.io import (
    GapBuildOptions,
    GapShellSource,
    build_gap_features_all_counties,
    build_precinct_gdf,
    merge_szvk_and_gaps,
    raw_precinct_list_total,
    read_shell_gdf,
    write_processed_geojson,
    write_processed_geoparquet,
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
        help="Path to shell GeoJSON/GPKG/Shapefile (required with --with-gaps)",
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

    gap_stats_payload: dict[str, object] | None = None
    shell_sha: str | None = None
    if args.with_gaps:
        if args.shell is None:
            print("--shell is required when using --with-gaps", file=sys.stderr)
            return 2
        shell_path = args.shell
        if not shell_path.is_absolute():
            shell_path = (repo_root / shell_path).resolve()
        if not shell_path.is_file():
            print(f"Shell file not found: {shell_path}", file=sys.stderr)
            return 2
        shell_src = GapShellSource(
            path=shell_path,
            maz_column=args.shell_maz_column,
            layer=args.shell_layer,
        )
        shell_gdf = read_shell_gdf(shell_src)
        gap_opts = GapBuildOptions(
            metric_crs=args.gap_metric_crs,
            min_area_m2=args.gap_min_area_m2,
            void_id_prefix=args.gap_void_prefix,
            precinct_union_buffer_m=args.gap_precinct_union_buffer_m,
        )
        gaps, gap_stats = build_gap_features_all_counties(
            shell_gdf,
            gdf,
            shell_maz_column=shell_src.maz_column,
            options=gap_opts,
        )
        gdf = merge_szvk_and_gaps(gdf, gaps)
        shell_sha = _sha256_file(shell_path)
        gap_stats_payload = {
            "n_shell_features_read": gap_stats.n_shell_features_read,
            "n_counties_processed": gap_stats.n_counties_processed,
            "n_gap_polygons": gap_stats.n_gap_polygons,
            "n_dropped_below_min_area": gap_stats.n_dropped_below_min_area,
            "total_gap_area_m2": gap_stats.total_gap_area_m2,
            "per_maz": gap_stats.per_maz,
            "options": {
                "metric_crs": gap_opts.metric_crs,
                "min_area_m2": gap_opts.min_area_m2,
                "void_id_prefix": gap_opts.void_id_prefix,
                "precinct_union_buffer_m": gap_opts.precinct_union_buffer_m,
                "shell_buffer_m": gap_opts.shell_buffer_m,
            },
        }
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

    payload: dict[str, Any] = {
        "szavkor_root": str(szavkor),
        "raw_list_total": raw_total,
        "n_files_read": stats.n_files_read,
        "n_records_in": stats.n_records_in,
        "n_rows_out": len(gdf),
        "n_rows_szvk": stats.n_rows_out,
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
    print(
        f"Wrote {len(gdf)} rows to {out_parquet} "
        f"(szvk rows: {stats.n_rows_out}, raw list rows: {raw_total}, "
        f"dropped szvk: {stats.n_dropped_unrepaired}{extra})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
