#!/usr/bin/env python3
"""Build canonical precinct GeoParquet (and optional GeoJSON) from ``szavkor_topo``.

Run from the repository root::

    uv run python scripts/build_precinct_layer.py

See ``docs/data-model.md`` (ETL / provenance) and ``README.md``.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

from hungary_ge.config import ProcessedPaths
from hungary_ge.io import (
    build_precinct_gdf,
    raw_precinct_list_total,
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
        "n_rows_out": stats.n_rows_out,
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

    if manifest_path is not None:
        _write_manifest(manifest_path, payload)

    print(
        f"Wrote {stats.n_rows_out} precincts to {out_parquet} "
        f"(raw list rows: {raw_total}, dropped: {stats.n_dropped_unrepaired})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
