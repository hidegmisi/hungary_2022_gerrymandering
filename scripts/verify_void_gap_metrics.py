#!/usr/bin/env python3
"""Quantify county shell vs precinct-union area (void = shell \\ union(szvk)).

Mirrors metric logic in ``hungary_ge.io.gaps.build_gap_features_for_maz`` (EPSG:32633,
``unary_union`` with ``buffer(0)`` on parts) to explain large hex void coverage.

Example::

    uv run python scripts/verify_void_gap_metrics.py \\
        --repo-root . \\
        --precinct-parquet data/processed/precincts_void_hex.parquet \\
        --shell data/raw/admin \\
        --out-json data/processed/manifests/void_gap_metrics.json
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import geopandas as gpd
from shapely.ops import unary_union

from hungary_ge.io import GapShellSource, load_processed_geoparquet, read_shell_gdf
from hungary_ge.pipeline.county_allocation import normalize_maz

METRIC_CRS = "EPSG:32633"


def _geoms_for_unary_union(geoms: Any) -> list[Any]:
    """Match ``hungary_ge.io.gaps._geoms_for_unary_union``."""
    out: list[Any] = []
    for g in geoms:
        if g is None or g.is_empty:
            continue
        try:
            g2 = g.buffer(0)
        except Exception:
            g2 = g
        if not g2.is_empty:
            out.append(g2)
    return out


def shell_precinct_metrics_for_maz(
    shell_gdf: gpd.GeoDataFrame,
    szvk_gdf: gpd.GeoDataFrame,
    maz: str,
    *,
    shell_maz_column: str = "maz",
) -> dict[str, Any]:
    """Areas in m² (metric CRS); void = shell \\ union(precincts)."""
    maz_n = normalize_maz(maz)
    maz_shell = shell_gdf[shell_maz_column].map(normalize_maz)
    shell_part = shell_gdf[maz_shell == maz_n]
    maz_p = szvk_gdf["maz"].map(normalize_maz)
    prec_part = szvk_gdf[maz_p == maz_n]

    out: dict[str, Any] = {
        "maz": maz_n,
        "n_shell_rows": int(len(shell_part)),
        "n_szvk_rows": int(len(prec_part)),
        "shell_area_m2": None,
        "precinct_union_area_m2": None,
        "void_area_m2": None,
        "precinct_union_over_shell": None,
        "void_over_shell": None,
    }

    if shell_part.empty or prec_part.empty:
        return out

    shell_m = shell_part.to_crs(METRIC_CRS)
    prec_m = prec_part.to_crs(METRIC_CRS)

    shell_u = unary_union(_geoms_for_unary_union(shell_m.geometry))
    prec_u = unary_union(_geoms_for_unary_union(prec_m.geometry))

    if shell_u.is_empty:
        return out

    gap_m = shell_u.difference(prec_u)
    inter = shell_u.intersection(prec_u)
    a_shell = float(shell_u.area)
    a_prec = float(prec_u.area)
    a_void = float(gap_m.area)
    a_overlap = float(inter.area)

    out["shell_area_m2"] = a_shell
    out["precinct_union_area_m2"] = a_prec
    out["void_area_m2"] = a_void
    out["shell_precinct_intersection_area_m2"] = a_overlap
    if a_shell > 0:
        out["precinct_union_over_shell"] = a_prec / a_shell
        out["void_over_shell"] = a_void / a_shell
        out["shell_precinct_intersection_over_shell"] = a_overlap / a_shell
    return out


def analyze(
    repo_root: Path,
    precinct_parquet: Path,
    shell_paths: list[Path],
    *,
    shell_maz_column: str,
) -> dict[str, Any]:
    szvk = load_processed_geoparquet(precinct_parquet)
    if "unit_kind" in szvk.columns:
        szvk = szvk[szvk["unit_kind"].astype(str) == "szvk"].copy()
    if "maz" not in szvk.columns:
        msg = "precinct layer needs a maz column"
        raise ValueError(msg)

    maz_in_precincts = sorted(
        {normalize_maz(m) for m in szvk["maz"].unique()},
        key=lambda x: int(x) if x.isdigit() else x,
    )

    results: dict[str, Any] = {
        "metric_crs": METRIC_CRS,
        "precinct_parquet": str(precinct_parquet.resolve()),
        "n_szvk_rows": len(szvk),
        "shell_sources": [],
    }

    for shell_path in shell_paths:
        sp = (
            shell_path
            if shell_path.is_absolute()
            else (repo_root / shell_path).resolve()
        )
        if not sp.is_file() and not sp.is_dir():
            results["shell_sources"].append(
                {
                    "path": str(sp),
                    "error": "path not found",
                }
            )
            continue

        shell_gdf = read_shell_gdf(GapShellSource(path=sp, maz_column=shell_maz_column))
        maz_shell = shell_gdf[shell_maz_column].map(normalize_maz)
        maz_in_shell = sorted(
            {str(m) for m in maz_shell.unique()},
            key=lambda x: int(x) if x.isdigit() else x,
        )

        per_maz = [
            shell_precinct_metrics_for_maz(
                shell_gdf,
                szvk,
                m,
                shell_maz_column=shell_maz_column,
            )
            for m in maz_in_precincts
        ]

        prec_set = set(maz_in_precincts)
        shell_set = set(maz_in_shell)
        results["shell_sources"].append(
            {
                "path": str(sp),
                "n_shell_features": len(shell_gdf),
                "maz_in_shell": maz_in_shell,
                "maz_in_precincts_not_in_shell": sorted(prec_set - shell_set),
                "maz_in_shell_not_in_precincts": sorted(shell_set - prec_set),
                "per_maz": per_maz,
            }
        )

    return results


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Shell vs precinct union areas (void gap diagnostics).",
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=Path.cwd(),
        help="Repository root for relative paths",
    )
    parser.add_argument(
        "--precinct-parquet",
        type=Path,
        default=Path("data/processed/precincts_void_hex.parquet"),
        help="GeoParquet with szvk (+ optional void rows; szvk used only)",
    )
    parser.add_argument(
        "--shell",
        type=Path,
        action="append",
        default=None,
        help=(
            "Shell file or admin directory (repeat for multiple). "
            "Default: data/raw/admin"
        ),
    )
    parser.add_argument(
        "--shell-maz-column",
        type=str,
        default="maz",
        help="County id column on shell layer",
    )
    parser.add_argument(
        "--out-json",
        type=Path,
        default=None,
        help="Write full results as JSON (e.g. data/processed/manifests/void_gap_metrics.json)",
    )
    args = parser.parse_args()
    repo_root = args.repo_root.resolve()
    pq = args.precinct_parquet
    if not pq.is_absolute():
        pq = (repo_root / pq).resolve()
    if not pq.is_file():
        print(f"Missing precinct parquet: {pq}", file=sys.stderr)
        return 1

    shell_paths = args.shell
    if not shell_paths:
        shell_paths = [Path("data/raw/admin")]

    payload = analyze(
        repo_root,
        pq,
        list(shell_paths),
        shell_maz_column=args.shell_maz_column,
    )

    # Compact console table: first shell source only
    src0 = payload["shell_sources"][0] if payload["shell_sources"] else None
    if src0 and "per_maz" in src0:
        print(
            "maz  shell_km2  prec_km2  overlap_km2  void_km2  "
            "overlap/shell  void/shell  n_szvk  n_shell_rows"
        )
        for row in src0["per_maz"]:
            m = row["maz"]
            sa = row.get("shell_area_m2")
            pa = row.get("precinct_union_area_m2")
            va = row.get("void_area_m2")
            oa = row.get("shell_precinct_intersection_area_m2")
            os_ = row.get("shell_precinct_intersection_over_shell")
            vs = row.get("void_over_shell")
            ns = row.get("n_szvk_rows")
            nr = row.get("n_shell_rows")
            if sa is None:
                print(
                    f"{m}  (no shell or no precincts)  rows_szvk={ns} rows_shell={nr}"
                )
                continue
            oa_f = float(oa) if oa is not None else float("nan")
            os_f = float(os_) if os_ is not None else float("nan")
            print(
                f"{m}  {sa / 1e6:8.2f}  {pa / 1e6:8.2f}  {oa_f / 1e6:8.2f}  {va / 1e6:8.2f}  "
                f"{os_f:10.4f}  {vs:10.4f}  {ns:6d}  {nr:12d}"
            )
        if src0.get("maz_in_precincts_not_in_shell"):
            print(
                "\nmaz in precincts but missing from shell:",
                src0["maz_in_precincts_not_in_shell"],
            )
        if src0.get("maz_in_shell_not_in_precincts"):
            print(
                "maz in shell but no precinct rows:",
                src0["maz_in_shell_not_in_precincts"],
            )

    if args.out_json is not None:
        out = args.out_json
        if not out.is_absolute():
            out = (repo_root / out).resolve()
        out.parent.mkdir(parents=True, exist_ok=True)

        def _json_safe(o: Any) -> Any:
            if isinstance(o, dict):
                return {k: _json_safe(v) for k, v in o.items()}
            if isinstance(o, list):
                return [_json_safe(v) for v in o]
            if isinstance(o, float):
                return o if o == o else None  # nan -> null
            return o

        out.write_text(
            json.dumps(_json_safe(payload), indent=2) + "\n",
            encoding="utf-8",
        )
        print(f"\nWrote {out}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
