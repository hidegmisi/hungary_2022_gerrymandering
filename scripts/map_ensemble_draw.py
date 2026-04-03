#!/usr/bin/env python3
"""Folium map: enacted OEVK (focal) and simulated ensemble draw(s) on precinct polygons.

Requires ``uv sync --extra viz``.

Example::

    uv sync --extra viz
    uv run python scripts/map_ensemble_draw.py \\
        --repo-root . --run-id MYRUN --maz 01 --draw 1 \\
        --out data/processed/runs/MYRUN/counties/01/ensemble/preview.html

Use ``--ensemble-parquet`` instead of ``--run-id`` when the assignments file
lives elsewhere; then pass ``--maz`` and ``--ndists`` unless they appear in the
ensemble ``.meta.json`` (``county_maz``, ``county_ndists``).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

try:
    import folium
except ImportError:
    print(  # noqa: T201
        "folium is required: uv sync --extra viz",
        file=sys.stderr,
    )
    raise SystemExit(1) from None

import geopandas as gpd
import numpy as np
from folium.map import CustomPane

from hungary_ge.config import (
    ENSEMBLE_ASSIGNMENTS_PARQUET,
    ProcessedPaths,
)
from hungary_ge.ensemble.persistence import load_plan_ensemble_draw_column
from hungary_ge.io import load_focal_assignments, load_processed_geoparquet
from hungary_ge.pipeline.county_allocation import normalize_maz
from hungary_ge.pipeline.county_sample import county_ndists_by_maz
from hungary_ge.problem import (
    DEFAULT_PRECINCT_ID_COLUMN,
    OevkProblem,
    prepare_precinct_layer,
)
from hungary_ge.viz.plan_assignments import (
    merge_enacted_districts,
    merge_simulated_districts,
)

ID_COL = DEFAULT_PRECINCT_ID_COLUMN


def _default_parquet_path(repo_root: Path) -> Path:
    void_hex = repo_root / "data/processed/precincts_void_hex.parquet"
    plain = repo_root / "data/processed/precincts.parquet"
    if void_hex.is_file():
        return void_hex
    return plain


def _default_county_borders_path(repo_root: Path) -> Path:
    return repo_root / "data/raw/admin/hu_megye_shell_maz.geojson"


def _county_border_gdf_for_map(
    path: Path,
    maz: str | None,
) -> gpd.GeoDataFrame | None:
    if not path.is_file():
        return None
    g = gpd.read_file(path)
    if maz is not None:
        if "maz" not in g.columns:
            print(  # noqa: T201
                f"County borders file has no 'maz' column; skipping: {path}",
                file=sys.stderr,
            )
            return None
        g = g[g["maz"].astype(str) == str(maz)].copy()
    if g.empty:
        return None
    if g.crs is not None:
        g = g.to_crs(4326)
    return g


def _load_manifest(ep_path: Path) -> dict | None:
    meta_path = ep_path.with_suffix(".meta.json")
    if not meta_path.is_file():
        return None
    return json.loads(meta_path.read_text(encoding="utf-8"))


def _stable_fill_color(prefix: bytes, label: str) -> str:
    h = hashlib.md5(prefix + label.encode(), usedforsecurity=False).hexdigest()
    return f"#{h[:6]}"


def _style_void_neutral() -> dict:
    return {
        "fillColor": "#dddddd",
        "color": "#999999",
        "weight": 0.6,
        "fillOpacity": 0.2,
        "dashArray": "4 3",
    }


def _parse_draw_ids(draw: int | None, draws_csv: str | None) -> list[int]:
    out: list[int] = []
    if draw is not None:
        out.append(int(draw))
    if draws_csv:
        for part in draws_csv.split(","):
            p = part.strip()
            if p:
                out.append(int(p))
    if not out:
        msg = "pass --draw and/or --draws with at least one draw label"
        raise ValueError(msg)
    # preserve order, unique
    seen: set[int] = set()
    ordered: list[int] = []
    for d in out:
        if d not in seen:
            seen.add(d)
            ordered.append(d)
    return ordered


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Folium choropleth: focal enacted OEVK + simulated draw(s).",
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=Path.cwd(),
        help="Repository root for default paths",
    )
    parser.add_argument(
        "--parquet",
        type=Path,
        default=None,
        help="Precinct GeoParquet (default: void_hex if present else precincts.parquet)",
    )
    parser.add_argument(
        "--run-id",
        type=str,
        default=None,
        help="Run id under data/processed/runs/<id>/ (with --maz for ensemble path)",
    )
    parser.add_argument(
        "--maz",
        type=str,
        default=None,
        help="Two-digit megye code; required unless inferable from ensemble manifest",
    )
    parser.add_argument(
        "--ensemble-parquet",
        type=Path,
        default=None,
        help="Path to ensemble_assignments.parquet (overrides --run-id county path)",
    )
    parser.add_argument(
        "--ndists",
        type=int,
        default=None,
        help="District count for OevkProblem when not using --run-id counts table",
    )
    parser.add_argument(
        "--draw",
        type=int,
        default=None,
        help="Single draw label (Parquet ``draw`` column)",
    )
    parser.add_argument(
        "--draws",
        type=str,
        default=None,
        help="Comma-separated draw labels (e.g. 1,2,5)",
    )
    parser.add_argument(
        "--pop-column",
        type=str,
        default="voters",
        help="Population column on precinct layer (match pipeline --sample-pop-column)",
    )
    parser.add_argument(
        "--focal-parquet",
        type=Path,
        default=None,
        help="focal_oevk_assignments.parquet (default: data/processed/...)",
    )
    parser.add_argument(
        "--no-enacted-layer",
        action="store_true",
        help="Do not add enacted focal choropleth (no focal file required)",
    )
    parser.add_argument(
        "--focal-allow-missing",
        action="store_true",
        help=(
            "Allow szvk rows with no focal row (e.g. missing oevk in raw JSON); "
            "style them like void gaps on the enacted layer"
        ),
    )
    parser.add_argument(
        "--no-county-borders",
        action="store_true",
        help="Do not draw megye outline",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=None,
        help="Output HTML (default: next to ensemble parquet as ensemble_map.html)",
    )
    args = parser.parse_args()

    repo_root = args.repo_root.resolve()
    paths = ProcessedPaths(repo_root)

    if args.parquet is None:
        pq = _default_parquet_path(repo_root)
    else:
        pq = args.parquet
        if not pq.is_absolute():
            pq = (repo_root / pq).resolve()
    if not pq.is_file():
        print(f"Missing precinct layer: {pq}", file=sys.stderr)  # noqa: T201
        return 1

    ensemble_parquet: Path
    if args.ensemble_parquet is not None:
        ensemble_parquet = args.ensemble_parquet.resolve()
        if not ensemble_parquet.is_absolute():
            ensemble_parquet = (repo_root / ensemble_parquet).resolve()
    else:
        if not args.run_id or args.maz is None:
            print(  # noqa: T201
                "Provide --ensemble-parquet or both --run-id and --maz.",
                file=sys.stderr,
            )
            return 1
        ensemble_parquet = (
            paths.county_ensemble_dir(args.run_id, args.maz)
            / ENSEMBLE_ASSIGNMENTS_PARQUET
        )
    if not ensemble_parquet.is_file():
        print(f"Missing ensemble parquet: {ensemble_parquet}", file=sys.stderr)  # noqa: T201
        return 1

    meta = _load_manifest(ensemble_parquet)
    meta_inner = dict(meta.get("metadata", {})) if meta else {}

    maz_n: str
    if args.maz is not None:
        maz_n = normalize_maz(args.maz)
    elif meta_inner.get("county_maz") is not None:
        maz_n = normalize_maz(str(meta_inner["county_maz"]))
    else:
        print(  # noqa: T201
            "Could not determine county: pass --maz or use ensemble manifest with county_maz.",
            file=sys.stderr,
        )
        return 1

    ndists: int | None = args.ndists
    if ndists is None and args.run_id:
        counts_path = paths.county_oevk_counts_parquet(args.run_id)
        if counts_path.is_file():
            nmap = county_ndists_by_maz(counts_path)
            ndists = nmap.get(maz_n)
    if ndists is None and meta_inner.get("county_ndists") is not None:
        ndists = int(meta_inner["county_ndists"])
    if ndists is None:
        print(  # noqa: T201
            "Could not determine ndists: pass --ndists or use --run-id with "
            "county_oevk_counts.parquet / manifest county_ndists.",
            file=sys.stderr,
        )
        return 1

    try:
        draw_labels = _parse_draw_ids(args.draw, args.draws)
    except ValueError as e:
        print(str(e), file=sys.stderr)  # noqa: T201
        return 1

    gdf = load_processed_geoparquet(pq)
    if "maz" not in gdf.columns:
        print("Precinct layer has no 'maz' column.", file=sys.stderr)  # noqa: T201
        return 1
    mzn = gdf["maz"].map(normalize_maz)
    county_gdf = gdf[mzn == maz_n].copy()
    if county_gdf.empty:
        print(f"No precinct rows for maz={maz_n!r}", file=sys.stderr)  # noqa: T201
        return 1

    if args.pop_column not in county_gdf.columns:
        print(  # noqa: T201
            f"Precinct layer has no population column {args.pop_column!r}.",
            file=sys.stderr,
        )
        return 1

    prob = OevkProblem(
        ndists=int(ndists),
        precinct_id_column=ID_COL,
        county_column=None,
        pop_column=args.pop_column,
        crs="EPSG:4326",
    )
    gdf2, _pmap = prepare_precinct_layer(county_gdf, prob)

    focal_path: Path | None = None
    if not args.no_enacted_layer:
        if args.focal_parquet is not None:
            focal_path = args.focal_parquet
            if not focal_path.is_absolute():
                focal_path = (repo_root / focal_path).resolve()
        else:
            focal_path = paths.focal_oevk_assignments_parquet.resolve()
        if not focal_path.is_file():
            print(  # noqa: T201
                f"Missing focal assignments (--focal-parquet or default): {focal_path}\n"
                "Use --no-enacted-layer to map simulated draws only.",
                file=sys.stderr,
            )
            return 1

    out_path = args.out
    if out_path is None:
        out_path = ensemble_parquet.parent / "ensemble_map.html"
    elif not out_path.is_absolute():
        out_path = (repo_root / out_path).resolve()

    # Map center / bounds from prepared geometries
    b = gdf2.total_bounds
    center_lat = float((b[1] + b[3]) / 2)
    center_lon = float((b[0] + b[2]) / 2)

    m = folium.Map(
        location=(center_lat, center_lon), zoom_start=10, tiles="CartoDB positron"
    )
    CustomPane("countyMegyeBorders", z_index=650).add_to(m)

    if not args.no_county_borders:
        cpath = _default_county_borders_path(repo_root)
        c_gdf = _county_border_gdf_for_map(cpath, maz_n)
        if c_gdf is not None:
            folium.GeoJson(
                c_gdf.to_json(),
                style_function=lambda _f: {
                    "fillOpacity": 0,
                    "fillColor": "#000000",
                    "color": "#000000",
                    "weight": 5,
                    "opacity": 1.0,
                },
                pane="countyMegyeBorders",
            ).add_to(m)

    manifest_path = ensemble_parquet.with_suffix(".meta.json")
    layer_names: list[str] = []

    if focal_path is not None:
        focal_all = load_focal_assignments(focal_path)
        pids = set(gdf2[ID_COL].astype(str))
        focal_sub = focal_all[focal_all[ID_COL].astype(str).isin(pids)].copy()
        gdf_focal = merge_enacted_districts(
            gdf2,
            focal_sub,
            precinct_id_column=ID_COL,
            require_all_szvk=not args.focal_allow_missing,
        )

        def _style_enacted(feature: dict) -> dict:
            props = feature.get("properties") or {}
            uk = props.get("unit_kind")
            if uk is not None and str(uk) == "void":
                return _style_void_neutral()
            v = props.get("enacted_oevk_full")
            if v is None or (isinstance(v, float) and np.isnan(v)):
                return _style_void_neutral()
            fill = _stable_fill_color(b"enact:", str(v))
            return {
                "fillColor": fill,
                "color": "#222222",
                "weight": 0.35,
                "fillOpacity": 0.52,
            }

        fg_en = folium.FeatureGroup(name="Enacted OEVK", show=True)
        folium.GeoJson(
            gdf_focal.to_json(),
            style_function=_style_enacted,
        ).add_to(fg_en)
        fg_en.add_to(m)
        layer_names.append("Enacted OEVK")

    for d_label in draw_labels:
        try:
            dist_col = load_plan_ensemble_draw_column(
                ensemble_parquet,
                int(d_label),
                manifest_path=manifest_path if manifest_path.is_file() else None,
            )
        except ValueError as e:
            print(f"draw {d_label}: {e}", file=sys.stderr)  # noqa: T201
            return 1
        meta_for_ids = meta or {}
        unit_ids_t = tuple(str(x) for x in meta_for_ids.get("unit_ids", []))
        if not unit_ids_t:
            print("Ensemble manifest missing unit_ids.", file=sys.stderr)  # noqa: T201
            return 1
        gdf_d = merge_simulated_districts(
            gdf2,
            precinct_id_column=ID_COL,
            unit_ids=unit_ids_t,
            districts=dist_col,
        )

        def _make_style_sim() -> object:
            key = "sim_district"

            def _style_sim(feature: dict) -> dict:
                props = feature.get("properties") or {}
                uk = props.get("unit_kind")
                if uk is not None and str(uk) == "void":
                    return _style_void_neutral()
                raw = props.get(key)
                if raw is None:
                    return _style_void_neutral()
                label = str(int(float(raw)))
                fill = _stable_fill_color(b"sim:", label)
                return {
                    "fillColor": fill,
                    "color": "#333333",
                    "weight": 0.4,
                    "fillOpacity": 0.48,
                }

            return _style_sim

        fg = folium.FeatureGroup(name=f"Simulated draw {d_label}", show=False)
        folium.GeoJson(
            gdf_d.to_json(),
            style_function=_make_style_sim(),
        ).add_to(fg)
        fg.add_to(m)
        layer_names.append(f"Draw {d_label}")

    if len(layer_names) > 1:
        folium.LayerControl(collapsed=False).add_to(m)

    m.fit_bounds([[b[1], b[0]], [b[3], b[2]]])
    out_path.parent.mkdir(parents=True, exist_ok=True)
    m.save(str(out_path))
    print(f"Wrote {out_path}")  # noqa: T201
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
