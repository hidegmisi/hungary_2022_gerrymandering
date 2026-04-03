#!/usr/bin/env python3
"""Interactive Folium map: precinct polygons (optional) and centroid–centroid adjacency edges.

Requires optional dependencies: ``uv sync --extra viz``

Example (single county, default caps)::

    uv sync --extra viz
    uv run python scripts/map_adjacency.py --maz 01 --out data/processed/graph/adjacency_map.html

National (no ``--maz``): builds adjacency **per county** plus bicounty cross edges — see
``hungary_ge.graph.national_adjacency``. Default national build uses **fuzzy** buffering
(3 m). Use ``--national-topological`` for pure Queen/Rook (no buffer / void correction).
The layer must include a ``maz`` column.
Prefer ``data/processed/precincts_void_hex.parquet`` when present (auto-picked
before ``precincts.parquet``); pass ``--parquet data/processed/precincts.parquet`` to
avoid void-hex / gap rows.

Polygons are drawn from the GeoParquet as stored (WGS84); void hex sizing and filters
are applied in ``build_precinct_layer`` / ``gaps_hex``, not in this script.

County (megye) outlines default to ``data/raw/admin`` (per-county ``NN.geojson``, black
thick stroke). Filtered to ``--maz`` when set. Use ``--no-county-borders`` to omit.
"""

from __future__ import annotations

import argparse
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
from folium.map import CustomPane

from hungary_ge.config import ProcessedPaths
from hungary_ge.graph import AdjacencyBuildOptions, adjacency_summary
from hungary_ge.io import (
    GapShellSource,
    load_processed_geoparquet,
    read_shell_gdf,
)
from hungary_ge.pipeline.county_allocation import normalize_maz
from hungary_ge.pipeline.graph_build import (
    adjacency_options_from_map_adjacency_args,
    build_precinct_adjacency,
)
from hungary_ge.problem import OevkProblem


def _default_parquet_path(repo_root: Path) -> Path:
    void_hex = repo_root / "data/processed/precincts_void_hex.parquet"
    plain = repo_root / "data/processed/precincts.parquet"
    if void_hex.is_file():
        return void_hex
    return plain


def _default_county_borders_path(repo_root: Path) -> Path:
    return repo_root / "data/raw/admin"


def _county_border_gdf_for_map(
    path: Path,
    maz: str | None,
) -> gpd.GeoDataFrame | None:
    """County/megye shells for Folium; filter to ``maz`` when set."""
    if not path.is_file() and not path.is_dir():
        return None
    try:
        g = read_shell_gdf(GapShellSource(path=path, maz_column="maz"))
    except ValueError as exc:
        print(  # noqa: T201
            f"County borders could not be loaded from {path}: {exc}",
            file=sys.stderr,
        )
        return None
    if maz is not None:
        if "maz" not in g.columns:
            print(  # noqa: T201
                f"County borders layer has no 'maz' column; skipping: {path}",
                file=sys.stderr,
            )
            return None
        mzn = normalize_maz(maz)
        g = g[g["maz"].map(normalize_maz) == mzn].copy()
    if g.empty:
        return None
    if g.crs is not None:
        g = g.to_crs(4326)
    return g


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Folium map of precinct adjacency (centroid links).",
    )
    parser.add_argument(
        "--parquet",
        type=Path,
        default=None,
        help=(
            "Precinct GeoParquet path "
            "(default: precincts_void_hex.parquet if present, else precincts.parquet)"
        ),
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=Path.cwd(),
        help="Repository root for default paths",
    )
    parser.add_argument(
        "--maz",
        type=str,
        default=None,
        help="Keep only this two-digit county code (e.g. 01). Omit for national map.",
    )
    parser.add_argument(
        "--max-features",
        type=int,
        default=5000,
        help="Cap rows after filters. National merge mode refuses truncation (use >= n_rows).",
    )
    parser.add_argument(
        "--max-edges",
        type=int,
        default=50000,
        help="Max adjacency edges to draw as lines.",
    )
    parser.add_argument(
        "--contiguity",
        choices=("queen", "rook"),
        default="queen",
        help="Queen/Rook for single-county maps when --fuzzy is not set",
    )
    parser.add_argument(
        "--fuzzy",
        action="store_true",
        help="Use libpysal fuzzy_contiguity instead of Queen/Rook (single-county / legacy national)",
    )
    parser.add_argument(
        "--fuzzy-buffering",
        action="store_true",
        help="With --fuzzy: buffer geometries in fuzzy_metric_crs (meters)",
    )
    parser.add_argument(
        "--fuzzy-tolerance",
        type=float,
        default=0.005,
        help="With --fuzzy: libpysal tolerance when buffer distance is derived from bbox",
    )
    parser.add_argument(
        "--fuzzy-buffer-m",
        type=float,
        default=None,
        help="With --fuzzy: fixed buffer distance in meters (national merge default: 3)",
    )
    parser.add_argument(
        "--fuzzy-metric-crs",
        type=str,
        default="EPSG:32633",
        help="With --fuzzy --fuzzy-buffering: projected CRS for buffering (default UTM 33N)",
    )
    parser.add_argument(
        "--county-borders",
        type=Path,
        default=None,
        help=(
            "GeoJSON of county (megye) polygons for outlines "
            "(default: data/raw/admin directory of per-county GeoJSON)"
        ),
    )
    parser.add_argument(
        "--no-county-borders",
        action="store_true",
        help="Do not draw county boundary outlines.",
    )
    parser.add_argument(
        "--no-polygons",
        action="store_true",
        help="Do not add precinct polygons to the map (faster/smaller HTML).",
    )
    parser.add_argument(
        "--no-gaps",
        action="store_true",
        help="If layer has unit_kind, draw szvk polygons only (hide void/gap features).",
    )
    parser.add_argument(
        "--national-topological",
        action="store_true",
        help=(
            "National map only: build county-merged graph with --contiguity queen/rook "
            "(no fuzzy buffering)."
        ),
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=None,
        help="Output HTML path (default: data/processed/graph/adjacency_map.html)",
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

    gdf = load_processed_geoparquet(pq)
    national_merge = args.maz is None
    if national_merge:
        if "maz" not in gdf.columns:
            print(
                "National county-merge mode requires a 'maz' column on the layer.",
                file=sys.stderr,
            )
            return 1
        if len(gdf) > args.max_features:
            print(
                f"National county-merge uses the full layer ({len(gdf)} rows > "
                f"--max-features {args.max_features}). Raise --max-features.",
                file=sys.stderr,
            )
            return 1
    else:
        if args.maz is not None and "maz" in gdf.columns:
            gdf = gdf[gdf["maz"].astype(str) == args.maz].copy()
        if len(gdf) > args.max_features:
            gdf = gdf.iloc[: args.max_features].copy()

    prob = OevkProblem(
        county_column=None,
        pop_column=None,
        crs="EPSG:4326",
    )

    if national_merge:
        nat_opts = None
        if args.national_topological:
            nat_opts = AdjacencyBuildOptions(contiguity=args.contiguity)
        graph, gdf2, _adj_opts = build_precinct_adjacency(
            gdf,
            prob,
            national_county_merge=True,
            national_fuzzy_tolerance=args.fuzzy_tolerance,
            national_fuzzy_buffer_m=args.fuzzy_buffer_m,
            national_fuzzy_metric_crs=args.fuzzy_metric_crs,
            national_adj_opts=nat_opts,
        )
    else:
        county_opts = adjacency_options_from_map_adjacency_args(args)
        graph, gdf2, _adj_opts = build_precinct_adjacency(
            gdf,
            prob,
            national_county_merge=False,
            national_fuzzy_tolerance=0.0,
            national_fuzzy_buffer_m=None,
            national_fuzzy_metric_crs=args.fuzzy_metric_crs,
            county_adj_opts=county_opts,
        )
    summ = adjacency_summary(graph)
    print(summ)  # noqa: T201

    id_key = graph.order.id_column
    island_ids = frozenset(str(graph.order.id_at(i)) for i in graph.island_nodes)
    if island_ids:
        print(  # noqa: T201
            f"{len(island_ids)} island unit(s) (no graph neighbors) — thick red outline on map",
        )

    def _style_szvk(feature: dict) -> dict:
        props = feature.get("properties") or {}
        pid = props.get(id_key)
        if pid is not None and str(pid) in island_ids:
            return {
                "fillColor": "#ffc8c8",
                "color": "#b00000",
                "weight": 5,
                "fillOpacity": 0.45,
            }
        return {
            "fillColor": "#eeeeee",
            "color": "#888888",
            "weight": 0.5,
            "fillOpacity": 0.25,
        }

    def _style_void(feature: dict) -> dict:
        props = feature.get("properties") or {}
        pid = props.get(id_key)
        if pid is not None and str(pid) in island_ids:
            return {
                "fillColor": "#ff9f1c",
                "color": "#b00000",
                "weight": 5,
                "fillOpacity": 0.5,
                "dashArray": "4 3",
            }
        return {
            "fillColor": "#ff9f1c",
            "color": "#cc7000",
            "weight": 1.0,
            "fillOpacity": 0.35,
            "dashArray": "4 3",
        }

    def _style_plain(feature: dict) -> dict:
        props = feature.get("properties") or {}
        pid = props.get(id_key)
        if pid is not None and str(pid) in island_ids:
            return {
                "fillColor": "#ffc8c8",
                "color": "#b00000",
                "weight": 5,
                "fillOpacity": 0.35,
            }
        return {
            "fillColor": "#eeeeee",
            "color": "#888888",
            "weight": 0.5,
            "fillOpacity": 0.2,
        }

    def _style_county_border(_feature: dict) -> dict:
        return {
            "fillOpacity": 0,
            "fillColor": "#000000",
            "color": "#000000",
            "weight": 5,
            "opacity": 1.0,
        }

    gdf_metric = gdf2.to_crs(3857)
    centroids_wgs = gdf_metric.geometry.centroid.to_crs(4326)
    lats = centroids_wgs.y.to_numpy()
    lons = centroids_wgs.x.to_numpy()

    m = folium.Map(
        location=[float(lats.mean()), float(lons.mean())],
        zoom_start=10,
        tiles="cartodbpositron",
    )
    # Render county outlines above precinct fills (same overlay pane stacks poorly otherwise).
    CustomPane("countyMegyeBorders", z_index=670, pointer_events=False).add_to(m)

    n_draw = 0
    for i in range(graph.n_nodes):
        for j in graph.neighbor_lists[i]:
            if i >= j:
                continue
            if n_draw >= args.max_edges:
                break
            lat1, lon1 = float(lats[i]), float(lons[i])
            lat2, lon2 = float(lats[j]), float(lons[j])
            folium.PolyLine(
                [(lat1, lon1), (lat2, lon2)],
                color="#3388ff",
                weight=1,
                opacity=0.6,
            ).add_to(m)
            n_draw += 1
        if n_draw >= args.max_edges:
            break

    has_void_layer = False
    county_fg_added = False

    if not args.no_polygons:
        fg_szvk = folium.FeatureGroup(name="Precincts (szvk)", show=True)
        has_void_layer = (
            not args.no_gaps
            and "unit_kind" in gdf2.columns
            and (gdf2["unit_kind"].astype(str) == "void").any()
        )
        if has_void_layer:
            is_void = gdf2["unit_kind"].astype(str) == "void"
            szvk_part = gdf2[~is_void]
            void_part = gdf2[is_void]
            folium.GeoJson(szvk_part.to_json(), style_function=_style_szvk).add_to(
                fg_szvk
            )
            fg_void = folium.FeatureGroup(name="Gap (void)", show=True)
            folium.GeoJson(void_part.to_json(), style_function=_style_void).add_to(
                fg_void
            )
            fg_szvk.add_to(m)
            fg_void.add_to(m)
        else:
            plot_gdf = gdf2
            if args.no_gaps and "unit_kind" in gdf2.columns:
                plot_gdf = gdf2[gdf2["unit_kind"].astype(str) != "void"]
            folium.GeoJson(plot_gdf.to_json(), style_function=_style_plain).add_to(
                fg_szvk
            )
            fg_szvk.add_to(m)

    if not args.no_county_borders:
        cpath = args.county_borders
        if cpath is None:
            cpath = _default_county_borders_path(repo_root)
        elif not cpath.is_absolute():
            cpath = (repo_root / cpath).resolve()
        c_gdf = _county_border_gdf_for_map(cpath, args.maz)
        if c_gdf is not None:
            fg_counties = folium.FeatureGroup(name="County borders", show=True)
            folium.GeoJson(
                c_gdf.to_json(),
                style_function=_style_county_border,
                pane="countyMegyeBorders",
            ).add_to(fg_counties)
            fg_counties.add_to(m)
            county_fg_added = True
        elif cpath.is_file() or cpath.is_dir():
            print(  # noqa: T201
                f"No county features to draw after filter (maz={args.maz!r}): {cpath}",
                file=sys.stderr,
            )

    if has_void_layer or (county_fg_added and not args.no_polygons):
        folium.LayerControl(collapsed=False).add_to(m)

    m.fit_bounds([[lats.min(), lons.min()], [lats.max(), lons.max()]])

    out = args.out
    if out is None:
        out = paths.graph_dir / "adjacency_map.html"
    elif not out.is_absolute():
        out = (repo_root / out).resolve()
    out.parent.mkdir(parents=True, exist_ok=True)
    m.save(str(out))
    print(f"Wrote {out} ({n_draw} edges drawn)")  # noqa: T201
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
