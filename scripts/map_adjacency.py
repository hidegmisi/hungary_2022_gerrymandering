#!/usr/bin/env python3
"""Interactive Folium map: precinct polygons (optional) and centroid–centroid adjacency edges.

Requires optional dependencies: ``uv sync --extra viz``

Example (single county, default caps)::

    uv sync --extra viz
    uv run python scripts/map_adjacency.py --maz 01 --out data/processed/graph/adjacency_map.html
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

from hungary_ge.config import ProcessedPaths
from hungary_ge.graph import AdjacencyBuildOptions, adjacency_summary, build_adjacency
from hungary_ge.io import load_processed_geoparquet
from hungary_ge.problem import OevkProblem, prepare_precinct_layer


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Folium map of precinct adjacency (centroid links).",
    )
    parser.add_argument(
        "--parquet",
        type=Path,
        default=Path("data/processed/precincts.parquet"),
        help="Precinct GeoParquet path",
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
        help="Keep only this two-digit county code (e.g. 01). Recommended for national data.",
    )
    parser.add_argument(
        "--max-features",
        type=int,
        default=5000,
        help="Max precinct rows after filters (safety cap).",
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
        help="Contiguity rule when --fuzzy is not set (libpysal Queen/Rook)",
    )
    parser.add_argument(
        "--fuzzy",
        action="store_true",
        help="Use libpysal fuzzy_contiguity instead of Queen/Rook",
    )
    parser.add_argument(
        "--fuzzy-buffering",
        action="store_true",
        help="With --fuzzy: buffer geometries in fuzzy_metric_crs (meters) to close small gaps",
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
        help="With --fuzzy: fixed buffer distance in meters (overrides tolerance-based buffer)",
    )
    parser.add_argument(
        "--fuzzy-metric-crs",
        type=str,
        default="EPSG:32633",
        help="With --fuzzy --fuzzy-buffering: projected CRS for buffering (default UTM 33N)",
    )
    parser.add_argument(
        "--no-polygons",
        action="store_true",
        help="Do not add precinct polygons to the map (faster/smaller HTML).",
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
    pq = args.parquet
    if not pq.is_absolute():
        pq = (repo_root / pq).resolve()
    if not pq.is_file():
        print(f"Missing precinct layer: {pq}", file=sys.stderr)  # noqa: T201
        return 1

    gdf = load_processed_geoparquet(pq)
    if args.maz is not None and "maz" in gdf.columns:
        gdf = gdf[gdf["maz"].astype(str) == args.maz].copy()
    if len(gdf) > args.max_features:
        gdf = gdf.iloc[: args.max_features].copy()

    prob = OevkProblem(
        county_column=None,
        pop_column=None,
        crs="EPSG:4326",
    )
    gdf2, pmap = prepare_precinct_layer(gdf, prob)
    if args.fuzzy:
        adj_opts = AdjacencyBuildOptions(
            fuzzy=True,
            fuzzy_buffering=args.fuzzy_buffering,
            fuzzy_tolerance=args.fuzzy_tolerance,
            fuzzy_buffer_m=args.fuzzy_buffer_m,
            fuzzy_metric_crs=args.fuzzy_metric_crs,
        )
    else:
        adj_opts = AdjacencyBuildOptions(contiguity=args.contiguity)
    graph = build_adjacency(
        gdf2,
        prob,
        pmap,
        options=adj_opts,
    )
    summ = adjacency_summary(graph)
    print(summ)  # noqa: T201

    gdf_metric = gdf2.to_crs(3857)
    centroids_wgs = gdf_metric.geometry.centroid.to_crs(4326)
    lats = centroids_wgs.y.to_numpy()
    lons = centroids_wgs.x.to_numpy()

    m = folium.Map(
        location=[float(lats.mean()), float(lons.mean())],
        zoom_start=10,
        tiles="cartodbpositron",
    )

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

    if not args.no_polygons:
        gj = folium.GeoJson(
            gdf2.to_json(),
            style_function=lambda _f: {
                "fillColor": "#eeeeee",
                "color": "#888888",
                "weight": 0.5,
                "fillOpacity": 0.2,
            },
        )
        gj.add_to(m)

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
