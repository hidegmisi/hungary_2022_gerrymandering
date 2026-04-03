"""Write adjacency Parquet + metadata (national or single-county slice)."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

from hungary_ge.config import ProcessedPaths
from hungary_ge.graph import adjacency_summary
from hungary_ge.graph.adjacency_io import save_adjacency
from hungary_ge.io import load_processed_geoparquet
from hungary_ge.pipeline.county_allocation import normalize_maz
from hungary_ge.pipeline.graph_build import (
    adjacency_options_from_graph_cli,
    build_precinct_adjacency,
)
from hungary_ge.problem import OevkProblem


def run_graph_export(
    repo_root: Path,
    parquet: Path,
    args: Any,
    *,
    maz_filter: str | None = None,
    edges_parquet: Path | None = None,
    log_prefix: str = "",
    county_run_id: str | None = None,
    strict_county_connectivity: bool = True,
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

    national_scope = maz_filter is None
    if national_scope:
        if "maz" not in gdf.columns:
            print(
                f"{log_prefix}national graph requires a 'maz' column "
                "(county-merge adjacency strategy)",
                file=sys.stderr,
            )
            return 1
        try:
            graph, _gdf_layer, adj_opts = build_precinct_adjacency(
                gdf,
                prob,
                national_county_merge=True,
                national_fuzzy_tolerance=args.graph_fuzzy_tolerance,
                national_fuzzy_buffer_m=args.graph_fuzzy_buffer_m,
                national_fuzzy_metric_crs=args.graph_fuzzy_metric_crs,
            )
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

    county_opts = adjacency_options_from_graph_cli(args)
    try:
        graph, _gdf_layer, adj_opts = build_precinct_adjacency(
            gdf,
            prob,
            national_county_merge=False,
            national_fuzzy_tolerance=0.0,
            national_fuzzy_buffer_m=None,
            national_fuzzy_metric_crs=args.graph_fuzzy_metric_crs,
            county_adj_opts=county_opts,
        )
    except ValueError as exc:
        print(f"{log_prefix}graph build failed: {exc}", file=sys.stderr)
        return 1
    summ = adjacency_summary(graph)
    print(f"{log_prefix}{summ}")  # noqa: T201
    assert maz_filter is not None
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
