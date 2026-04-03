"""Shared adjacency construction for pipeline graph stage and Folium map script."""

from __future__ import annotations

from typing import Any

import geopandas as gpd

from hungary_ge.graph import AdjacencyBuildOptions, AdjacencyGraph, build_adjacency
from hungary_ge.graph.national_adjacency import build_national_adjacency_merged
from hungary_ge.problem import OevkProblem, prepare_precinct_layer


def adjacency_options_from_graph_cli(args: Any) -> AdjacencyBuildOptions:
    """Match graph stage contiguity settings from pipeline CLI namespace."""
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


def adjacency_options_from_map_adjacency_args(args: Any) -> AdjacencyBuildOptions:
    """Match ``scripts/map_adjacency.py`` flag names."""
    if args.fuzzy:
        return AdjacencyBuildOptions(
            fuzzy=True,
            fuzzy_buffering=args.fuzzy_buffering,
            fuzzy_tolerance=args.fuzzy_tolerance,
            fuzzy_buffer_m=args.fuzzy_buffer_m,
            fuzzy_metric_crs=args.fuzzy_metric_crs,
        )
    if args.contiguity == "rook":
        return AdjacencyBuildOptions(contiguity="rook")
    return AdjacencyBuildOptions(contiguity="queen")


def build_precinct_adjacency(
    gdf: gpd.GeoDataFrame,
    prob: OevkProblem,
    *,
    national_county_merge: bool,
    national_fuzzy_tolerance: float,
    national_fuzzy_buffer_m: float | None,
    national_fuzzy_metric_crs: str,
    county_adj_opts: AdjacencyBuildOptions | None = None,
    national_adj_opts: AdjacencyBuildOptions | None = None,
) -> tuple[AdjacencyGraph, gpd.GeoDataFrame, AdjacencyBuildOptions]:
    """Build adjacency; national scope uses county-merge graph (needs ``maz`` on ``gdf``).

    When ``national_adj_opts`` is set, it is used for the national merge (e.g. pure
    queen/rook). Otherwise the default is fuzzy buffering (3 m unless
    ``national_fuzzy_buffer_m`` is set).
    """
    if national_county_merge:
        if national_adj_opts is not None:
            adj_opts = national_adj_opts
        else:
            buf_m = national_fuzzy_buffer_m if national_fuzzy_buffer_m is not None else 3.0
            adj_opts = AdjacencyBuildOptions(
                fuzzy=True,
                fuzzy_buffering=True,
                fuzzy_tolerance=national_fuzzy_tolerance,
                fuzzy_buffer_m=buf_m,
                fuzzy_metric_crs=national_fuzzy_metric_crs,
            )
        graph = build_national_adjacency_merged(gdf, prob, adj_opts)
        gdf2, _ = prepare_precinct_layer(gdf, prob)
        return graph, gdf2, adj_opts

    if county_adj_opts is None:
        msg = "county_adj_opts is required when national_county_merge is False"
        raise ValueError(msg)
    gdf2, pmap = prepare_precinct_layer(gdf, prob)
    graph = build_adjacency(gdf2, prob, pmap, options=county_adj_opts)
    return graph, gdf2, county_adj_opts
