"""Precinct adjacency / dual graph (ALARM contiguity input)."""

from __future__ import annotations

from typing import Any

from geopandas import GeoDataFrame
from libpysal.weights import Queen, Rook, fuzzy_contiguity

from hungary_ge.graph.adjacency_graph import (
    AdjacencyBuildOptions,
    AdjacencyGraph,
    from_libpysal_w,
)
from hungary_ge.problem import OevkProblem, PrecinctIndexMap


def build_adjacency(
    gdf: GeoDataFrame,
    problem: OevkProblem,
    index_map: PrecinctIndexMap,
    *,
    options: AdjacencyBuildOptions | None = None,
) -> AdjacencyGraph:
    """Build a contiguity graph from precinct geometries.

    Row ``i`` of ``gdf`` must match :meth:`PrecinctIndexMap.id_at` for the same
    ``i``. Use :func:`~hungary_ge.problem.precinct_index_map.prepare_precinct_layer`
    to obtain an aligned frame and map.

    Args:
        gdf: Precinct GeoDataFrame (e.g. from :func:`hungary_ge.io.load_processed_geoparquet`).
        problem: Uses ``precinct_id_column`` for alignment checks only.
        index_map: Canonical precinct order and ids.
        options: ``queen`` / ``rook``, or ``fuzzy`` (libpysal ``fuzzy_contiguity``).

    Returns:
        Immutable :class:`~hungary_ge.graph.adjacency_graph.AdjacencyGraph`.
    """
    opts = options if options is not None else AdjacencyBuildOptions()
    col = problem.precinct_id_column
    got = list(gdf[col].astype(str))
    exp = list(index_map.ids)
    if got != exp:
        msg = "GeoDataFrame precinct id order does not match PrecinctIndexMap"
        raise ValueError(msg)
    if len(gdf) != index_map.n_units:
        msg = "GeoDataFrame length does not match PrecinctIndexMap"
        raise ValueError(msg)

    if opts.fuzzy:
        gdf_w = gdf.to_crs(opts.fuzzy_metric_crs) if opts.fuzzy_buffering else gdf
        fc_kw: dict[str, Any] = {
            "tolerance": opts.fuzzy_tolerance,
            "buffering": opts.fuzzy_buffering,
            "drop": True,
            "predicate": opts.fuzzy_predicate,
        }
        if opts.fuzzy_buffer_m is not None:
            fc_kw["buffer"] = opts.fuzzy_buffer_m
        w = fuzzy_contiguity(gdf_w, **fc_kw)
        contiguity_label = "fuzzy:buffered" if opts.fuzzy_buffering else "fuzzy:intersect"
        return from_libpysal_w(w, index_map, contiguity_label)

    if opts.contiguity == "queen":
        w = Queen.from_dataframe(gdf, use_index=False)
    elif opts.contiguity == "rook":
        w = Rook.from_dataframe(gdf, use_index=False)
    else:
        msg = f"unknown contiguity: {opts.contiguity!r}"
        raise ValueError(msg)

    return from_libpysal_w(w, index_map, opts.contiguity)
