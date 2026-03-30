"""Precinct adjacency / dual graph (ALARM contiguity input)."""

from __future__ import annotations

from typing import Any

from hungary_ge.problem import OevkProblem

GeoDataFrame = Any


def build_adjacency(
    gdf: GeoDataFrame,
    problem: OevkProblem,
    *,
    contiguity: str = "queen",
) -> object:
    """Build adjacency structure from precinct geometries.

    Args:
        gdf: Precinct GeoDataFrame (e.g. from :func:`hungary_ge.io.load_processed_geojson`).
        problem: Problem specification (uses ``precinct_id_column``).
        contiguity: ``queen`` or ``rook`` (shared boundary / shared vertex).

    Returns:
        Graph or sparse matrix (format TBD; e.g. NetworkX or scipy.sparse).
    """
    raise NotImplementedError(
        "build_adjacency: implement via libpysal.weights or equivalent"
    )
