"""Adjacency and contiguity (between ``redist_map`` and sampling in ALARM)."""

from hungary_ge.graph.adjacency import build_adjacency
from hungary_ge.graph.adjacency_graph import (
    AdjacencyBuildOptions,
    AdjacencyGraph,
    adjacency_summary,
)
from hungary_ge.graph.adjacency_io import (
    AdjacencyPatch,
    AdjacencyPatchStats,
    apply_adjacency_patch,
    load_adjacency,
    save_adjacency,
)
from hungary_ge.graph.national_adjacency import (
    build_national_adjacency_merged,
    county_adjacent_maz_pairs,
)

__all__ = [
    "AdjacencyBuildOptions",
    "AdjacencyGraph",
    "AdjacencyPatch",
    "AdjacencyPatchStats",
    "adjacency_summary",
    "apply_adjacency_patch",
    "build_adjacency",
    "build_national_adjacency_merged",
    "county_adjacent_maz_pairs",
    "load_adjacency",
    "save_adjacency",
]
