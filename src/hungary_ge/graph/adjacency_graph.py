"""Adjacency graph types and summaries (contiguity weights, Slice 3)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

import numpy as np
from scipy.sparse import csr_matrix
from scipy.sparse.csgraph import connected_components

from hungary_ge.problem.precinct_index_map import PrecinctIndexMap

ContiguityKind = Literal["queen", "rook"]


@dataclass(frozen=True)
class AdjacencyBuildOptions:
    """Parameters for :func:`~hungary_ge.graph.adjacency.build_adjacency`."""

    contiguity: ContiguityKind = "queen"


@dataclass(frozen=True)
class AdjacencyGraph:
    """Undirected contiguity graph aligned with :class:`PrecinctIndexMap` row indices.

    Neighbor lists use integer indices ``0 .. n_nodes-1`` matching the prepared
    :class:`~geopandas.GeoDataFrame` row order.
    """

    order: PrecinctIndexMap
    contiguity: str
    neighbor_lists: tuple[tuple[int, ...], ...]
    island_nodes: tuple[int, ...]
    n_components: int
    largest_component_size: int

    @property
    def n_nodes(self) -> int:
        return len(self.neighbor_lists)

    @property
    def n_edges(self) -> int:
        return sum(len(n) for n in self.neighbor_lists) // 2

    def neighbors(self, i: int) -> tuple[int, ...]:
        return self.neighbor_lists[i]

    def degree(self, i: int) -> int:
        return len(self.neighbor_lists[i])


def adjacency_summary(graph: AdjacencyGraph) -> dict[str, Any]:
    """Connectivity stats for logging or manifests."""
    return {
        "n_nodes": graph.n_nodes,
        "n_edges": graph.n_edges,
        "n_components": graph.n_components,
        "largest_component_size": graph.largest_component_size,
        "n_island_nodes": len(graph.island_nodes),
        "contiguity": graph.contiguity,
    }


def _neighbor_lists_from_weights(w: Any) -> tuple[tuple[int, ...], ...]:
    """Build immutable neighbor lists from a libpysal ``W`` (integer ids 0..n-1)."""
    n = w.n
    return tuple(tuple(sorted(w.neighbors[i])) for i in range(n))


def _island_nodes(neighbor_lists: tuple[tuple[int, ...], ...]) -> tuple[int, ...]:
    return tuple(i for i, neigh in enumerate(neighbor_lists) if len(neigh) == 0)


def _connectivity_from_w(w: Any) -> tuple[int, int]:
    """Return ``(n_components, largest_component_size)`` from libpysal weights ``W``."""
    n_comp, labels = connected_components(w.sparse, directed=False, return_labels=True)
    if n_comp == 0:
        return 0, 0
    counts = np.bincount(labels)
    return int(n_comp), int(counts.max())


def from_libpysal_w(
    w: Any,
    order: PrecinctIndexMap,
    contiguity: str,
) -> AdjacencyGraph:
    """Build :class:`AdjacencyGraph` from a fitted libpysal ``W`` object."""
    if w.n != order.n_units:
        msg = f"weights n={w.n} does not match PrecinctIndexMap n={order.n_units}"
        raise ValueError(msg)
    nbr = _neighbor_lists_from_weights(w)
    islands = _island_nodes(nbr)
    n_comp, largest = _connectivity_from_w(w)
    return AdjacencyGraph(
        order=order,
        contiguity=contiguity,
        neighbor_lists=nbr,
        island_nodes=islands,
        n_components=n_comp,
        largest_component_size=largest,
    )


def from_neighbor_lists(
    order: PrecinctIndexMap,
    contiguity: str,
    neighbor_lists: tuple[tuple[int, ...], ...],
) -> AdjacencyGraph:
    """Build graph from explicit symmetric neighbor lists (e.g. after load/patch)."""
    if len(neighbor_lists) != order.n_units:
        msg = "neighbor_lists length must match PrecinctIndexMap"
        raise ValueError(msg)
    # Build a minimal CSR for connectivity (symmetrize)
    n = len(neighbor_lists)
    rows: list[int] = []
    cols: list[int] = []
    for i, neigh in enumerate(neighbor_lists):
        for j in neigh:
            rows.append(i)
            cols.append(j)
    data = np.ones(len(rows), dtype=np.int8)
    mat = csr_matrix((data, (rows, cols)), shape=(n, n))
    n_comp, labels = connected_components(mat, directed=False, return_labels=True)
    largest = int(np.bincount(labels).max()) if n_comp else 0
    islands = _island_nodes(neighbor_lists)
    return AdjacencyGraph(
        order=order,
        contiguity=contiguity,
        neighbor_lists=neighbor_lists,
        island_nodes=islands,
        n_components=int(n_comp),
        largest_component_size=largest,
    )
