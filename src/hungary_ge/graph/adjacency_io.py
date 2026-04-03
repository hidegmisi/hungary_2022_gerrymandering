"""Save / load adjacency edge lists and apply index-space patches."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any

import pandas as pd

from hungary_ge.graph.adjacency_graph import (
    AdjacencyBuildOptions,
    AdjacencyGraph,
    from_neighbor_lists,
)
from hungary_ge.problem.precinct_index_map import PrecinctIndexMap

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AdjacencyPatch:
    """Edge edits in :class:`PrecinctIndexMap` index space (undirected)."""

    add: tuple[tuple[int, int], ...] = ()
    remove: tuple[tuple[int, int], ...] = ()


@dataclass(frozen=True)
class AdjacencyPatchStats:
    n_add_applied: int
    n_remove_applied: int


def _normalize_edge(i: int, j: int) -> tuple[int, int]:
    if i == j:
        msg = f"self-loop not allowed: ({i}, {j})"
        raise ValueError(msg)
    return (i, j) if i < j else (j, i)


def _edges_to_neighbor_lists(
    n_nodes: int,
    edges: set[tuple[int, int]],
) -> tuple[tuple[int, ...], ...]:
    nbr: list[set[int]] = [set() for _ in range(n_nodes)]
    for a, b in edges:
        if not (0 <= a < n_nodes and 0 <= b < n_nodes):
            msg = f"edge out of range: ({a}, {b}) for n_nodes={n_nodes}"
            raise ValueError(msg)
        nbr[a].add(b)
        nbr[b].add(a)
    return tuple(tuple(sorted(s)) for s in nbr)


def _graph_to_edge_set(graph: AdjacencyGraph) -> set[tuple[int, int]]:
    edges: set[tuple[int, int]] = set()
    for i, neigh in enumerate(graph.neighbor_lists):
        for j in neigh:
            if i < j:
                edges.add((i, j))
    return edges


def apply_adjacency_patch(
    graph: AdjacencyGraph,
    patch: AdjacencyPatch,
) -> tuple[AdjacencyGraph, AdjacencyPatchStats]:
    """Return a new graph after applying undirected add/remove edge patches."""
    n = graph.n_nodes
    edges = _graph_to_edge_set(graph)
    add_applied = 0
    remove_applied = 0
    for pair in patch.add:
        a, b = pair[0], pair[1]
        e = _normalize_edge(a, b)
        if e not in edges:
            edges.add(e)
            add_applied += 1
    for pair in patch.remove:
        a, b = pair[0], pair[1]
        e = _normalize_edge(a, b)
        if e in edges:
            edges.discard(e)
            remove_applied += 1
    nbr = _edges_to_neighbor_lists(n, edges)
    out = from_neighbor_lists(graph.order, graph.contiguity, nbr)
    stats = AdjacencyPatchStats(
        n_add_applied=add_applied,
        n_remove_applied=remove_applied,
    )
    logger.info(
        "adjacency patch: added %s edges, removed %s edges",
        add_applied,
        remove_applied,
    )
    return out, stats


def _package_version() -> str:
    try:
        return version("hungary-ge")
    except PackageNotFoundError:
        return "0.1.0"


def save_adjacency(
    graph: AdjacencyGraph,
    edges_parquet: str | Path,
    meta_json: str | Path | None = None,
    *,
    hungary_ge_version: str | None = None,
    build_options: AdjacencyBuildOptions | None = None,
    extra_meta: dict[str, Any] | None = None,
) -> None:
    """Write undirected edges ``i < j`` to Parquet and metadata to JSON.

    When ``build_options`` is passed with ``fuzzy=True``, fuzzy-related fields are
    written into the JSON for provenance (load ignores them).

    ``extra_meta``: optional top-level keys merged into the written JSON (e.g. county
    graph provenance and ``graph_health``); must not replace required loader fields.
    """
    edges_parquet = Path(edges_parquet)
    rows: list[dict[str, int]] = []
    for i, neigh in enumerate(graph.neighbor_lists):
        for j in neigh:
            if i < j:
                rows.append({"i": i, "j": j})
    df = pd.DataFrame(rows, columns=["i", "j"])
    edges_parquet.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(edges_parquet, index=False)

    ver = hungary_ge_version if hungary_ge_version is not None else _package_version()
    meta: dict[str, Any] = {
        "precinct_id_column": graph.order.id_column,
        "contiguity": graph.contiguity,
        "n_nodes": graph.n_nodes,
        "n_edges": graph.n_edges,
        "ids": list(graph.order.ids),
        "hungary_ge_version": ver,
        "summary": {
            "n_components": graph.n_components,
            "largest_component_size": graph.largest_component_size,
            "n_island_nodes": len(graph.island_nodes),
        },
    }
    if build_options is not None and build_options.fuzzy:
        meta["fuzzy_buffering"] = build_options.fuzzy_buffering
        meta["fuzzy_tolerance"] = build_options.fuzzy_tolerance
        meta["fuzzy_predicate"] = build_options.fuzzy_predicate
        if build_options.fuzzy_buffer_m is not None:
            meta["fuzzy_buffer_m"] = build_options.fuzzy_buffer_m
        if build_options.fuzzy_buffering:
            meta["fuzzy_metric_crs"] = build_options.fuzzy_metric_crs
    if extra_meta:
        meta.update(extra_meta)
    if meta_json is None:
        meta_json = edges_parquet.with_suffix(".meta.json")
    else:
        meta_json = Path(meta_json)
    meta_json.parent.mkdir(parents=True, exist_ok=True)
    meta_json.write_text(json.dumps(meta, indent=2) + "\n", encoding="utf-8")


def load_adjacency(
    edges_parquet: str | Path,
    meta_json: str | Path | None = None,
) -> AdjacencyGraph:
    """Load :class:`AdjacencyGraph` from Parquet + JSON metadata."""
    edges_parquet = Path(edges_parquet)
    if meta_json is None:
        meta_json = edges_parquet.with_suffix(".meta.json")
    else:
        meta_json = Path(meta_json)
    df = pd.read_parquet(edges_parquet)
    meta = json.loads(meta_json.read_text(encoding="utf-8"))
    n_nodes = int(meta["n_nodes"])
    ids_list = meta["ids"]
    id_column = str(meta["precinct_id_column"])
    contiguity = str(meta["contiguity"])
    order = PrecinctIndexMap(ids=tuple(ids_list), id_column=id_column)

    edges: set[tuple[int, int]] = set()
    for i, j in zip(df["i"].astype(int), df["j"].astype(int), strict=False):
        edges.add(_normalize_edge(int(i), int(j)))
    nbr = _edges_to_neighbor_lists(n_nodes, edges)
    return from_neighbor_lists(order, contiguity, nbr)


def load_patch_from_json(path: str | Path) -> AdjacencyPatch:
    """Load :class:`AdjacencyPatch` from ``{"add": [[i,j],...], "remove": [...]}``."""
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    add = tuple(tuple(pair) for pair in raw.get("add", []))
    remove = tuple(tuple(pair) for pair in raw.get("remove", []))
    return AdjacencyPatch(add=add, remove=remove)
