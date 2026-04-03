"""National adjacency by merging per-county fuzzy builds plus bicounty cross-border edges.

Intra-county edges use the same subgraph as a county-only run; inter-county edges
come from fuzzy contiguity on two counties together. That graph can differ from
a single libpysal ``fuzzy_contiguity`` fit on the entire national layer.
"""

from __future__ import annotations

from geopandas import GeoDataFrame

from hungary_ge.graph.adjacency import build_adjacency
from hungary_ge.graph.adjacency_graph import (
    AdjacencyBuildOptions,
    AdjacencyGraph,
    from_neighbor_lists,
)
from hungary_ge.problem import OevkProblem, prepare_precinct_layer

_MERGED_CONTIGUITY_LABEL = "fuzzy:buffered:county_merged"


def _normalize_maz(value: str | int | float) -> str:
    s = str(value).strip()
    if s.isdigit():
        return s.zfill(2)
    return s


def county_adjacent_maz_pairs(gdf: GeoDataFrame) -> set[tuple[str, str]]:
    """Return unordered ``(maz_min, maz_max)`` pairs whose dissolved szvk footprints touch.

    Uses only rows that are not ``unit_kind=void`` when that column exists, so void
    geometry does not invent county–county contacts.
    """
    if "maz" not in gdf.columns:
        msg = "county_adjacent_maz_pairs requires a 'maz' column"
        raise ValueError(msg)
    g = gdf
    if "unit_kind" in g.columns:
        g = g[g["unit_kind"].astype(str) != "void"].copy()
    if g.empty:
        return set()
    work = g.copy()
    work["_maz_n"] = work["maz"].map(_normalize_maz)
    work = work.drop(columns=["maz"])
    dissolved = work.dissolve(by="_maz_n", as_index=False)
    dissolved = dissolved.rename(columns={"_maz_n": "maz"}).reset_index(drop=True)
    if len(dissolved) < 2:
        return set()
    mazes = [_normalize_maz(m) for m in dissolved["maz"].tolist()]
    geoms = dissolved.geometry
    pairs: set[tuple[str, str]] = set()
    # O(n^2) over ~20 megye; avoids self-sjoin duplicate column quirks.
    for i in range(len(dissolved)):
        for j in range(i + 1, len(dissolved)):
            if mazes[i] == mazes[j]:
                continue
            if geoms.iloc[i].touches(geoms.iloc[j]):
                a, b = mazes[i], mazes[j]
                pairs.add((a, b) if a < b else (b, a))
    return pairs


def _national_id_to_index(gdf_nat: GeoDataFrame, problem: OevkProblem) -> dict[str, int]:
    col = problem.precinct_id_column
    return {str(pid): i for i, pid in enumerate(gdf_nat[col].astype(str))}


def _national_id_to_maz(gdf_nat: GeoDataFrame, problem: OevkProblem) -> dict[str, str]:
    col = problem.precinct_id_column
    return {
        str(pid): _normalize_maz(m)
        for pid, m in zip(
            gdf_nat[col], gdf_nat["maz"], strict=False
        )
    }


def _graph_edges_as_national_indices(
    graph: AdjacencyGraph,
    id_to_nat_idx: dict[str, int],
) -> set[tuple[int, int]]:
    out: set[tuple[int, int]] = set()
    order = graph.order
    for i, neigh in enumerate(graph.neighbor_lists):
        id_i = str(order.id_at(i))
        ni = id_to_nat_idx.get(id_i)
        if ni is None:
            continue
        for j in neigh:
            if i >= j:
                continue
            id_j = str(order.id_at(j))
            nj = id_to_nat_idx.get(id_j)
            if nj is None:
                continue
            out.add((ni, nj) if ni < nj else (nj, ni))
    return out


def _neighbor_lists_from_edge_set(
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


def build_national_adjacency_merged(
    gdf_raw: GeoDataFrame,
    problem: OevkProblem,
    options: AdjacencyBuildOptions,
) -> AdjacencyGraph:
    """Build national graph: intra-county fuzzy on each ``maz``, plus cross edges from bicounty fuzzy.

    Expects ``gdf_raw`` with ``maz`` and geometries aligned to ``problem`` (same as
    single-pass ``prepare_precinct_layer`` + ``build_adjacency`` inputs).

    Intra-county edges use a graph fitted **only** on that county's rows (like the
    county pipeline). Cross-county edges come from fuzzy contiguity on the **union**
    of two adjacent counties, keeping only pairs whose ``maz`` differ. This can
    differ slightly from one libpysal run on the full national layer.
    """
    if "maz" not in gdf_raw.columns:
        msg = "build_national_adjacency_merged requires a 'maz' column"
        raise ValueError(msg)
    if not options.fuzzy:
        msg = "build_national_adjacency_merged expects options.fuzzy=True"
        raise ValueError(msg)

    gdf_nat, pmap_nat = prepare_precinct_layer(gdf_raw, problem)
    n = pmap_nat.n_units
    id_to_idx = _national_id_to_index(gdf_nat, problem)
    id_to_maz = _national_id_to_maz(gdf_nat, problem)

    edges: set[tuple[int, int]] = set()

    maz_vals = sorted({_normalize_maz(m) for m in gdf_nat["maz"].tolist()})
    mzn = gdf_nat["maz"].map(_normalize_maz)

    for maz in maz_vals:
        sub = gdf_nat.loc[mzn == maz].copy()
        if sub.empty:
            continue
        gdf_m, pmap_m = prepare_precinct_layer(sub, problem)
        graph_m = build_adjacency(gdf_m, problem, pmap_m, options=options)
        edges |= _graph_edges_as_national_indices(graph_m, id_to_idx)

    for a, b in county_adjacent_maz_pairs(gdf_raw):
        sub = gdf_nat.loc[(mzn == a) | (mzn == b)].copy()
        if sub.empty:
            continue
        gdf_ab, pmap_ab = prepare_precinct_layer(sub, problem)
        graph_ab = build_adjacency(gdf_ab, problem, pmap_ab, options=options)
        order_ab = graph_ab.order
        for i, neigh in enumerate(graph_ab.neighbor_lists):
            id_i = str(order_ab.id_at(i))
            maz_i = id_to_maz.get(id_i)
            ni = id_to_idx.get(id_i)
            if maz_i is None or ni is None:
                continue
            for j in neigh:
                if i >= j:
                    continue
                id_j = str(order_ab.id_at(j))
                maz_j = id_to_maz.get(id_j)
                nj = id_to_idx.get(id_j)
                if maz_j is None or nj is None:
                    continue
                if maz_i == maz_j:
                    continue
                edges.add((ni, nj) if ni < nj else (nj, ni))

    nbr = _neighbor_lists_from_edge_set(n, edges)
    return from_neighbor_lists(pmap_nat, _MERGED_CONTIGUITY_LABEL, nbr)
