"""Per-county redist ensemble generation (Slice D)."""

from __future__ import annotations

import logging
from dataclasses import replace
from pathlib import Path

import numpy as np
import pandas as pd
from geopandas import GeoDataFrame
from scipy.sparse import csr_matrix
from scipy.sparse.csgraph import connected_components

from hungary_ge.config import ENSEMBLE_ASSIGNMENTS_PARQUET, ProcessedPaths
from hungary_ge.ensemble.persistence import save_plan_ensemble
from hungary_ge.ensemble.plan_ensemble import PlanEnsemble
from hungary_ge.graph import AdjacencyBuildOptions, build_adjacency
from hungary_ge.graph.adjacency_graph import AdjacencyGraph
from hungary_ge.graph.adjacency_io import AdjacencyPatch, apply_adjacency_patch
from hungary_ge.io import load_processed_geoparquet
from hungary_ge.pipeline.county_allocation import normalize_maz
from hungary_ge.problem import (
    DEFAULT_PRECINCT_ID_COLUMN,
    OevkProblem,
    prepare_precinct_layer,
)
from hungary_ge.sampling import sample_plans

logger = logging.getLogger(__name__)


def county_ndists_by_maz(counts_parquet: Path) -> dict[str, int]:
    """``maz`` → enacted district count from ``county_oevk_counts.parquet``."""
    df = pd.read_parquet(counts_parquet)
    if "maz" not in df.columns or "n_oevk" not in df.columns:
        msg = "county_oevk_counts.parquet must have columns 'maz' and 'n_oevk'"
        raise ValueError(msg)
    return {
        normalize_maz(m): int(n) for m, n in zip(df["maz"], df["n_oevk"], strict=False)
    }


def _county_subgraph_ok(graph: AdjacencyGraph) -> bool:
    return graph.n_components == 1 and len(graph.island_nodes) == 0


def _bridge_components_for_redist(
    graph: AdjacencyGraph,
    gdf: GeoDataFrame,
    *,
    metric_crs: str,
) -> tuple[AdjacencyGraph, int]:
    """Add edges so every component touches the largest component (redist requires one component).

    Uses row-order centroids in ``metric_crs``; one new edge per non-main component.
    """
    n = graph.n_nodes
    rows: list[int] = []
    cols: list[int] = []
    for i, neigh in enumerate(graph.neighbor_lists):
        for j in neigh:
            rows.append(i)
            cols.append(j)
    mat = csr_matrix((np.ones(len(rows), dtype=np.int8), (rows, cols)), shape=(n, n))
    n_comp, labels = connected_components(mat, directed=False, return_labels=True)
    if n_comp <= 1:
        return graph, 0

    counts = np.bincount(labels)
    main_lab = int(np.argmax(counts))
    main_nodes = np.flatnonzero(labels == main_lab)
    if main_nodes.size == 0:
        return graph, 0

    g_metric = gdf.to_crs(metric_crs)
    centroids = g_metric.geometry.centroid
    x = centroids.x.to_numpy(dtype=np.float64)
    y = centroids.y.to_numpy(dtype=np.float64)

    adds: list[tuple[int, int]] = []
    for lab in range(n_comp):
        if lab == main_lab:
            continue
        sub = np.flatnonzero(labels == lab)
        if sub.size == 0:
            continue
        u = int(sub[0])
        dx = x[u] - x[main_nodes]
        dy = y[u] - y[main_nodes]
        v = int(main_nodes[int(np.argmin(dx * dx + dy * dy))])
        adds.append((u, v))

    patched, stats = apply_adjacency_patch(graph, AdjacencyPatch(add=tuple(adds)))
    return patched, stats.n_add_applied


def run_county_redist_sample(
    *,
    precinct_parquet: Path,
    paths: ProcessedPaths,
    run_id: str,
    maz: str,
    ndists: int,
    pop_column: str,
    adj_opts: AdjacencyBuildOptions,
    n_draws: int,
    n_runs: int,
    seed: int | None,
    pop_tol: float,
    compactness: float,
    rscript_path: Path | None,
    strict_county_connectivity: bool,
    log_prefix: str = "",
) -> PlanEnsemble:
    """Filter precincts to ``maz``, build graph, run ``redist`` SMC, save ensemble Parquet."""
    pq = precinct_parquet.resolve()
    gdf = load_processed_geoparquet(pq)
    if "maz" not in gdf.columns:
        msg = "precinct layer has no 'maz' column"
        raise ValueError(msg)
    if pop_column not in gdf.columns:
        msg = f"precinct layer has no population column {pop_column!r}"
        raise ValueError(msg)

    maz_n = normalize_maz(maz)
    mzn = gdf["maz"].map(normalize_maz)
    county_gdf: GeoDataFrame = gdf[mzn == maz_n].copy()
    if county_gdf.empty:
        msg = f"no precinct rows for maz={maz_n!r}"
        raise ValueError(msg)

    n_units = len(county_gdf)
    if n_units < ndists:
        msg = f"maz {maz_n}: n_units={n_units} < ndists={ndists} (cannot partition)"
        raise ValueError(msg)

    prob = OevkProblem(
        ndists=ndists,
        precinct_id_column=DEFAULT_PRECINCT_ID_COLUMN,
        county_column=None,
        pop_column=pop_column,
        crs="EPSG:4326",
    )
    gdf2, pmap = prepare_precinct_layer(county_gdf, prob)
    graph = build_adjacency(gdf2, prob, pmap, options=adj_opts)

    metric_crs = (
        adj_opts.fuzzy_metric_crs
        if adj_opts.fuzzy and adj_opts.fuzzy_buffering
        else "EPSG:32633"
    )
    if not _county_subgraph_ok(graph):
        graph, n_br = _bridge_components_for_redist(graph, gdf2, metric_crs=metric_crs)
        if n_br > 0:
            logger.warning(
                "%sredist prep: added %s bridging edge(s) between disconnected components "
                "(counts=%s, islands=%s)",
                log_prefix,
                n_br,
                graph.n_components,
                len(graph.island_nodes),
            )

    if strict_county_connectivity and not _county_subgraph_ok(graph):
        msg = (
            f"{log_prefix}subgraph not simply connected after bridging: "
            f"components={graph.n_components}, "
            f"n_island_nodes={len(graph.island_nodes)} "
            "(use --allow-disconnected-county-graph on graph stage for export only; "
            "sampling expects one component with no islands)"
        )
        raise ValueError(msg)

    bundle_dir = paths.county_redist_bundle_dir(run_id, maz_n)
    bundle_dir.mkdir(parents=True, exist_ok=True)

    ensemble = sample_plans(
        prob,
        graph,
        backend="redist",
        gdf=gdf2,
        n_draws=n_draws,
        n_runs=n_runs,
        seed=seed,
        work_dir=bundle_dir,
        pop_tol=pop_tol,
        compactness=compactness,
        rscript_path=rscript_path,
    )

    meta = {
        **dict(ensemble.metadata),
        "county_maz": maz_n,
        "county_ndists": ndists,
        "county_run_id": run_id,
        "county_n_units": ensemble.n_units,
    }
    ensemble_tagged = replace(ensemble, metadata=meta)

    out_dir = paths.county_ensemble_dir(run_id, maz_n)
    out_dir.mkdir(parents=True, exist_ok=True)
    parquet_out = out_dir / ENSEMBLE_ASSIGNMENTS_PARQUET
    save_plan_ensemble(ensemble_tagged, parquet_out)
    return ensemble_tagged
