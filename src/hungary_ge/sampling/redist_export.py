"""Export a `redist`-ready file bundle (GeoPackage, edges, manifest)."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import geopandas as gpd
import pandas as pd
from geopandas import GeoDataFrame

from hungary_ge.graph.adjacency_graph import AdjacencyGraph
from hungary_ge.sampling.sampler_config import SamplerConfig

RUN_SCHEMA_VERSION = "hungary_ge.redist_run/v1"

PRECINCT_GPKG_NAME = "precincts.gpkg"
EDGES_CSV_NAME = "edges.csv"
RUN_JSON_NAME = "run.json"
ASSIGNMENTS_CSV_NAME = "assignments.csv"


def _edges_from_graph(graph: AdjacencyGraph) -> list[tuple[int, int]]:
    """Undirected unique edges as ``(i, j)`` with ``i < j``, 0-based row indices.

    ``r/redist/run_smc.R`` builds an adjacency list aligned with this ordering,
    then converts neighbor vertex ids to **0-based** for ``redist`` (same as
    ``redist::redist.adjacency`` output).
    """
    seen: list[tuple[int, int]] = []
    for i, neigh in enumerate(graph.neighbor_lists):
        for j in neigh:
            if i < j:
                seen.append((i, j))
    return sorted(seen)


def _validate_frame_aligns_graph(
    gdf: GeoDataFrame,
    graph: AdjacencyGraph,
    *,
    id_column: str,
    pop_column: str,
) -> None:
    if len(gdf) != graph.n_nodes:
        msg = f"gdf rows {len(gdf)} != graph.n_nodes {graph.n_nodes}"
        raise ValueError(msg)
    if id_column not in gdf.columns:
        msg = f"missing id column {id_column!r}"
        raise ValueError(msg)
    if pop_column not in gdf.columns:
        msg = f"missing pop column {pop_column!r}"
        raise ValueError(msg)
    ids_frame = gdf[id_column].astype(str).tolist()
    if tuple(ids_frame) != graph.order.ids:
        msg = "gdf row order does not match graph.order.ids (PrecinctIndexMap)"
        raise ValueError(msg)
    if not pd.api.types.is_numeric_dtype(gdf[pop_column]):
        msg = f"population column {pop_column!r} must be numeric"
        raise ValueError(msg)


def export_redist_bundle(
    gdf: GeoDataFrame,
    graph: AdjacencyGraph,
    *,
    config: SamplerConfig,
    run_dir: Path,
    ndists: int,
    precinct_id_column: str,
    pop_column: str,
    total_pop_column: str = "pop",
) -> Path:
    """Write GeoPackage, ``edges.csv``, and ``run.json`` under ``run_dir``.

    Args:
        gdf: Frame in **canonical row order** (same as ``graph.order.ids``).
        graph: Contiguity graph (0-based indices aligned with ``gdf`` rows).
        config: Sampler configuration (seeds, tolerances, paths base).
        run_dir: Directory to create/populate (caller may mkdir parents).
        ndists: District count for `redist`.
        precinct_id_column: Source column for unit ids (kept in GPKG).
        pop_column: Elector / population column in ``gdf`` (copied to ``total_pop_column``).
        total_pop_column: Column name required by `redist` (default ``"pop"``).

    Returns:
        ``run_dir`` (absolute path resolved).

    Raises:
        ValueError: On row / id / population misalignment.
    """
    _validate_frame_aligns_graph(
        gdf, graph, id_column=precinct_id_column, pop_column=pop_column
    )
    run_dir = run_dir.resolve()
    run_dir.mkdir(parents=True, exist_ok=True)

    out_gdf = gdf[[precinct_id_column, pop_column, "geometry"]].copy()
    out_gdf = out_gdf.rename(columns={pop_column: total_pop_column})

    gpkg_path = run_dir / PRECINCT_GPKG_NAME
    out_gdf.to_file(gpkg_path, driver="GPKG", layer="precincts")

    edges = _edges_from_graph(graph)
    edges_df = pd.DataFrame(edges, columns=["i", "j"], dtype="int64")
    edges_path = run_dir / EDGES_CSV_NAME
    edges_df.to_csv(edges_path, index=False)

    versions_hint = os.environ.get("HUNGARY_GE_VERSIONS_STRING", "")

    run_payload: dict[str, Any] = {
        "schema_version": RUN_SCHEMA_VERSION,
        "precinct_gpkg": str(gpkg_path.name),
        "edges_csv": str(edges_path.name),
        "assignments_csv": ASSIGNMENTS_CSV_NAME,
        "ndists": ndists,
        "pop_tol": config.pop_tol,
        "n_sims": config.n_sims,
        "n_runs": config.n_runs,
        "seed": config.seed,
        "compactness": config.compactness,
        "n_nodes": graph.n_nodes,
        "id_column": precinct_id_column,
        "total_pop_column": total_pop_column,
        "redist_extras": dict(config.redist_extras),
    }
    if versions_hint:
        run_payload["versions"] = versions_hint

    run_json_path = run_dir / RUN_JSON_NAME
    run_json_path.write_text(
        json.dumps(run_payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    return run_dir


def load_run_manifest(run_dir: Path) -> dict[str, Any]:
    """Load ``run.json`` (stdlib JSON)."""
    path = run_dir / RUN_JSON_NAME
    return json.loads(path.read_text(encoding="utf-8"))


def precinct_layer_from_bundle(run_dir: Path) -> GeoDataFrame:
    """Read the exported precinct GeoPackage (for tests / checks)."""
    path = run_dir / PRECINCT_GPKG_NAME
    return gpd.read_file(path, layer="precincts")
