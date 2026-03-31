"""Repository paths and canonical ``data/processed/`` artifact basenames.

Basenames match ``docs/data-model.md``. Resolve paths with
:class:`ProcessedPaths` relative to the repository root.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

PRECINCTS_GEOJSON = "precincts.geojson"
PRECINCTS_PARQUET = "precincts.parquet"
PRECINCT_VOTES_PARQUET = "precinct_votes.parquet"
ENSEMBLE_ASSIGNMENTS_PARQUET = "ensemble_assignments.parquet"
ENSEMBLE_ASSIGNMENTS_META = "ensemble_assignments.meta.json"
# Sidecar to assignments parquet (same directory; default ``{stem}_diagnostics.json``).
ENSEMBLE_ASSIGNMENTS_DIAGNOSTICS_JSON = "ensemble_assignments_diagnostics.json"
FOCAL_OEVK_ASSIGNMENTS_PARQUET = "focal_oevk_assignments.parquet"

PROCESSED_DIR = Path("data/processed")
MANIFESTS_DIR = Path("data/processed/manifests")
GRAPH_DIR = Path("data/processed/graph")
ADJACENCY_EDGES_PARQUET = "adjacency_edges.parquet"


@dataclass(frozen=True)
class ProcessedPaths:
    """Canonical files under ``data/processed/`` relative to ``repo_root``."""

    repo_root: Path

    @property
    def processed_dir(self) -> Path:
        return self.repo_root / PROCESSED_DIR

    @property
    def manifests_dir(self) -> Path:
        return self.repo_root / MANIFESTS_DIR

    @property
    def precincts_geojson(self) -> Path:
        return self.processed_dir / PRECINCTS_GEOJSON

    @property
    def precincts_parquet(self) -> Path:
        return self.processed_dir / PRECINCTS_PARQUET

    @property
    def precinct_votes_parquet(self) -> Path:
        return self.processed_dir / PRECINCT_VOTES_PARQUET

    @property
    def ensemble_assignments_parquet(self) -> Path:
        return self.processed_dir / ENSEMBLE_ASSIGNMENTS_PARQUET

    @property
    def ensemble_assignments_meta(self) -> Path:
        return self.processed_dir / ENSEMBLE_ASSIGNMENTS_META

    @property
    def ensemble_assignments_diagnostics_json(self) -> Path:
        return self.processed_dir / ENSEMBLE_ASSIGNMENTS_DIAGNOSTICS_JSON

    @property
    def focal_oevk_assignments_parquet(self) -> Path:
        return self.processed_dir / FOCAL_OEVK_ASSIGNMENTS_PARQUET

    def manifest_json(self, build_id: str) -> Path:
        """Optional reproducibility manifest: ``manifests/{build_id}.json``."""
        return self.manifests_dir / f"{build_id}.json"

    @property
    def graph_dir(self) -> Path:
        return self.repo_root / GRAPH_DIR

    @property
    def adjacency_edges_parquet(self) -> Path:
        return self.graph_dir / ADJACENCY_EDGES_PARQUET
