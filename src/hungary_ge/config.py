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
FOCAL_OEVK_PARQUET = "focal_oevk.parquet"

PROCESSED_DIR = Path("data/processed")
MANIFESTS_DIR = Path("data/processed/manifests")


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
    def focal_oevk_parquet(self) -> Path:
        return self.processed_dir / FOCAL_OEVK_PARQUET

    def manifest_json(self, build_id: str) -> Path:
        """Optional reproducibility manifest: ``manifests/{build_id}.json``."""
        return self.manifests_dir / f"{build_id}.json"
