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
COUNTY_OEVK_COUNTS_PARQUET = "county_oevk_counts.parquet"
COUNTY_OEVK_COUNTS_META = "county_oevk_counts.meta.json"

PROCESSED_DIR = Path("data/processed")
RUNS_DIRNAME = "runs"
MANIFESTS_DIR = Path("data/processed/manifests")
GRAPH_DIR = Path("data/processed/graph")
ADJACENCY_EDGES_PARQUET = "adjacency_edges.parquet"
ADJACENCY_MAP_HTML = "adjacency_map.html"
COUNTY_WORK_SUBDIR = "counties"
COUNTY_GRAPH_SUBDIR = "graph"
COUNTY_ENSEMBLE_SUBDIR = "ensemble"
COUNTY_REDIST_BUNDLE_SUBDIR = "redist_bundle"
COUNTY_REPORTS_SUBDIR = "reports"
COUNTY_DIAGNOSTICS_JSON = "diagnostics.json"
COUNTY_PARTISAN_REPORT_JSON = "partisan_report.json"
NATIONAL_REPORT_JSON = "national_report.json"


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

    def run_dir(self, run_id: str) -> Path:
        """Per-run artifacts: ``data/processed/runs/<run_id>/``."""
        return self.processed_dir / RUNS_DIRNAME / run_id

    def county_oevk_counts_parquet(self, run_id: str) -> Path:
        return self.run_dir(run_id) / COUNTY_OEVK_COUNTS_PARQUET

    def county_oevk_counts_meta(self, run_id: str) -> Path:
        return self.run_dir(run_id) / COUNTY_OEVK_COUNTS_META

    def national_report_path(self, run_id: str) -> Path:
        """National rollup artifact: ``runs/<run_id>/national_report.json``."""
        return self.run_dir(run_id) / NATIONAL_REPORT_JSON

    def county_work_dir(self, run_id: str, maz: str) -> Path:
        """``runs/<run_id>/counties/<maz>/``."""
        m = str(maz).strip()
        if m.isdigit():
            m = m.zfill(2)
        return self.run_dir(run_id) / COUNTY_WORK_SUBDIR / m

    def county_graph_dir(self, run_id: str, maz: str) -> Path:
        """Per-county graph artifacts: ``runs/<run_id>/counties/<maz>/graph/``."""
        return self.county_work_dir(run_id, maz) / COUNTY_GRAPH_SUBDIR

    def county_ensemble_dir(self, run_id: str, maz: str) -> Path:
        """Per-county ensemble outputs: ``.../counties/<maz>/ensemble/``."""
        return self.county_work_dir(run_id, maz) / COUNTY_ENSEMBLE_SUBDIR

    def county_redist_bundle_dir(self, run_id: str, maz: str) -> Path:
        return self.county_ensemble_dir(run_id, maz) / COUNTY_REDIST_BUNDLE_SUBDIR

    def county_reports_dir(self, run_id: str, maz: str) -> Path:
        """``runs/<run_id>/counties/<maz>/reports/``."""
        return self.county_work_dir(run_id, maz) / COUNTY_REPORTS_SUBDIR

    def county_adjacency_map_path(self, run_id: str, maz: str) -> Path:
        return self.county_graph_dir(run_id, maz) / ADJACENCY_MAP_HTML

    def manifest_json(self, build_id: str) -> Path:
        """Optional reproducibility manifest: ``manifests/{build_id}.json``."""
        return self.manifests_dir / f"{build_id}.json"

    @property
    def graph_dir(self) -> Path:
        return self.repo_root / GRAPH_DIR

    @property
    def adjacency_edges_parquet(self) -> Path:
        return self.graph_dir / ADJACENCY_EDGES_PARQUET
