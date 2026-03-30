"""Hungary OEVK gerrymandering analysis (ensemble methodology).

Pipeline stages mirror ALARM / ``redist`` (see ``docs/alarm-methodology.md``):
``io`` → ``problem`` → ``graph`` → ``sampling`` → ``ensemble``; then
``diagnostics`` and ``metrics``. Subpackages: ``hungary_ge.io``,
``hungary_ge.problem``, ``hungary_ge.graph``, ``hungary_ge.constraints``,
``hungary_ge.sampling``, ``hungary_ge.ensemble``, ``hungary_ge.diagnostics``,
``hungary_ge.metrics``.
"""

from hungary_ge.config import (
    ENSEMBLE_ASSIGNMENTS_PARQUET,
    FOCAL_OEVK_PARQUET,
    PRECINCT_VOTES_PARQUET,
    PRECINCTS_GEOJSON,
    PROCESSED_DIR,
    ProcessedPaths,
)
from hungary_ge.diagnostics import summarize_ensemble
from hungary_ge.ensemble import PlanEnsemble
from hungary_ge.graph import build_adjacency
from hungary_ge.io import (
    load_processed_geojson,
    load_szavkor_settlement_json,
    write_processed_geojson,
)
from hungary_ge.metrics import partisan_metrics
from hungary_ge.problem import DEFAULT_NDISTS, OevkProblem
from hungary_ge.sampling import sample_plans

__all__ = [
    "DEFAULT_NDISTS",
    "ENSEMBLE_ASSIGNMENTS_PARQUET",
    "FOCAL_OEVK_PARQUET",
    "PRECINCTS_GEOJSON",
    "PRECINCT_VOTES_PARQUET",
    "PROCESSED_DIR",
    "ProcessedPaths",
    "OevkProblem",
    "PlanEnsemble",
    "build_adjacency",
    "load_processed_geojson",
    "load_szavkor_settlement_json",
    "partisan_metrics",
    "sample_plans",
    "summarize_ensemble",
    "write_processed_geojson",
]

__version__ = "0.1.0"
