"""Hungary OEVK gerrymandering analysis (ensemble methodology).

Pipeline stages mirror ALARM / ``redist`` (see ``docs/alarm-methodology.md``):
``io`` → ``problem`` → ``graph`` → ``sampling`` → ``ensemble``; then
``diagnostics`` and ``metrics``. Subpackages: ``hungary_ge.io``,
``hungary_ge.problem``, ``hungary_ge.graph``, ``hungary_ge.constraints``,
``hungary_ge.sampling``, ``hungary_ge.ensemble``, ``hungary_ge.diagnostics``,
``hungary_ge.metrics``.
"""

from hungary_ge.config import (
    ADJACENCY_EDGES_PARQUET,
    ENSEMBLE_ASSIGNMENTS_PARQUET,
    FOCAL_OEVK_ASSIGNMENTS_PARQUET,
    GRAPH_DIR,
    PRECINCT_VOTES_PARQUET,
    PRECINCTS_GEOJSON,
    PRECINCTS_PARQUET,
    PROCESSED_DIR,
    ProcessedPaths,
)
from hungary_ge.diagnostics import summarize_ensemble
from hungary_ge.ensemble import PlanEnsemble
from hungary_ge.graph import (
    AdjacencyBuildOptions,
    AdjacencyGraph,
    AdjacencyPatch,
    adjacency_summary,
    apply_adjacency_patch,
    build_adjacency,
    load_adjacency,
    save_adjacency,
)
from hungary_ge.io import (
    PrecinctBuildStats,
    assert_focal_assignments_valid,
    build_electoral_tables,
    build_precinct_gdf,
    join_electoral_to_gdf,
    load_focal_assignments,
    load_processed_geojson,
    load_processed_geoparquet,
    load_szavkor_settlement_json,
    load_votes_table,
    raw_precinct_list_total,
    write_processed_geojson,
    write_processed_geoparquet,
)
from hungary_ge.metrics import partisan_metrics
from hungary_ge.problem import (
    DEFAULT_NDISTS,
    OevkProblem,
    PrecinctIndexMap,
    ProblemFrameValidationError,
    prepare_precinct_layer,
    validate_problem_frame,
)
from hungary_ge.sampling import sample_plans

__all__ = [
    "DEFAULT_NDISTS",
    "ADJACENCY_EDGES_PARQUET",
    "ENSEMBLE_ASSIGNMENTS_PARQUET",
    "FOCAL_OEVK_ASSIGNMENTS_PARQUET",
    "GRAPH_DIR",
    "PRECINCTS_GEOJSON",
    "PRECINCTS_PARQUET",
    "PRECINCT_VOTES_PARQUET",
    "PROCESSED_DIR",
    "ProcessedPaths",
    "AdjacencyBuildOptions",
    "AdjacencyGraph",
    "AdjacencyPatch",
    "assert_focal_assignments_valid",
    "build_electoral_tables",
    "join_electoral_to_gdf",
    "load_focal_assignments",
    "load_votes_table",
    "PrecinctBuildStats",
    "PrecinctIndexMap",
    "ProblemFrameValidationError",
    "OevkProblem",
    "PlanEnsemble",
    "adjacency_summary",
    "apply_adjacency_patch",
    "build_adjacency",
    "build_precinct_gdf",
    "load_processed_geojson",
    "load_adjacency",
    "load_processed_geoparquet",
    "load_szavkor_settlement_json",
    "partisan_metrics",
    "prepare_precinct_layer",
    "raw_precinct_list_total",
    "sample_plans",
    "save_adjacency",
    "summarize_ensemble",
    "validate_problem_frame",
    "write_processed_geojson",
    "write_processed_geoparquet",
]

__version__ = "0.1.0"
