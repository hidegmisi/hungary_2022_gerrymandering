"""Geographic I/O: raw settlement JSON and processed precinct layers.

Corresponds to preparing inputs before the ALARM ``redist_map`` stage is fully
instantiated in memory (GeoPandas + problem metadata).
"""

from hungary_ge.io.gaps import (
    GapBuildOptions,
    GapBuildStats,
    GapShellSource,
    build_gap_features_all_counties,
    build_gap_features_for_maz,
    merge_szvk_and_gaps,
    read_shell_gdf,
)
from hungary_ge.io.geoio import (
    load_processed_geojson,
    load_processed_geoparquet,
    load_szavkor_settlement_json,
    write_processed_geojson,
    write_processed_geoparquet,
)
from hungary_ge.io.precinct_etl import (
    PrecinctBuildStats,
    build_precinct_gdf,
    raw_precinct_list_total,
)
from hungary_ge.io.szavkor_parse import (
    SzavkorRecord,
    composite_precinct_id,
    parse_poligon,
)

__all__ = [
    "GapBuildOptions",
    "GapBuildStats",
    "GapShellSource",
    "PrecinctBuildStats",
    "SzavkorRecord",
    "build_gap_features_all_counties",
    "build_gap_features_for_maz",
    "build_precinct_gdf",
    "composite_precinct_id",
    "merge_szvk_and_gaps",
    "load_processed_geojson",
    "load_processed_geoparquet",
    "load_szavkor_settlement_json",
    "parse_poligon",
    "read_shell_gdf",
    "raw_precinct_list_total",
    "write_processed_geojson",
    "write_processed_geoparquet",
]
