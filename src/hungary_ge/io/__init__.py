"""Geographic I/O: raw settlement JSON and processed precinct layers.

Corresponds to preparing inputs before the ALARM ``redist_map`` stage is fully
instantiated in memory (GeoPandas + problem metadata).
"""

from hungary_ge.io.electoral_etl import (
    ElectoralBuildStats,
    ListPartyMap,
    assert_focal_assignments_valid,
    build_electoral_tables,
    default_list_party_map_path,
    electoral_vote_columns,
    join_electoral_to_gdf,
    load_focal_assignments,
    load_list_party_map,
    load_votes_table,
    write_electoral_parquets,
)
from hungary_ge.io.gaps import (
    GapBuildOptions,
    GapBuildStats,
    GapShellSource,
    build_gap_features_all_counties,
    build_gap_features_for_maz,
    merge_szvk_and_gaps,
    read_shell_gdf,
)
from hungary_ge.io.gaps_hex import (
    HexVoidOptions,
    circumradius_from_hex_area,
    hex_area_from_circumradius,
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
from hungary_ge.io.precinct_geometry_qa import (
    compute_precinct_metrics,
    compute_precinct_overlaps,
    filter_szvk_rows,
)
from hungary_ge.io.szavkor_parse import (
    SzavkorRecord,
    composite_precinct_id,
    parse_poligon,
)

__all__ = [
    "ElectoralBuildStats",
    "GapBuildOptions",
    "GapBuildStats",
    "GapShellSource",
    "HexVoidOptions",
    "ListPartyMap",
    "assert_focal_assignments_valid",
    "build_electoral_tables",
    "circumradius_from_hex_area",
    "default_list_party_map_path",
    "electoral_vote_columns",
    "hex_area_from_circumradius",
    "join_electoral_to_gdf",
    "load_focal_assignments",
    "load_list_party_map",
    "load_votes_table",
    "PrecinctBuildStats",
    "SzavkorRecord",
    "compute_precinct_metrics",
    "compute_precinct_overlaps",
    "filter_szvk_rows",
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
    "write_electoral_parquets",
    "write_processed_geojson",
    "write_processed_geoparquet",
]
