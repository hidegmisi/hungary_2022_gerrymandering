"""Geographic I/O: raw settlement JSON and processed precinct layers.

Corresponds to preparing inputs before the ALARM ``redist_map`` stage is fully
instantiated in memory (GeoPandas + problem metadata).
"""

from hungary_ge.io.geoio import (
    load_processed_geojson,
    load_szavkor_settlement_json,
    write_processed_geojson,
)

__all__ = [
    "load_processed_geojson",
    "load_szavkor_settlement_json",
    "write_processed_geojson",
]
