"""OEVK districting problem specification (redist_map analogue).

Bundles metadata needed for contiguity-constrained redistricting: number of
districts, attribute column names, and optional population balance tolerance.
Geographic units are expected as a GeoPandas GeoDataFrame joined elsewhere.
"""

from __future__ import annotations

from dataclasses import dataclass, field

# Default number of OEVK single-member districts under the post-2011 framework.
DEFAULT_NDISTS = 106

# Composite precinct key as in docs/data-model.md (maz-taz-szk).
DEFAULT_PRECINCT_ID_COLUMN = "precinct_id"


@dataclass(frozen=True)
class OevkProblem:
    """Specification for Hungarian OEVK ensemble analysis.

    Conceptual counterpart of R **redist**'s ``redist_map``: geographic units,
    population (or weight) semantics, district count, and CRS are provided
    through field conventions; the actual geometries live in a GeoDataFrame
    loaded by :mod:`hungary_ge.io`.

    Attributes:
        ndists: Number of districts to partition into (default 106 OEVKs).
        precinct_id_column: Column for stable precinct key (e.g. ``maz-taz-szk``).
        county_column: Optional county code column (``maz``).
        settlement_column: Optional settlement column (``maz-taz`` or similar).
        pop_column: Optional population column for balance constraints.
        vote_columns: Optional party or vote total columns for metrics.
        crs: Coordinate reference system label (e.g. ``EPSG:4326``).
        pop_tol: Optional max fractional deviation from equal population per
            district (soft or hard depending on sampler).
    """

    ndists: int = DEFAULT_NDISTS
    precinct_id_column: str = DEFAULT_PRECINCT_ID_COLUMN
    county_column: str | None = "maz"
    settlement_column: str | None = None
    pop_column: str | None = "population"
    vote_columns: tuple[str, ...] = field(default_factory=tuple)
    crs: str | None = "EPSG:4326"
    pop_tol: float | None = None

    def __post_init__(self) -> None:
        if self.ndists < 1:
            msg = f"ndists must be >= 1, got {self.ndists}"
            raise ValueError(msg)
