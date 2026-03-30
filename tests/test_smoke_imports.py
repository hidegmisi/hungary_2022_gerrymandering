"""Install and import smoke tests (Slice 0)."""

from __future__ import annotations


def test_import_geopandas() -> None:
    import geopandas  # noqa: F401


def test_import_hungary_ge() -> None:
    import hungary_ge  # noqa: F401


def test_import_libpysal() -> None:
    import libpysal  # noqa: F401


def test_processed_paths_constant_basenames() -> None:
    from hungary_ge import (
        ENSEMBLE_ASSIGNMENTS_PARQUET,
        FOCAL_OEVK_PARQUET,
        PRECINCT_VOTES_PARQUET,
        PRECINCTS_GEOJSON,
    )

    assert PRECINCTS_GEOJSON == "precincts.geojson"
    assert PRECINCT_VOTES_PARQUET == "precinct_votes.parquet"
    assert ENSEMBLE_ASSIGNMENTS_PARQUET == "ensemble_assignments.parquet"
    assert FOCAL_OEVK_PARQUET == "focal_oevk.parquet"
