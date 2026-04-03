"""Install and import smoke tests (Slice 0)."""

from __future__ import annotations


def test_import_geopandas() -> None:
    import geopandas  # noqa: F401


def test_import_hungary_ge() -> None:
    import hungary_ge  # noqa: F401


def test_import_ensemble_persistence() -> None:
    from hungary_ge.ensemble import (  # noqa: F401
        load_plan_ensemble,
        load_plan_ensemble_draw_column,
        save_plan_ensemble,
    )


def test_import_viz_plan_assignments() -> None:
    from hungary_ge.viz import (  # noqa: F401
        merge_enacted_districts,
        merge_simulated_districts,
    )


def test_import_libpysal() -> None:
    import libpysal  # noqa: F401


def test_processed_paths_constant_basenames() -> None:
    from hungary_ge import (
        ENSEMBLE_ASSIGNMENTS_DIAGNOSTICS_JSON,
        ENSEMBLE_ASSIGNMENTS_PARQUET,
        FOCAL_OEVK_ASSIGNMENTS_PARQUET,
        PRECINCT_VOTES_PARQUET,
        PRECINCTS_GEOJSON,
    )

    assert PRECINCTS_GEOJSON == "precincts.geojson"
    assert PRECINCT_VOTES_PARQUET == "precinct_votes.parquet"
    assert ENSEMBLE_ASSIGNMENTS_PARQUET == "ensemble_assignments.parquet"
    assert (
        ENSEMBLE_ASSIGNMENTS_DIAGNOSTICS_JSON == "ensemble_assignments_diagnostics.json"
    )
    assert FOCAL_OEVK_ASSIGNMENTS_PARQUET == "focal_oevk_assignments.parquet"
