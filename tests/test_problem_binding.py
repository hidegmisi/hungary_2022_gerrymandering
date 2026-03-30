"""Tests for PrecinctIndexMap, validation, and OevkProblem binding (Slice 2)."""

from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import pytest
from shapely.geometry import Polygon

from hungary_ge.problem import (
    DEFAULT_PRECINCT_ID_COLUMN,
    OevkProblem,
    PrecinctIndexMap,
    ProblemFrameValidationError,
    prepare_precinct_layer,
    validate_problem_frame,
)


def _box(lon: float, lat: float, d: float = 0.01) -> Polygon:
    return Polygon(
        [
            (lon, lat),
            (lon + d, lat),
            (lon + d, lat + d),
            (lon, lat + d),
            (lon, lat),
        ],
    )


def _minimal_problem() -> OevkProblem:
    return OevkProblem(county_column=None, pop_column=None, crs="EPSG:4326")


def test_from_frame_sorts_lexicographic() -> None:
    gdf = gpd.GeoDataFrame(
        {
            DEFAULT_PRECINCT_ID_COLUMN: ["02-a", "01-b"],
            "geometry": [_box(19.0, 47.0), _box(19.1, 47.1)],
        },
        crs="EPSG:4326",
    )
    pmap, sorted_gdf = PrecinctIndexMap.from_frame(gdf)
    assert list(sorted_gdf[DEFAULT_PRECINCT_ID_COLUMN]) == ["01-b", "02-a"]
    assert pmap.ids == ("01-b", "02-a")
    assert pmap.id_at(0) == "01-b"
    assert pmap.index_of("02-a") == 1


def test_from_frame_does_not_mutate_input() -> None:
    gdf = gpd.GeoDataFrame(
        {
            DEFAULT_PRECINCT_ID_COLUMN: ["z", "a"],
            "geometry": [_box(19.0, 47.0), _box(19.1, 47.1)],
        },
        crs="EPSG:4326",
    )
    orig = list(gdf[DEFAULT_PRECINCT_ID_COLUMN])
    PrecinctIndexMap.from_frame(gdf)
    assert list(gdf[DEFAULT_PRECINCT_ID_COLUMN]) == orig


def test_from_frame_duplicate_ids() -> None:
    gdf = gpd.GeoDataFrame(
        {
            DEFAULT_PRECINCT_ID_COLUMN: ["same", "same"],
            "geometry": [_box(19.0, 47.0), _box(19.1, 47.1)],
        },
        crs="EPSG:4326",
    )
    with pytest.raises(ProblemFrameValidationError, match="duplicate"):
        PrecinctIndexMap.from_frame(gdf)


def test_index_of_unknown() -> None:
    gdf = gpd.GeoDataFrame(
        {
            DEFAULT_PRECINCT_ID_COLUMN: ["only"],
            "geometry": [_box(19.0, 47.0)],
        },
        crs="EPSG:4326",
    )
    pmap, _ = PrecinctIndexMap.from_frame(gdf)
    with pytest.raises(KeyError, match="unknown"):
        pmap.index_of("missing")


def test_validate_problem_frame_ok() -> None:
    prob = _minimal_problem()
    gdf = gpd.GeoDataFrame(
        {
            DEFAULT_PRECINCT_ID_COLUMN: ["a"],
            "geometry": [_box(19.0, 47.0)],
        },
        crs="EPSG:4326",
    )
    validate_problem_frame(gdf, prob)


def test_validate_missing_precinct_column() -> None:
    prob = OevkProblem(
        precinct_id_column="pid",
        county_column=None,
        pop_column=None,
        crs="EPSG:4326",
    )
    gdf = gpd.GeoDataFrame({"geometry": [_box(19.0, 47.0)]}, crs="EPSG:4326")
    with pytest.raises(ProblemFrameValidationError, match="missing column"):
        validate_problem_frame(gdf, prob)


def test_validate_requires_population_when_configured() -> None:
    prob = OevkProblem(
        county_column=None,
        pop_column="population",
        crs="EPSG:4326",
    )
    gdf = gpd.GeoDataFrame(
        {
            DEFAULT_PRECINCT_ID_COLUMN: ["a"],
            "geometry": [_box(19.0, 47.0)],
        },
        crs="EPSG:4326",
    )
    with pytest.raises(ProblemFrameValidationError, match="population"):
        validate_problem_frame(gdf, prob)


def test_validate_crs_mismatch() -> None:
    prob = OevkProblem(county_column=None, pop_column=None, crs="EPSG:3857")
    gdf = gpd.GeoDataFrame(
        {
            DEFAULT_PRECINCT_ID_COLUMN: ["a"],
            "geometry": [_box(19.0, 47.0)],
        },
        crs="EPSG:4326",
    )
    with pytest.raises(ProblemFrameValidationError, match="CRS"):
        validate_problem_frame(gdf, prob)


def test_prepare_precinct_layer_end_to_end() -> None:
    prob = OevkProblem(county_column=None, pop_column=None, crs="EPSG:4326")
    gdf = gpd.GeoDataFrame(
        {
            DEFAULT_PRECINCT_ID_COLUMN: ["c", "b", "a"],
            "geometry": [_box(19.0, 47.0), _box(19.1, 47.1), _box(19.2, 47.2)],
        },
        crs="EPSG:4326",
    )
    out, pmap = prepare_precinct_layer(gdf, prob)
    assert list(out[DEFAULT_PRECINCT_ID_COLUMN]) == ["a", "b", "c"]
    assert pmap.ids == ("a", "b", "c")


def test_oevk_problem_with_artifact() -> None:
    p = OevkProblem()
    q = p.with_artifact(Path("/tmp/precincts.parquet"), sha256="abc")
    assert q.artifact_path == Path("/tmp/precincts.parquet")
    assert q.artifact_sha256 == "abc"
    assert p.artifact_path is None
