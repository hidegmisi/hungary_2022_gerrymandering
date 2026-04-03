"""Void / gap polygon build (shell minus szvk union)."""

from __future__ import annotations

import json
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import box

from hungary_ge.graph import AdjacencyBuildOptions, build_adjacency
from hungary_ge.io.gaps import (
    GapBuildOptions,
    GapShellSource,
    build_gap_features_all_counties,
    build_gap_features_for_maz,
    compute_shell_source_sha256,
    merge_szvk_and_gaps,
    read_shell_gdf,
)
from hungary_ge.problem import DEFAULT_PRECINCT_ID_COLUMN, OevkProblem
from hungary_ge.problem.precinct_index_map import prepare_precinct_layer


@pytest.fixture
def crs_metric() -> str:
    return "EPSG:32633"


def test_read_shell_gdf_roundtrip(tmp_path: Path, crs_metric: str) -> None:
    shell = gpd.GeoDataFrame(
        {"maz": ["01", 2], "geometry": [box(0, 0, 1, 1), box(2, 2, 3, 3)]},
        crs=crs_metric,
    )
    p = tmp_path / "shell.geojson"
    shell.to_file(p, driver="GeoJSON")
    g = read_shell_gdf(GapShellSource(path=p, maz_column="maz"))
    assert len(g) == 2
    assert "maz" in g.columns


def test_read_shell_gdf_admin_directory(tmp_path: Path, crs_metric: str) -> None:
    admin = tmp_path / "admin"
    admin.mkdir()
    for maz, xmin in [("01", 0), ("02", 10)]:
        g = gpd.GeoDataFrame(
            {"ksh": [maz], "geometry": [box(xmin, 0, xmin + 1, 1)]},
            crs=crs_metric,
        )
        g.to_file(admin / f"{maz}.geojson", driver="GeoJSON")
    out = read_shell_gdf(GapShellSource(path=admin, maz_column="maz"))
    assert len(out) == 2
    assert set(out["maz"].astype(str).tolist()) == {"01", "02"}


def test_read_shell_gdf_admin_directory_ksh_mismatch(
    tmp_path: Path, crs_metric: str
) -> None:
    admin = tmp_path / "admin"
    admin.mkdir()
    g = gpd.GeoDataFrame(
        {"ksh": ["02"], "geometry": [box(0, 0, 1, 1)]},
        crs=crs_metric,
    )
    g.to_file(admin / "01.geojson", driver="GeoJSON")
    with pytest.raises(ValueError, match="ksh"):
        read_shell_gdf(GapShellSource(path=admin, maz_column="maz"))


def test_read_shell_gdf_admin_directory_requires_maz_column_maz(
    tmp_path: Path, crs_metric: str
) -> None:
    admin = tmp_path / "admin"
    admin.mkdir()
    g = gpd.GeoDataFrame(
        {"ksh": ["01"], "geometry": [box(0, 0, 1, 1)]},
        crs=crs_metric,
    )
    g.to_file(admin / "01.geojson", driver="GeoJSON")
    with pytest.raises(ValueError, match="shell_maz_column"):
        read_shell_gdf(GapShellSource(path=admin, maz_column="ksh"))


def test_compute_shell_source_sha256_directory(
    tmp_path: Path, crs_metric: str
) -> None:
    admin = tmp_path / "admin"
    admin.mkdir()
    for maz in ("01", "02"):
        g = gpd.GeoDataFrame(
            {"ksh": [maz], "geometry": [box(0, 0, 1, 1)]},
            crs=crs_metric,
        )
        g.to_file(admin / f"{maz}.geojson", driver="GeoJSON")
    h1 = compute_shell_source_sha256(admin)
    h2 = compute_shell_source_sha256(admin)
    assert h1 == h2
    assert len(h1) == 64


def test_merge_szvk_and_gaps_overlap_raises(crs_metric: str) -> None:
    szvk = gpd.GeoDataFrame(
        {
            "maz": ["01"],
            "taz": ["001"],
            "szk": ["01"],
            DEFAULT_PRECINCT_ID_COLUMN: ["01-001-01"],
            "geometry": [box(0, 0, 1, 1)],
        },
        crs=crs_metric,
    )
    gap = gpd.GeoDataFrame(
        {
            "maz": ["01"],
            "taz": ["000"],
            "szk": ["000"],
            DEFAULT_PRECINCT_ID_COLUMN: ["01-001-01"],
            "unit_kind": ["void"],
            "geometry": [box(5, 5, 6, 6)],
        },
        crs=crs_metric,
    )
    with pytest.raises(ValueError, match="precinct_id overlap"):
        merge_szvk_and_gaps(szvk, gap)


def test_build_gap_features_fills_channel(crs_metric: str) -> None:
    """Two szvk blocks separated by a strip; gap is the middle void."""
    shell = gpd.GeoDataFrame(
        {"maz": ["01"], "geometry": [box(0, 0, 1000, 1000)]},
        crs=crs_metric,
    )
    szvk = gpd.GeoDataFrame(
        {
            "maz": ["01", "01"],
            "taz": ["001", "002"],
            "szk": ["01", "01"],
            DEFAULT_PRECINCT_ID_COLUMN: ["01-001-01", "01-002-01"],
            "geometry": [
                box(0, 400, 300, 600),
                box(700, 400, 1000, 600),
            ],
        },
        crs=crs_metric,
    )
    gaps, st = build_gap_features_for_maz(
        shell,
        szvk,
        "01",
        shell_maz_column="maz",
        options=GapBuildOptions(min_area_m2=1.0),
    )
    assert st.n_gap_polygons >= 1
    assert len(gaps) >= 1
    assert gaps["unit_kind"].eq("void").all()
    assert gaps[DEFAULT_PRECINCT_ID_COLUMN].str.startswith("gap-01-").all()


def test_queen_adjacency_connects_through_void(crs_metric: str) -> None:
    shell = gpd.GeoDataFrame(
        {"maz": ["01"], "geometry": [box(0, 0, 1000, 1000)]},
        crs=crs_metric,
    )
    s1 = gpd.GeoDataFrame(
        {
            "maz": ["01", "01"],
            "taz": ["001", "002"],
            "szk": ["01", "01"],
            DEFAULT_PRECINCT_ID_COLUMN: ["01-001-01", "01-002-01"],
            "geometry": [
                box(0, 400, 300, 600),
                box(700, 400, 1000, 600),
            ],
        },
        crs=crs_metric,
    )
    gaps, _st = build_gap_features_for_maz(
        shell,
        s1,
        "01",
        shell_maz_column="maz",
        options=GapBuildOptions(min_area_m2=1.0),
    )
    merged = merge_szvk_and_gaps(s1, gaps)
    prob = OevkProblem(
        county_column=None,
        pop_column=None,
        crs=crs_metric,
    )
    prepared, pmap = prepare_precinct_layer(merged, prob)
    graph = build_adjacency(
        prepared,
        prob,
        pmap,
        options=AdjacencyBuildOptions(contiguity="queen"),
    )
    assert graph.n_components == 1
    assert graph.n_edges >= 2
    void_idx = prepared.index[prepared["unit_kind"] == "void"].tolist()
    assert len(void_idx) == 1
    assert graph.degree(void_idx[0]) >= 2


def test_build_gap_features_all_counties_two_maz(crs_metric: str) -> None:
    shell = gpd.GeoDataFrame(
        {
            "maz": ["01", "02"],
            "geometry": [
                box(0, 0, 500, 500),
                box(0, 0, 400, 400),
            ],
        },
        crs=crs_metric,
    )
    szvk = pd.concat(
        [
            gpd.GeoDataFrame(
                {
                    "maz": ["01"],
                    "taz": ["001"],
                    "szk": ["01"],
                    DEFAULT_PRECINCT_ID_COLUMN: ["01-001-01"],
                    "geometry": [box(0, 0, 100, 100)],
                },
                crs=crs_metric,
            ),
            gpd.GeoDataFrame(
                {
                    "maz": ["02"],
                    "taz": ["001"],
                    "szk": ["01"],
                    DEFAULT_PRECINCT_ID_COLUMN: ["02-001-01"],
                    "geometry": [box(0, 0, 50, 50)],
                },
                crs=crs_metric,
            ),
        ],
        ignore_index=True,
    )
    all_gaps, agg = build_gap_features_all_counties(
        shell,
        szvk,
        shell_maz_column="maz",
        options=GapBuildOptions(min_area_m2=1.0),
    )
    assert agg.n_counties_processed == 2
    assert len(all_gaps) >= 2


def test_gap_manifest_json_serializable(crs_metric: str) -> None:
    """Per-maz stats should json-serialize (build script manifest)."""
    shell = gpd.GeoDataFrame(
        {"maz": ["01"], "geometry": [box(0, 0, 100, 100)]},
        crs=crs_metric,
    )
    szvk = gpd.GeoDataFrame(
        {
            "maz": ["01"],
            "taz": ["001"],
            "szk": ["01"],
            DEFAULT_PRECINCT_ID_COLUMN: ["01-001-01"],
            "geometry": [box(0, 0, 10, 10)],
        },
        crs=crs_metric,
    )
    _gaps, agg = build_gap_features_all_counties(
        shell,
        szvk,
        shell_maz_column="maz",
        options=GapBuildOptions(min_area_m2=1.0),
    )
    payload = {
        "per_maz": agg.per_maz,
        "n_gap_polygons": agg.n_gap_polygons,
    }
    json.dumps(payload)
