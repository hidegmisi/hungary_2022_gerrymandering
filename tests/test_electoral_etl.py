"""Tests for electoral ETL, loaders, and GeoDataFrame joins."""

from __future__ import annotations

import json
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import Point

from hungary_ge.io.electoral_etl import (
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
from hungary_ge.problem import DEFAULT_PRECINCT_ID_COLUMN


def _tiny_szavkor_dir(tmp_path: Path) -> Path:
    sz = tmp_path / "szavkor"
    (sz / "01").mkdir(parents=True)
    data = {
        "header": {"vl_id": 100, "nvv_id": 200},
        "list": [
            {
                "maz": "01",
                "taz": "001",
                "szk": "001",
                "centrum": "47.0 19.0",
                "poligon": "47.0 19.0,47.01 19.0,47.01 19.01,47.0 19.01,47.0 19.0",
                "voters": 50,
                "listVotes": {"10": 30, "20": 20},
                "oevk_id": "3",
                "oevk_id_full": "0103",
            },
            {
                "maz": "01",
                "taz": "001",
                "szk": "002",
                "centrum": "47.02 19.02",
                "poligon": "47.02 19.02,47.03 19.02,47.03 19.03,47.02 19.03,47.02 19.02",
                "voters": 10,
                "listVotes": {"10": 6, "20": 4},
                "oevk_id": "3",
                "oevk_id_full": "0103",
            },
        ],
    }
    (sz / "01" / "01-001.json").write_text(json.dumps(data), encoding="utf-8")
    return sz


def _party_map(tmp_path: Path) -> Path:
    p = tmp_path / "map.json"
    p.write_text(
        json.dumps(
            {
                "election_year": 2022,
                "lists": {
                    "10": {"column": "votes_party_a", "label_hu": "A"},
                    "20": {"column": "votes_party_b", "label_hu": "B"},
                },
            }
        ),
        encoding="utf-8",
    )
    return p


def test_default_list_party_map_loads() -> None:
    path = default_list_party_map_path()
    assert path.is_file()
    pmap = load_list_party_map(path)
    assert pmap.election_year == 2022
    assert pmap.list_id_to_column["952"] == "votes_list_952"


def test_build_electoral_tables_and_join(tmp_path: Path) -> None:
    sz = _tiny_szavkor_dir(tmp_path)
    pmap_path = _party_map(tmp_path)
    votes_df, focal_df, stats = build_electoral_tables(sz, pmap_path)

    assert stats.n_rows_votes == 2
    assert stats.n_rows_focal == 2
    assert stats.n_duplicate_precinct_id == 0
    assert votes_df.loc[0, "votes_party_a"] == 30
    assert votes_df.loc[1, DEFAULT_PRECINCT_ID_COLUMN] == "01-001-002"
    assert focal_df["oevk_id_full"].tolist() == ["0103", "0103"]

    assert_focal_assignments_valid(focal_df)

    gdf = gpd.GeoDataFrame(
        {
            DEFAULT_PRECINCT_ID_COLUMN: ["01-001-001", "01-001-002"],
            "unit_kind": ["szvk", "szvk"],
            "geometry": [Point(19.0, 47.0), Point(19.02, 47.02)],
        },
        crs="EPSG:4326",
    )
    joined = join_electoral_to_gdf(gdf, votes_df)
    assert joined.loc[0, "votes_party_a"] == 30
    assert "maz_x" not in joined.columns


def test_join_clears_votes_on_void(tmp_path: Path) -> None:
    sz = _tiny_szavkor_dir(tmp_path)
    pmap_path = _party_map(tmp_path)
    votes_df, _, _ = build_electoral_tables(sz, pmap_path)
    gdf = gpd.GeoDataFrame(
        {
            DEFAULT_PRECINCT_ID_COLUMN: ["01-001-001", "gap-01-0001"],
            "unit_kind": ["szvk", "void"],
            "geometry": [Point(19.0, 47.0), Point(19.5, 47.5)],
        },
        crs="EPSG:4326",
    )
    joined = join_electoral_to_gdf(gdf, votes_df)
    assert joined.loc[0, "votes_party_a"] == 30
    assert pd.isna(joined.loc[1, "votes_party_a"])


def test_strict_unknown_list_raises(tmp_path: Path) -> None:
    sz = _tiny_szavkor_dir(tmp_path)
    bad = tmp_path / "bad.json"
    bad.write_text(
        json.dumps(
            {"election_year": 2022, "lists": {"10": {"column": "votes_party_a"}}}
        ),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="unknown listVotes"):
        build_electoral_tables(sz, bad, strict_unknown_lists=True)


def test_unknown_list_skipped_and_counted(tmp_path: Path) -> None:
    sz = _tiny_szavkor_dir(tmp_path)
    partial = tmp_path / "partial.json"
    partial.write_text(
        json.dumps(
            {"election_year": 2022, "lists": {"10": {"column": "votes_party_a"}}}
        ),
        encoding="utf-8",
    )
    votes_df, _, stats = build_electoral_tables(sz, partial, strict_unknown_lists=False)
    assert "votes_party_b" not in votes_df.columns
    assert stats.unknown_list_vote_keys["20"] == 2
    assert votes_df["votes_party_a"].tolist() == [30, 6]


def test_electoral_vote_columns() -> None:
    df = pd.DataFrame(
        {
            DEFAULT_PRECINCT_ID_COLUMN: ["a"],
            "maz": ["01"],
            "votes_party_a": [1],
            "voters": [5],
        }
    )
    cols = electoral_vote_columns(df)
    assert cols == ["voters", "votes_party_a"]


def test_parquet_roundtrip(tmp_path: Path) -> None:
    sz = _tiny_szavkor_dir(tmp_path)
    pmap_path = _party_map(tmp_path)
    votes_df, focal_df, _ = build_electoral_tables(sz, pmap_path)
    vpath = tmp_path / "votes.parquet"
    fpath = tmp_path / "focal.parquet"
    write_electoral_parquets(votes_df, focal_df, vpath, fpath)
    again = load_votes_table(vpath)
    assert len(again) == 2
    assert again["votes_party_a"].tolist() == [30, 6]
    focal_again = load_focal_assignments(fpath)
    assert len(focal_again) == 2
    assert focal_again["oevk_id_full"].tolist() == ["0103", "0103"]
