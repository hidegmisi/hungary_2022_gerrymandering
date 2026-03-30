"""Golden and property tests for szavkor_topo ETL (Slice 1)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from shapely.geometry import MultiPolygon, Polygon

from hungary_ge.io.geoio import (
    load_processed_geoparquet,
    write_processed_geoparquet,
)
from hungary_ge.io.precinct_etl import (
    PrecinctBuildStats,
    _rows_from_settlement,
    build_precinct_gdf,
    iter_settlement_json_paths,
    raw_precinct_list_total,
)
from hungary_ge.io.szavkor_parse import (
    composite_precinct_id,
    parse_centrum,
    parse_poligon,
    repair_polygonal_geometry,
)
from hungary_ge.problem import DEFAULT_PRECINCT_ID_COLUMN

TINY_SETTLEMENT = {
    "header": {"generated": "2022-04-02T23:30:00", "vl_id": 1, "nvv_id": 1},
    "list": [
        {
            "maz": "01",
            "taz": "001",
            "szk": "001",
            "centrum": "47.5 19.0",
            "poligon": "47.5 19.0,47.51 19.0,47.51 19.01,47.5 19.01,47.5 19.0",
        },
        {
            "maz": "01",
            "taz": "001",
            "szk": "002",
            "centrum": "47.52 19.02",
            "poligon": "47.52 19.02,47.53 19.02,47.53 19.03,47.52 19.03,47.52 19.02",
        },
    ],
}


def test_parse_poligon_square() -> None:
    poly = parse_poligon(
        "47.5 19.0,47.51 19.0,47.51 19.01,47.5 19.01,47.5 19.0",
    )
    assert isinstance(poly, Polygon)
    assert poly.is_valid


def test_parse_centrum() -> None:
    assert parse_centrum("47.5 19.03") == (19.03, 47.5)


def test_composite_precinct_id_normalizes() -> None:
    assert composite_precinct_id("1", "2", "3") == "01-002-003"


def test_golden_settlement_rows() -> None:
    stats = PrecinctBuildStats()
    rows = _rows_from_settlement(TINY_SETTLEMENT, stats)
    assert len(rows) == 2
    assert rows[0][DEFAULT_PRECINCT_ID_COLUMN] == "01-001-001"
    assert stats.n_dropped_unrepaired == 0


def test_build_precinct_gdf_tmp(
    tmp_path: Path,
) -> None:
    root = tmp_path / "szavkor_topo"
    (root / "01").mkdir(parents=True)
    path = root / "01" / "01-001.json"
    path.write_text(json.dumps(TINY_SETTLEMENT), encoding="utf-8")
    gdf, stats = build_precinct_gdf(root)
    assert len(gdf) == 2
    assert gdf.crs is not None
    assert str(gdf.crs).upper().endswith("4326") or gdf.crs.to_epsg() == 4326
    ids = gdf[DEFAULT_PRECINCT_ID_COLUMN].tolist()
    assert len(ids) == len(set(ids))
    for geom in gdf.geometry:
        assert isinstance(geom, (Polygon, MultiPolygon))
    assert stats.n_rows_out == 2
    assert raw_precinct_list_total(root) == 2


def test_iter_settlement_json_paths_order(tmp_path: Path) -> None:
    root = tmp_path / "szavkor_topo"
    (root / "02").mkdir(parents=True)
    (root / "01").mkdir(parents=True)
    (root / "01" / "01-001.json").write_text("{}", encoding="utf-8")
    (root / "02" / "02-001.json").write_text("{}", encoding="utf-8")
    paths = list(iter_settlement_json_paths(root))
    assert [p.name for p in paths] == ["01-001.json", "02-001.json"]


def test_geoparquet_roundtrip(tmp_path: Path) -> None:
    root = tmp_path / "szavkor_topo"
    (root / "01").mkdir(parents=True)
    (root / "01" / "01-001.json").write_text(
        json.dumps(TINY_SETTLEMENT),
        encoding="utf-8",
    )
    gdf, _ = build_precinct_gdf(root)
    out = tmp_path / "p.parquet"
    write_processed_geoparquet(gdf, out)
    back = load_processed_geoparquet(out)
    assert len(back) == len(gdf)
    assert set(back[DEFAULT_PRECINCT_ID_COLUMN]) == set(gdf[DEFAULT_PRECINCT_ID_COLUMN])


def test_parse_poligon_too_few_vertices() -> None:
    with pytest.raises(ValueError, match="at least three"):
        parse_poligon("47.5 19.0,47.51 19.0")


def test_repair_polygonal_geometry_accepts_valid() -> None:
    p = parse_poligon(
        "47.5 19.0,47.51 19.0,47.51 19.01,47.5 19.01,47.5 19.0",
    )
    fixed = repair_polygonal_geometry(p)
    assert fixed is not None
    assert fixed.equals(p)
