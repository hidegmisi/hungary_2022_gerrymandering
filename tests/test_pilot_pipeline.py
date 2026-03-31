"""Slice 10 pilot pipeline orchestration."""

from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import pytest
from shapely.geometry import box

from hungary_ge.io.geoio import write_processed_geoparquet
from hungary_ge.pipeline.runner import main
from hungary_ge.problem import DEFAULT_PRECINCT_ID_COLUMN


def _grid_gdf() -> gpd.GeoDataFrame:
    polys = [
        box(0, 0, 1, 1),
        box(1, 0, 2, 1),
        box(0, 1, 1, 2),
        box(1, 1, 2, 2),
    ]
    return gpd.GeoDataFrame(
        {
            DEFAULT_PRECINCT_ID_COLUMN: ["d", "c", "b", "a"],
            "geometry": polys,
        },
        crs="EPSG:4326",
    )


def test_pilot_pipeline_help_exits_zero() -> None:
    with pytest.raises(SystemExit) as exc_info:
        main(["--help"])
    assert exc_info.value.code == 0


def test_pilot_pipeline_graph_stage_writes_edges(tmp_path: Path) -> None:
    repo = tmp_path
    proc = repo / "data" / "processed"
    proc.mkdir(parents=True)
    pq = proc / "precincts.parquet"
    write_processed_geoparquet(_grid_gdf(), pq)
    code = main(
        [
            "--repo-root",
            str(repo),
            "--only",
            "graph",
            "--parquet",
            str(pq),
        ]
    )
    assert code == 0
    edges = repo / "data" / "processed" / "graph" / "adjacency_edges.parquet"
    meta = edges.with_suffix(".meta.json")
    assert edges.is_file()
    assert meta.is_file()
