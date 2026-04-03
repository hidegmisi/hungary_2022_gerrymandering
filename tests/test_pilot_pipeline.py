"""Slice 10 pilot pipeline orchestration."""

from __future__ import annotations

import json
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import box

from hungary_ge.io.geoio import write_processed_geoparquet
from hungary_ge.pipeline.county_allocation import CANONICAL_MEGYE_CODES
from hungary_ge.pipeline.runner import main
from hungary_ge.problem import DEFAULT_PRECINCT_ID_COLUMN


def _synthetic_focal_106() -> pd.DataFrame:
    """Minimal focal table satisfying allocation validation (106 districts, 20 megye)."""
    codes = list(CANONICAL_MEGYE_CODES)
    base, rem = divmod(106, len(codes))
    rows: list[dict[str, str]] = []
    for i, maz in enumerate(codes):
        n_d = base + (1 if i < rem else 0)
        for j in range(1, n_d + 1):
            rows.append(
                {
                    DEFAULT_PRECINCT_ID_COLUMN: f"{maz}-tst-{j:03d}",
                    "oevk_id_full": f"{maz}{j:02d}",
                    "maz": maz,
                },
            )
    return pd.DataFrame(rows)


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


def test_county_mode_requires_run_id() -> None:
    assert main(["--mode", "county", "--only", "allocation"]) == 2


def test_no_progress_flag_accepted_in_county_mode() -> None:
    assert main(["--mode", "county", "--only", "allocation", "--no-progress"]) == 2


def test_allocation_stage_requires_run_id() -> None:
    assert main(["--only", "allocation"]) == 2


def test_rollup_stage_requires_county_mode(tmp_path: Path) -> None:
    proc = tmp_path / "data" / "processed"
    proc.mkdir(parents=True)
    pq = proc / "precincts.parquet"
    gdf = gpd.GeoDataFrame(
        {
            DEFAULT_PRECINCT_ID_COLUMN: ["01-001-001"],
            "maz": ["01"],
            "voters": [1.0],
            "geometry": [box(0, 0, 1, 1)],
        },
        crs="EPSG:4326",
    )
    write_processed_geoparquet(gdf, pq)
    assert (
        main(
            [
                "--repo-root",
                str(tmp_path),
                "--only",
                "rollup",
                "--run-id",
                "r",
                "--parquet",
                str(pq),
            ],
        )
        == 2
    )


def test_reports_stage_requires_county_mode(tmp_path: Path) -> None:
    proc = tmp_path / "data" / "processed"
    proc.mkdir(parents=True)
    pq = proc / "precincts.parquet"
    gdf = gpd.GeoDataFrame(
        {
            DEFAULT_PRECINCT_ID_COLUMN: ["01-001-001"],
            "maz": ["01"],
            "voters": [1.0],
            "geometry": [box(0, 0, 1, 1)],
        },
        crs="EPSG:4326",
    )
    write_processed_geoparquet(gdf, pq)
    assert (
        main(
            [
                "--repo-root",
                str(tmp_path),
                "--only",
                "reports",
                "--run-id",
                "r",
                "--parquet",
                str(pq),
            ],
        )
        == 2
    )


def test_sample_stage_requires_county_mode(tmp_path: Path) -> None:
    proc = tmp_path / "data" / "processed"
    proc.mkdir(parents=True)
    pq = proc / "precincts.parquet"
    gdf = gpd.GeoDataFrame(
        {
            DEFAULT_PRECINCT_ID_COLUMN: ["01-001-001"],
            "maz": ["01"],
            "voters": [1.0],
            "geometry": [box(0, 0, 1, 1)],
        },
        crs="EPSG:4326",
    )
    write_processed_geoparquet(gdf, pq)
    assert (
        main(
            [
                "--repo-root",
                str(tmp_path),
                "--only",
                "sample",
                "--run-id",
                "r",
                "--parquet",
                str(pq),
            ],
        )
        == 2
    )


def test_county_mode_allocation_writes_run_parquet(tmp_path: Path) -> None:
    repo = tmp_path
    proc = repo / "data" / "processed"
    proc.mkdir(parents=True)
    focal_path = proc / "focal_oevk_assignments.parquet"
    _synthetic_focal_106().to_parquet(focal_path, index=False)
    code = main(
        [
            "--repo-root",
            str(repo),
            "--mode",
            "county",
            "--run-id",
            "r-allocation",
            "--only",
            "allocation",
        ],
    )
    assert code == 0
    out = proc / "runs" / "r-allocation" / "county_oevk_counts.parquet"
    assert out.is_file()


def test_county_mode_graph_writes_per_county(tmp_path: Path) -> None:
    repo = tmp_path
    proc = repo / "data" / "processed"
    proc.mkdir(parents=True)
    run = proc / "runs" / "r-graph"
    run.mkdir(parents=True)
    pd.DataFrame({"maz": ["01"], "n_oevk": [6]}).to_parquet(
        run / "county_oevk_counts.parquet",
        index=False,
    )
    pq = proc / "precincts.parquet"
    gdf = gpd.GeoDataFrame(
        {
            DEFAULT_PRECINCT_ID_COLUMN: ["01-001-001", "01-001-002"],
            "maz": ["01", "01"],
            "geometry": [box(0, 0, 1, 1), box(1, 0, 2, 1)],
        },
        crs="EPSG:4326",
    )
    write_processed_geoparquet(gdf, pq)
    code = main(
        [
            "--repo-root",
            str(repo),
            "--mode",
            "county",
            "--run-id",
            "r-graph",
            "--only",
            "graph",
            "--no-county-maps",
            "--parquet",
            str(pq),
        ],
    )
    assert code == 0
    edges = run / "counties" / "01" / "graph" / "adjacency_edges.parquet"
    meta_path = edges.with_suffix(".meta.json")
    assert edges.is_file()
    assert meta_path.is_file()
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    assert meta["county_maz"] == "01"
    assert meta["run_id"] == "r-graph"
    assert meta["graph_health"]["ok"] is True
    assert meta["graph_health"]["n_components"] == 1
    assert meta["graph_health"]["n_island_nodes"] == 0


@pytest.mark.filterwarnings(
    "ignore:The weights matrix is not fully connected:UserWarning",
)
def test_county_graph_disconnected_strict_fails(tmp_path: Path) -> None:
    repo = tmp_path
    proc = repo / "data" / "processed"
    proc.mkdir(parents=True)
    run = proc / "runs" / "r-disc"
    run.mkdir(parents=True)
    pd.DataFrame({"maz": ["01"], "n_oevk": [1]}).to_parquet(
        run / "county_oevk_counts.parquet",
        index=False,
    )
    pq = proc / "precincts.parquet"
    gdf = gpd.GeoDataFrame(
        {
            DEFAULT_PRECINCT_ID_COLUMN: ["01-001-001", "01-001-002"],
            "maz": ["01", "01"],
            "geometry": [box(0, 0, 1, 1), box(10, 10, 11, 11)],
        },
        crs="EPSG:4326",
    )
    write_processed_geoparquet(gdf, pq)
    code = main(
        [
            "--repo-root",
            str(repo),
            "--mode",
            "county",
            "--run-id",
            "r-disc",
            "--only",
            "graph",
            "--no-county-maps",
            "--parquet",
            str(pq),
        ],
    )
    assert code == 1


@pytest.mark.filterwarnings(
    "ignore:The weights matrix is not fully connected:UserWarning",
)
def test_county_graph_disconnected_allowed_writes(tmp_path: Path) -> None:
    repo = tmp_path
    proc = repo / "data" / "processed"
    proc.mkdir(parents=True)
    run = proc / "runs" / "r-disc2"
    run.mkdir(parents=True)
    pd.DataFrame({"maz": ["01"], "n_oevk": [1]}).to_parquet(
        run / "county_oevk_counts.parquet",
        index=False,
    )
    pq = proc / "precincts.parquet"
    gdf = gpd.GeoDataFrame(
        {
            DEFAULT_PRECINCT_ID_COLUMN: ["01-001-001", "01-001-002"],
            "maz": ["01", "01"],
            "geometry": [box(0, 0, 1, 1), box(10, 10, 11, 11)],
        },
        crs="EPSG:4326",
    )
    write_processed_geoparquet(gdf, pq)
    code = main(
        [
            "--repo-root",
            str(repo),
            "--mode",
            "county",
            "--run-id",
            "r-disc2",
            "--only",
            "graph",
            "--no-county-maps",
            "--allow-disconnected-county-graph",
            "--parquet",
            str(pq),
        ],
    )
    assert code == 0
    meta_path = run / "counties" / "01" / "graph" / "adjacency_edges.meta.json"
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    assert meta["graph_health"]["ok"] is False
    assert meta["graph_health"]["n_components"] == 2


def test_pilot_pipeline_graph_stage_writes_edges(tmp_path: Path) -> None:
    repo = tmp_path
    proc = repo / "data" / "processed"
    proc.mkdir(parents=True)
    pq = proc / "precincts.parquet"
    gdf = _grid_gdf()
    gdf["maz"] = "01"
    write_processed_geoparquet(gdf, pq)
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
