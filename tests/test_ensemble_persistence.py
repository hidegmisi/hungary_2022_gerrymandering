"""Parquet persistence for PlanEnsemble (Slice 7)."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pytest

from hungary_ge.ensemble import PlanEnsemble
from hungary_ge.ensemble.persistence import (
    ENSEMBLE_MANIFEST_SCHEMA_V1,
    MAX_WIDE_DRAWS_DEFAULT,
    load_plan_ensemble,
    load_plan_ensemble_draw_column,
    save_plan_ensemble,
)


def test_to_long_frame_row_count() -> None:
    e = PlanEnsemble.from_columns(
        ("a", "b", "gap-01"),
        ([1, 2, 1], [1, 1, 2], [2, 2, 1]),
        draw_ids=(5, 6, 7),
        chain_or_run=(1, 1, 2),
    )
    df = e.to_long_frame()
    assert len(df) == e.n_units * e.n_draws
    assert set(df["precinct_id"]) == {"a", "b", "gap-01"}
    assert sorted(df["draw"].unique().tolist()) == [5, 6, 7]


def test_roundtrip_long_metadata_and_chain(tmp_path: Path) -> None:
    e = PlanEnsemble.from_columns(
        ("p1", "p2"),
        ([1, 2], [2, 1], [1, 1]),
        draw_ids=(100, 101, 102),
        chain_or_run=(1, 2, 2),
        metadata={"seed": 42, "sampler": "redist"},
    )
    p = tmp_path / "ens.parquet"
    save_plan_ensemble(e, p, write_sha256=False)
    assert (tmp_path / "ens.meta.json").is_file()
    meta = json.loads((tmp_path / "ens.meta.json").read_text(encoding="utf-8"))
    assert meta["schema_version"] == ENSEMBLE_MANIFEST_SCHEMA_V1
    assert "sha256" not in meta
    e2 = load_plan_ensemble(p)
    assert e2.unit_ids == e.unit_ids
    assert e2.assignments == e.assignments
    assert e2.draw_ids == e.draw_ids
    assert e2.chain_or_run == e.chain_or_run
    assert dict(e2.metadata) == dict(e.metadata)


def test_roundtrip_long_with_sha256(tmp_path: Path) -> None:
    e = PlanEnsemble.from_columns(("x",), ([1], [2]))
    p = tmp_path / "e.parquet"
    save_plan_ensemble(e, p, write_sha256=True)
    meta = json.loads((tmp_path / "e.meta.json").read_text(encoding="utf-8"))
    assert "sha256" in meta
    assert len(meta["sha256"]) == 64


def test_load_plan_ensemble_draw_column_matches_column(tmp_path: Path) -> None:
    e = PlanEnsemble.from_columns(
        ("c", "a", "b"),
        ([2, 1, 2], [1, 1, 2]),
        draw_ids=(1, 2),
    )
    p = tmp_path / "long.parquet"
    save_plan_ensemble(e, p, write_sha256=False)
    full = load_plan_ensemble(p)
    col0 = load_plan_ensemble_draw_column(p, 1)
    col1 = load_plan_ensemble_draw_column(p, 2)
    assert np.array_equal(col0, np.array([2, 1, 2], dtype=np.int32))
    assert np.array_equal(col1, np.array([1, 1, 2], dtype=np.int32))
    for i in range(e.n_units):
        assert full.assignments[i][0] == int(col0[i])
        assert full.assignments[i][1] == int(col1[i])


def test_roundtrip_wide_small(tmp_path: Path) -> None:
    e = PlanEnsemble.from_columns(
        ("a", "z"),
        ([1, 1], [2, 2], [1, 2]),
        draw_ids=(7, 8, 9),
    )
    p = tmp_path / "w.parquet"
    save_plan_ensemble(e, p, layout="wide", write_sha256=False)
    e2 = load_plan_ensemble(p)
    assert e2.unit_ids == e.unit_ids
    assert e2.assignments == e.assignments
    assert e2.draw_ids == e.draw_ids


def test_wide_rejects_large_n_draws(tmp_path: Path) -> None:
    n_draws = MAX_WIDE_DRAWS_DEFAULT + 1
    plan_cols = tuple([1, 1] for _ in range(n_draws))
    e = PlanEnsemble.from_columns(("u0", "u1"), plan_cols)
    p = tmp_path / "big.parquet"
    with pytest.raises(ValueError, match="wide layout"):
        save_plan_ensemble(e, p, layout="wide")


def test_manifest_schema_version_enforced(tmp_path: Path) -> None:
    p = tmp_path / "x.parquet"
    e = PlanEnsemble.from_columns(("a",), ([1],))
    save_plan_ensemble(e, p, write_sha256=False)
    mp = tmp_path / "x.meta.json"
    meta = json.loads(mp.read_text(encoding="utf-8"))
    meta["schema_version"] = "bogus"
    mp.write_text(json.dumps(meta), encoding="utf-8")
    with pytest.raises(ValueError, match="schema_version"):
        load_plan_ensemble(p)


def test_load_long_without_manifest_sorts_ids(tmp_path: Path) -> None:
    """Manifest optional: unit order from lexicographic sort of precinct_id."""
    # Manually write long parquet only (no manifest)
    import pandas as pd

    rows = [
        {"precinct_id": "b", "draw": 1, "district": 2},
        {"precinct_id": "a", "draw": 1, "district": 1},
        {"precinct_id": "b", "draw": 2, "district": 1},
        {"precinct_id": "a", "draw": 2, "district": 2},
    ]
    df = pd.DataFrame(rows)
    p = tmp_path / "nometa.parquet"
    df.to_parquet(p, index=False)
    e = load_plan_ensemble(p)
    assert e.unit_ids == ("a", "b")
    assert e.assignments[0] == (1, 2)
    assert e.assignments[1] == (2, 1)
