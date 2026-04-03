"""Tests for county OEVK count derivation from focal assignments."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from hungary_ge.config import COUNTY_OEVK_COUNTS_META, COUNTY_OEVK_COUNTS_PARQUET
from hungary_ge.pipeline.county_allocation import (
    CANONICAL_MEGYE_CODES,
    derive_county_oevk_counts_from_focal,
    normalize_maz,
    write_county_oevk_counts,
)


def _focal_from_count_map(
    counts_by_maz: dict[str, int],
    *,
    national_ndists: int = 106,
) -> pd.DataFrame:
    """Build minimal focal rows: one precinct per enacted district."""
    rows: list[dict[str, str]] = []
    total = sum(counts_by_maz.values())
    if total != national_ndists:
        msg = "counts must sum to national_ndists"
        raise ValueError(msg)
    for maz, n in sorted(counts_by_maz.items()):
        for k in range(1, n + 1):
            pid = f"{maz}-tst-{k:03d}"
            oevk = f"{maz}{k:02d}"
            rows.append(
                {"precinct_id": pid, "oevk_id_full": oevk, "maz": maz},
            )
    return pd.DataFrame(rows)


def _official_style_counts() -> dict[str, int]:
    """106 seats across 20 megye (illustrative, satisfies sum only)."""
    codes = list(CANONICAL_MEGYE_CODES)
    base, rem = divmod(106, len(codes))
    out: dict[str, int] = {}
    for i, maz in enumerate(codes):
        out[maz] = base + (1 if i < rem else 0)
    assert sum(out.values()) == 106  # noqa: S101
    return out


def test_normalize_maz() -> None:
    assert normalize_maz(1) == "01"
    assert normalize_maz("9") == "09"
    assert normalize_maz("12") == "12"


def test_derive_county_oevk_counts_happy() -> None:
    counts_map = _official_style_counts()
    focal = _focal_from_count_map(counts_map)
    out = derive_county_oevk_counts_from_focal(focal)
    assert list(out.columns) == ["maz", "n_oevk"]
    assert len(out) == 20
    by_maz = dict(zip(out["maz"].tolist(), out["n_oevk"].tolist(), strict=True))
    assert by_maz == counts_map


def test_derive_rejects_missing_megye() -> None:
    counts_map = _official_style_counts()
    lost = counts_map.pop("20")
    counts_map["19"] = counts_map["19"] + lost
    focal = _focal_from_count_map(counts_map)
    with pytest.raises(ValueError, match="missing megye"):
        derive_county_oevk_counts_from_focal(focal)


def test_derive_rejects_multi_maz_oevk() -> None:
    counts_map = _official_style_counts()
    focal = _focal_from_count_map(counts_map)
    extra = focal.iloc[[0]].copy()
    extra["maz"] = "02"
    extra["precinct_id"] = "02-extra-000"
    focal2 = pd.concat([focal, extra], ignore_index=True)
    with pytest.raises(ValueError, match="more than one maz"):
        derive_county_oevk_counts_from_focal(focal2)


def test_derive_rejects_wrong_national_uniques(tmp_path: Path) -> None:
    counts_map = {maz: 1 for maz in CANONICAL_MEGYE_CODES}
    focal = _focal_from_count_map(counts_map, national_ndists=20)
    with pytest.raises(ValueError, match="national distinct"):
        derive_county_oevk_counts_from_focal(focal)


def test_write_county_oevk_counts_roundtrip(tmp_path: Path) -> None:
    counts_map = _official_style_counts()
    focal = _focal_from_count_map(counts_map)
    fp = tmp_path / "focal.parquet"
    focal.to_parquet(fp, index=False)
    run_dir = tmp_path / "runs" / "smoke-1"
    pq_out, meta_out = write_county_oevk_counts(run_dir, fp)
    assert pq_out.name == COUNTY_OEVK_COUNTS_PARQUET
    assert meta_out.name == COUNTY_OEVK_COUNTS_META
    again = pd.read_parquet(pq_out)
    pd.testing.assert_frame_equal(again, derive_county_oevk_counts_from_focal(focal))
    meta = json.loads(meta_out.read_text(encoding="utf-8"))
    assert meta["artifact"] == "county_oevk_counts"
    assert meta["sum_n_oevk"] == 106
    assert meta["n_counties"] == 20
