"""Derive enacted county-level OEVK district counts from focal precinct assignments.

Slice A of the county-segregated pipeline: canonical table from
``focal_oevk_assignments.parquet`` only, with hard validation against 20 megye
codes and 106 national districts.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from hungary_ge.config import (
    COUNTY_OEVK_COUNTS_META,
    COUNTY_OEVK_COUNTS_PARQUET,
)
from hungary_ge.io.electoral_etl import (
    assert_focal_assignments_valid,
    load_focal_assignments,
)
from hungary_ge.problem import DEFAULT_NDISTS

# NVI / data-model megye folder codes (19 counties + Budapest).
CANONICAL_MEGYE_CODES: tuple[str, ...] = tuple(f"{i:02d}" for i in range(1, 21))

_COUNT_COL = "n_oevk"
_MAZ_COL = "maz"


def normalize_maz(value: str | int | float) -> str:
    """Two-digit megye code string (zero-padded if numeric)."""
    s = str(value).strip()
    if s.isdigit():
        return s.zfill(2)
    return s


def derive_county_oevk_counts_from_focal(
    focal: pd.DataFrame,
    *,
    national_enacted_ndists: int = DEFAULT_NDISTS,
    canonical_megye_codes: tuple[str, ...] = CANONICAL_MEGYE_CODES,
) -> pd.DataFrame:
    """Return one row per megye with enacted district count (unique ``oevk_id_full``).

    Validates:
        * Focal table integrity (:func:`assert_focal_assignments_valid`).
        * ``maz`` column present; normalized codes match ``canonical_megye_codes``
          exactly (no missing / extra counties).
        * Each ``oevk_id_full`` belongs to exactly one ``maz``.
        * National distinct ``oevk_enacted_ndists`` count matches ``national_enacted_ndists``.
        * Sum of per-county distinct counts matches ``national_enacted_ndists`` (follows
          from single-county membership).

    Columns: ``maz``, ``n_oevk`` sorted by ``maz``.
    """
    if focal.empty:
        msg = "focal assignments table is empty"
        raise ValueError(msg)
    assert_focal_assignments_valid(focal)
    if _MAZ_COL not in focal.columns:
        msg = f"focal table missing {_MAZ_COL!r}"
        raise ValueError(msg)

    work = focal.copy()
    work[_MAZ_COL] = work[_MAZ_COL].map(normalize_maz)

    n_global = int(work["oevk_id_full"].nunique())
    if n_global != national_enacted_ndists:
        msg = (
            f"national distinct oevk_id_full count is {n_global}, "
            f"expected {national_enacted_ndists}"
        )
        raise ValueError(msg)

    maz_per_oevk = work.groupby("oevk_id_full", sort=False)[_MAZ_COL].nunique()
    if (maz_per_oevk > 1).any():
        bad = maz_per_oevk[maz_per_oevk > 1].index.tolist()[:5]
        msg = f"oevk_id_full appears under more than one maz (examples: {bad!r})"
        raise ValueError(msg)

    expected = frozenset(canonical_megye_codes)
    present = frozenset(work[_MAZ_COL].unique())
    missing = sorted(expected - present)
    extra = sorted(present - expected)
    if missing:
        msg = f"missing megye codes in focal maz: {missing!r}"
        raise ValueError(msg)
    if extra:
        msg = f"unexpected megye codes in focal maz: {extra!r}"
        raise ValueError(msg)

    grouped = (
        work.groupby(_MAZ_COL, sort=False)["oevk_id_full"]
        .nunique()
        .rename(_COUNT_COL)
        .reset_index()
    )
    grouped = grouped.sort_values(_MAZ_COL, kind="mergesort").reset_index(drop=True)

    total = int(grouped[_COUNT_COL].sum())
    if total != national_enacted_ndists:
        msg = f"sum of county n_oevk is {total}, expected {national_enacted_ndists}"
        raise ValueError(msg)

    if len(grouped) != len(canonical_megye_codes):
        # Defensive: duplicate maz after normalize should not happen if present==expected
        dup_maz = grouped[_MAZ_COL].duplicated()
        if dup_maz.any():
            msg = f"duplicate {_MAZ_COL} rows after aggregation"
            raise ValueError(msg)
        msg = f"expected {len(canonical_megye_codes)} counties, got {len(grouped)}"
        raise ValueError(msg)

    return grouped


def _build_meta(
    *,
    focal_path: str,
    n_focal_rows: int,
    national_enacted_ndists: int,
    counts_df: pd.DataFrame,
    canonical_megye_codes: tuple[str, ...],
) -> dict[str, Any]:
    utc_now = datetime.now(UTC)
    return {
        "artifact": "county_oevk_counts",
        "created_at_utc": utc_now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "focal_source": focal_path,
        "n_focal_rows": n_focal_rows,
        "national_enacted_ndists": national_enacted_ndists,
        "n_counties": len(counts_df),
        "sum_n_oevk": int(counts_df[_COUNT_COL].sum()),
        "canonical_megye_codes": list(canonical_megye_codes),
    }


def write_county_oevk_counts(
    run_dir: str | Path,
    focal_path: str | Path,
    *,
    national_enacted_ndists: int = DEFAULT_NDISTS,
    canonical_megye_codes: tuple[str, ...] = CANONICAL_MEGYE_CODES,
) -> tuple[Path, Path]:
    """Write ``county_oevk_counts.parquet`` and ``.meta.json`` under ``run_dir``.

    Returns:
        ``(parquet_path, meta_path)``.
    """
    run_dir = Path(run_dir)
    focal_path = Path(focal_path)
    focal_df = load_focal_assignments(focal_path)
    counts_df = derive_county_oevk_counts_from_focal(
        focal_df,
        national_enacted_ndists=national_enacted_ndists,
        canonical_megye_codes=canonical_megye_codes,
    )
    run_dir.mkdir(parents=True, exist_ok=True)
    pq_out = run_dir / COUNTY_OEVK_COUNTS_PARQUET
    meta_out = run_dir / COUNTY_OEVK_COUNTS_META
    counts_df.to_parquet(pq_out, index=False)
    meta = _build_meta(
        focal_path=str(focal_path.as_posix()),
        n_focal_rows=len(focal_df),
        national_enacted_ndists=national_enacted_ndists,
        counts_df=counts_df,
        canonical_megye_codes=canonical_megye_codes,
    )
    meta_out.write_text(json.dumps(meta, indent=2) + "\n", encoding="utf-8")
    return pq_out, meta_out
