"""County code ordering from allocation Parquet (county mode)."""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

from hungary_ge.pipeline.county_allocation import normalize_maz


def county_maz_sequence(
    counts_parquet: Path,
    maz_filter: str | None,
    *,
    log_prefix: str = "",
    exclude_maz: frozenset[str] | None = None,
) -> list[str] | None:
    """Return sorted county codes from allocation Parquet, or ``None`` on error."""
    if not counts_parquet.is_file():
        print(
            f"{log_prefix}missing county allocation table: {counts_parquet} "
            "(run the allocation stage first)",
            file=sys.stderr,
        )
        return None
    df = pd.read_parquet(counts_parquet)
    if "maz" not in df.columns:
        print(
            f"{log_prefix}county_oevk_counts.parquet missing 'maz' column",
            file=sys.stderr,
        )
        return None
    ex = exclude_maz or frozenset()
    maz_vals = sorted({normalize_maz(m) for m in df["maz"].tolist()})
    if maz_filter is not None:
        mf = normalize_maz(maz_filter)
        if mf in ex:
            print(
                f"{log_prefix}maz {mf!r} is excluded (--exclude-maz)",
                file=sys.stderr,
            )
            return None
        if mf not in maz_vals:
            print(
                f"{log_prefix}maz {mf!r} not in county allocation table "
                f"(available: {maz_vals!r})",
                file=sys.stderr,
            )
            return None
        return [mf]
    maz_vals = [m for m in maz_vals if m not in ex]
    if not maz_vals:
        print(
            f"{log_prefix}no counties left after --exclude-maz {sorted(ex)!r}",
            file=sys.stderr,
        )
        return None
    return maz_vals
