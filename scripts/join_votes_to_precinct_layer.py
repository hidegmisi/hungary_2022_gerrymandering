#!/usr/bin/env python3
"""Left-join ``precinct_votes`` onto a precinct GeoParquet (e.g. void-hex layer).

``redist`` sampling expects a population column (default ``voters``) on the same
GeoParquet used for county graphs. Void-hex ETL does not embed votes; run votes
ETL first, then this script, then county ``graph`` / ``sample``.

Example::

    uv run python scripts/join_votes_to_precinct_layer.py \\
      --precinct-parquet data/processed/precincts_void_hex.parquet \\
      --votes-parquet data/processed/precinct_votes.parquet \\
      --out-parquet data/processed/precincts_void_hex_voters.parquet
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

from hungary_ge.io import join_electoral_to_gdf, load_processed_geoparquet


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--precinct-parquet",
        type=Path,
        required=True,
        help="Input precinct GeoParquet (geometry + precinct_id)",
    )
    parser.add_argument(
        "--votes-parquet",
        type=Path,
        default=Path("data/processed/precinct_votes.parquet"),
        help="Precinct votes table from votes ETL",
    )
    parser.add_argument(
        "--out-parquet",
        type=Path,
        required=True,
        help="Output GeoParquet with vote columns joined",
    )
    parser.add_argument(
        "--require-voters",
        action="store_true",
        help="Exit with error if joined layer has no 'voters' column",
    )
    args = parser.parse_args(argv)

    if not args.precinct_parquet.is_file():
        print(f"missing precinct layer: {args.precinct_parquet}", file=sys.stderr)
        return 1
    if not args.votes_parquet.is_file():
        print(f"missing votes table: {args.votes_parquet}", file=sys.stderr)
        return 1

    gdf = load_processed_geoparquet(args.precinct_parquet)
    votes = pd.read_parquet(args.votes_parquet)
    merged = join_electoral_to_gdf(gdf, votes)
    # Void / gap units have vote totals cleared to NA; redist requires non-missing pop.
    if "voters" in merged.columns:
        merged["voters"] = (
            pd.to_numeric(merged["voters"], errors="coerce").fillna(0).astype("int64")
        )
    args.out_parquet.parent.mkdir(parents=True, exist_ok=True)
    merged.to_parquet(args.out_parquet, index=False)
    print(f"Wrote {args.out_parquet.resolve()} ({len(merged)} rows)")  # noqa: T201

    if args.require_voters and "voters" not in merged.columns:
        print("joined layer has no 'voters' column", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
