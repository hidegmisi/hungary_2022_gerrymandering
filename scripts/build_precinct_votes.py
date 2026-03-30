#!/usr/bin/env python3
"""Build ``precinct_votes.parquet`` and ``focal_oevk_assignments.parquet`` from ``szavkor_topo``.

Run from the repository root::

    uv run python scripts/build_precinct_votes.py

Uses the default list map at ``src/hungary_ge/io/data/election_2022_list_map.json``.
Override with ``--party-map``. See ``docs/data-model.md`` (Electoral tables).
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from hungary_ge.config import ProcessedPaths
from hungary_ge.io.electoral_etl import (
    build_electoral_tables,
    default_list_party_map_path,
    write_electoral_parquets,
)


def _write_manifest(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Extract list votes and focal OEVK ids from szavkor_topo JSON to Parquet.",
    )
    parser.add_argument(
        "--szavkor-root",
        type=Path,
        default=Path("data/raw/szavkor_topo"),
        help="Root folder with {maz}/{maz}-{taz}.json layout",
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=Path.cwd(),
        help="Repository root for default output paths",
    )
    parser.add_argument(
        "--party-map",
        type=Path,
        default=None,
        help="List ID → column JSON (default: package stub next to electoral_etl)",
    )
    parser.add_argument(
        "--out-votes",
        type=Path,
        default=None,
        help="Output path for precinct_votes.parquet",
    )
    parser.add_argument(
        "--out-focal",
        type=Path,
        default=None,
        help="Output path for focal_oevk_assignments.parquet",
    )
    parser.add_argument(
        "--strict-unknown-lists",
        action="store_true",
        help="Fail if listVotes contains an id not in the party map",
    )
    parser.add_argument(
        "--manifest",
        type=Path,
        default=None,
        help="Write build stats JSON (default: manifests/precinct_votes_etl.json)",
    )
    parser.add_argument(
        "--no-manifest",
        action="store_true",
        help="Do not write a manifest",
    )
    args = parser.parse_args()
    repo = args.repo_root
    paths = ProcessedPaths(repo)
    party_map = (
        args.party_map if args.party_map is not None else default_list_party_map_path()
    )
    out_votes = args.out_votes or paths.precinct_votes_parquet
    out_focal = args.out_focal or paths.focal_oevk_assignments_parquet

    votes_df, focal_df, stats = build_electoral_tables(
        args.szavkor_root,
        party_map,
        strict_unknown_lists=args.strict_unknown_lists,
    )
    write_electoral_parquets(votes_df, focal_df, out_votes, out_focal)

    manifest_path = args.manifest
    if manifest_path is None and not args.no_manifest:
        manifest_path = paths.manifests_dir / "precinct_votes_etl.json"
    if manifest_path is not None:
        payload: dict[str, Any] = {
            "szavkor_root": str(args.szavkor_root.resolve()),
            "party_map": str(party_map.resolve()),
            "out_votes": str(out_votes.resolve()),
            "out_focal": str(out_focal.resolve()),
            "stats": {
                "n_files_read": stats.n_files_read,
                "n_records_in": stats.n_records_in,
                "n_rows_votes": stats.n_rows_votes,
                "n_rows_focal": stats.n_rows_focal,
                "n_duplicate_precinct_id": stats.n_duplicate_precinct_id,
                "n_records_missing_oevk_full": stats.n_records_missing_oevk_full,
                "unknown_list_vote_keys": dict(stats.unknown_list_vote_keys),
                "n_warnings": len(stats.warnings),
            },
        }
        _write_manifest(manifest_path, payload)

    print(
        f"Wrote {len(votes_df)} vote rows → {out_votes}",
        file=sys.stderr,
    )
    print(
        f"Wrote {len(focal_df)} focal rows → {out_focal}",
        file=sys.stderr,
    )
    if stats.warnings:
        for w in stats.warnings[:20]:
            print(f"warning: {w}", file=sys.stderr)
        if len(stats.warnings) > 20:
            print(f"... and {len(stats.warnings) - 20} more warnings", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
