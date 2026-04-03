"""Per-county diagnostics and partisan reports."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from hungary_ge.config import ENSEMBLE_ASSIGNMENTS_PARQUET
from hungary_ge.pipeline.context import PipelineContext
from hungary_ge.pipeline.county_reports import run_county_reports
from hungary_ge.pipeline.progress import county_tqdm
from hungary_ge.pipeline.stages.county_sequence import county_maz_sequence

NAME = "reports"


def add_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--reports-votes",
        type=Path,
        default=Path("data/processed/precinct_votes.parquet"),
        help="reports stage: precinct votes Parquet",
    )
    parser.add_argument(
        "--reports-focal",
        type=Path,
        default=Path("data/processed/focal_oevk_assignments.parquet"),
        help="reports stage: focal OEVK assignments Parquet",
    )
    parser.add_argument(
        "--reports-pop-column",
        type=str,
        default="voters",
        help="reports stage: population/weight column in votes (diagnostics)",
    )
    parser.add_argument(
        "--reports-pop-tol",
        type=float,
        default=0.25,
        help="reports stage: flag draws exceeding this max rel pop deviation",
    )
    parser.add_argument(
        "--reports-party-coding",
        type=Path,
        default=None,
        help="reports stage: partisan party coding JSON (default: packaged stub)",
    )
    parser.add_argument(
        "--reports-loose-focal",
        action="store_true",
        help="reports stage: allow units with votes but missing focal district",
    )
    parser.add_argument(
        "--reports-fail-fast",
        action="store_true",
        help="reports stage: exit on first county failure",
    )
    parser.add_argument(
        "--reports-no-smc-log-scan",
        action="store_true",
        help="reports stage: skip redist log scrape in diagnostics",
    )


def run(ctx: PipelineContext) -> int:
    args = ctx.args
    repo_root = ctx.repo_root
    paths = ctx.paths
    run_id = ctx.run_id
    exclude_maz_set = ctx.exclude_maz_set

    if args.mode != "county":
        print("reports stage requires --mode county", file=sys.stderr)
        return 2
    assert run_id is not None
    counts_pq = paths.county_oevk_counts_parquet(run_id)
    maz_list_rpt = county_maz_sequence(
        counts_pq,
        args.maz,
        log_prefix="",
        exclude_maz=exclude_maz_set,
    )
    if maz_list_rpt is None:
        return 1
    votes_path = args.reports_votes
    if not votes_path.is_absolute():
        votes_path = (repo_root / votes_path).resolve()
    focal_path = args.reports_focal
    if not focal_path.is_absolute():
        focal_path = (repo_root / focal_path).resolve()
    if not votes_path.is_file():
        print(f"reports: missing votes {votes_path}", file=sys.stderr)
        return 1
    if not focal_path.is_file():
        print(f"reports: missing focal {focal_path}", file=sys.stderr)
        return 1
    party_coding_path = (
        Path(args.reports_party_coding).resolve()
        if args.reports_party_coding is not None
        else None
    )
    if party_coding_path is not None and not party_coding_path.is_file():
        print(
            f"reports: missing party coding {party_coding_path}",
            file=sys.stderr,
        )
        return 1
    r_failures: list[str] = []
    with county_tqdm(
        maz_list_rpt,
        desc="reports",
        no_progress=args.no_progress,
    ) as pbar:
        for maz in pbar:
            prefix = f"[run {run_id} county {maz}] "
            ens_check = (
                paths.county_ensemble_dir(run_id, maz)
                / ENSEMBLE_ASSIGNMENTS_PARQUET
            )
            if not ens_check.is_file():
                pbar.set_postfix_str(f"{maz} skip", refresh=True)
                print(
                    f"{prefix}reports: skip (no {ens_check.name})",
                    file=sys.stderr,
                )
                continue
            pbar.set_postfix_str(f"{maz} reports", refresh=False)
            print(f"{prefix}stage reports: diagnostics + partisan")  # noqa: T201
            try:
                run_county_reports(
                    paths=paths,
                    run_id=run_id,
                    maz=maz,
                    votes_parquet=votes_path,
                    focal_parquet=focal_path,
                    pop_column=args.reports_pop_column,
                    pop_tol=args.reports_pop_tol,
                    party_coding=None,
                    party_coding_path=party_coding_path,
                    strict_focal_for_voting_units=not args.reports_loose_focal,
                    include_smc_log_scan=not args.reports_no_smc_log_scan,
                )
            except (OSError, ValueError) as exc:
                pbar.set_postfix_str(f"{maz} fail", refresh=True)
                print(f"{prefix}reports failed: {exc}", file=sys.stderr)
                r_failures.append(maz)
                if args.reports_fail_fast:
                    return 1
                continue
            pbar.set_postfix_str(f"{maz} done", refresh=True)
            print(f"{prefix}wrote diagnostics.json and partisan_report.json")  # noqa: T201
    if r_failures:
        print(
            f"[run {run_id}] reports: failed counties: {sorted(r_failures)!r}",
            file=sys.stderr,
        )
        return 1
    return 0
