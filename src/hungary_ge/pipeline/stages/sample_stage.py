"""Per-county redist SMC sampling stage."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from hungary_ge.config import ENSEMBLE_ASSIGNMENTS_PARQUET
from hungary_ge.pipeline.context import PipelineContext
from hungary_ge.pipeline.county_sample import (
    county_ndists_by_maz,
    run_county_redist_sample,
)
from hungary_ge.pipeline.graph_build import adjacency_options_from_graph_cli
from hungary_ge.pipeline.progress import county_tqdm
from hungary_ge.pipeline.stages.county_sequence import county_maz_sequence
from hungary_ge.sampling.redist_adapter import RedistBackendError

NAME = "sample"


def add_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--sample-n-draws",
        type=int,
        default=4,
        help="sample stage: SMC draws per county (redist nsims)",
    )
    parser.add_argument(
        "--sample-n-runs",
        type=int,
        default=1,
        help="sample stage: parallel SMC runs (redist nruns)",
    )
    parser.add_argument(
        "--sample-seed",
        type=int,
        default=None,
        help="sample stage: optional RNG seed for redist",
    )
    parser.add_argument(
        "--sample-pop-column",
        type=str,
        default="voters",
        help="sample stage: numeric weight column on precinct layer (redist pop)",
    )
    parser.add_argument(
        "--sample-pop-tol",
        type=float,
        default=0.25,
        help="sample stage: max relative population deviation per district",
    )
    parser.add_argument(
        "--sample-compactness",
        type=float,
        default=1.0,
        help="sample stage: redist compactness weight",
    )
    parser.add_argument(
        "--sample-rscript-path",
        type=Path,
        default=None,
        help="sample stage: override path to r/redist/run_smc.R",
    )
    parser.add_argument(
        "--sample-fail-fast",
        action="store_true",
        help="sample stage: exit on first county failure (default: run all counties, then fail if any error)",
    )
    parser.add_argument(
        "--sample-skip-existing",
        action="store_true",
        help=(
            "sample stage: skip a county when ensemble_assignments.parquet already exists "
            "(resume long multi-county runs)"
        ),
    )


def run(ctx: PipelineContext) -> int:
    args = ctx.args
    paths = ctx.paths
    pq_graph = ctx.pq_graph
    run_id = ctx.run_id
    exclude_maz_set = ctx.exclude_maz_set

    if args.mode != "county":
        print("sample stage requires --mode county", file=sys.stderr)
        return 2
    if not pq_graph.is_file():
        print(f"Missing precinct layer for sample: {pq_graph}", file=sys.stderr)
        return 1
    assert run_id is not None
    counts_pq = paths.county_oevk_counts_parquet(run_id)
    maz_list_s = county_maz_sequence(
        counts_pq,
        args.maz,
        log_prefix="",
        exclude_maz=exclude_maz_set,
    )
    if maz_list_s is None:
        return 1
    try:
        nd_map = county_ndists_by_maz(counts_pq)
    except ValueError as exc:
        print(f"sample: {exc}", file=sys.stderr)
        return 1
    adj_sample = adjacency_options_from_graph_cli(args)
    rs_path = (
        Path(args.sample_rscript_path).resolve()
        if args.sample_rscript_path is not None
        else None
    )
    failures: list[str] = []
    with county_tqdm(
        maz_list_s,
        desc="sample",
        no_progress=args.no_progress,
    ) as pbar:
        for maz in pbar:
            prefix = f"[run {run_id} county {maz}] "
            ens_existing = (
                paths.county_ensemble_dir(run_id, maz)
                / ENSEMBLE_ASSIGNMENTS_PARQUET
            )
            if args.sample_skip_existing and ens_existing.is_file():
                pbar.set_postfix_str(f"{maz} skip", refresh=True)
                print(f"{prefix}sample: skip existing {ens_existing.name}")  # noqa: T201
                continue
            ndists_c = nd_map[maz]
            pbar.set_postfix_str(f"{maz} SMC ndists={ndists_c}", refresh=False)
            print(f"{prefix}stage sample: redist SMC, ndists={ndists_c}")  # noqa: T201
            try:
                run_county_redist_sample(
                    precinct_parquet=pq_graph,
                    paths=paths,
                    run_id=run_id,
                    maz=maz,
                    ndists=ndists_c,
                    pop_column=args.sample_pop_column,
                    adj_opts=adj_sample,
                    n_draws=args.sample_n_draws,
                    n_runs=args.sample_n_runs,
                    seed=args.sample_seed,
                    pop_tol=args.sample_pop_tol,
                    compactness=args.sample_compactness,
                    rscript_path=rs_path,
                    strict_county_connectivity=not args.allow_disconnected_county_graph,
                    log_prefix=prefix,
                    redist_progress=not args.no_progress,
                )
            except (ValueError, RedistBackendError) as exc:
                pbar.set_postfix_str(f"{maz} fail", refresh=True)
                print(f"{prefix}sample failed: {exc}", file=sys.stderr)
                failures.append(maz)
                if args.sample_fail_fast:
                    return 1
                continue
            pbar.set_postfix_str(f"{maz} done", refresh=True)
            print(f"{prefix}wrote ensemble_assignments.parquet")  # noqa: T201
    if failures:
        print(
            f"[run {run_id}] sample: failed counties: {sorted(failures)!r}",
            file=sys.stderr,
        )
        return 1
    return 0
