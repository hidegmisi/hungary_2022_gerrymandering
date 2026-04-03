"""Orchestrate pipeline stages via :mod:`hungary_ge.pipeline.stages`.

Default stages: ``etl``, ``votes``, ``graph``. ``viz`` is opt-in (needs ``viz`` extra).

``--mode county`` uses ``--run-id`` and per-county work under
``data/processed/runs/<run-id>/counties/<maz>/`` for ``graph``, ``viz``, and
``sample`` (``redist`` SMC → ``ensemble/ensemble_assignments.parquet``), and
``reports`` (``diagnostics.json`` + ``partisan_report.json`` per county), and
``rollup`` (``national_report.json``).
"""

from __future__ import annotations

import sys
from pathlib import Path

from hungary_ge.config import ProcessedPaths
from hungary_ge.pipeline.cli import build_parser
from hungary_ge.pipeline.context import PipelineContext
from hungary_ge.pipeline.county_allocation import normalize_maz
from hungary_ge.pipeline.profiles import apply_pipeline_profile
from hungary_ge.pipeline.run_manifest import write_pipeline_run_manifest
from hungary_ge.pipeline.stages import STAGE_RUNNERS
from hungary_ge.pipeline.stages.core import DEFAULT_STAGES
from hungary_ge.pipeline.validate import validate_pipeline_args


def _build_context(args: object, repo_root: Path) -> PipelineContext:
    stages = list(dict.fromkeys(args.only or DEFAULT_STAGES))
    pq_arg = args.parquet
    if not pq_arg.is_absolute():
        pq_graph = (repo_root / pq_arg).resolve()
    else:
        pq_graph = pq_arg.resolve()

    paths = ProcessedPaths(repo_root)
    exclude_maz_set: frozenset[str] | None = None
    if args.exclude_maz:
        exclude_maz_set = frozenset(
            normalize_maz(x) for x in args.exclude_maz if str(x).strip()
        )

    szavkor = args.szavkor_root
    if not szavkor.is_absolute():
        szavkor = (repo_root / szavkor).resolve()

    return PipelineContext(
        args=args,
        repo_root=repo_root,
        paths=paths,
        pq_graph=pq_graph,
        stages=stages,
        exclude_maz_set=exclude_maz_set,
        run_id=args.run_id,
        szavkor=szavkor,
    )


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    apply_pipeline_profile(args)

    repo_root = args.repo_root.resolve()
    err = validate_pipeline_args(args, repo_root)
    if err is not None:
        return err

    ctx = _build_context(args, repo_root)
    stages = ctx.stages
    paths = ctx.paths
    pq_graph = ctx.pq_graph

    for name in stages:
        runner = STAGE_RUNNERS.get(name)
        if runner is None:
            print(f"unknown stage: {name!r}", file=sys.stderr)
            return 2
        code = runner(ctx)
        if code != 0:
            return code

    argv_effective = list(sys.argv[1:] if argv is None else argv)
    manifest_paths: list[Path] = []
    ae = paths.adjacency_edges_parquet
    if ae.is_file():
        manifest_paths.append(ae)
    try:
        mp = write_pipeline_run_manifest(
            repo_root,
            argv=argv_effective,
            stages_run=stages,
            pq_graph=pq_graph,
            extra_output_paths=manifest_paths,
        )
        print(f"Wrote run manifest {mp.name}")  # noqa: T201
    except OSError as exc:
        print(f"warning: could not write run manifest: {exc}", file=sys.stderr)
    return 0


def main_entry() -> None:
    """Setuptools / ``uv run hungary-ge-pipeline`` console_script target."""
    raise SystemExit(main())
