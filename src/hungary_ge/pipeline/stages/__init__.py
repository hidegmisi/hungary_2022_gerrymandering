"""Pipeline stages: each exposes ``add_arguments(parser)`` and ``run(ctx)``."""

from __future__ import annotations

import argparse
from collections.abc import Callable

from hungary_ge.pipeline.context import PipelineContext

from . import (
    allocation_stage,
    etl_stage,
    graph_stage,
    reports_stage,
    rollup_stage,
    sample_stage,
    shared_runtime,
    viz_stage,
    votes_stage,
)

# Order matches previous monolithic CLI (core → ETL → graph → viz → runtime → downstream).
_ARGUMENT_REGISTRARS: tuple[Callable[[argparse.ArgumentParser], None], ...] = (
    etl_stage.add_arguments,
    graph_stage.add_arguments,
    viz_stage.add_arguments,
    shared_runtime.add_arguments,
    sample_stage.add_arguments,
    reports_stage.add_arguments,
    rollup_stage.add_arguments,
    votes_stage.add_arguments,
    allocation_stage.add_arguments,
)

STAGE_RUNNERS: dict[str, Callable[[PipelineContext], int]] = {
    etl_stage.NAME: etl_stage.run,
    votes_stage.NAME: votes_stage.run,
    allocation_stage.NAME: allocation_stage.run,
    graph_stage.NAME: graph_stage.run,
    viz_stage.NAME: viz_stage.run,
    sample_stage.NAME: sample_stage.run,
    reports_stage.NAME: reports_stage.run,
    rollup_stage.NAME: rollup_stage.run,
}


def register_stage_arguments(parser: argparse.ArgumentParser) -> None:
    for add in _ARGUMENT_REGISTRARS:
        add(parser)
