"""Subprocess adapter for R `redist` SMC (Slice 6)."""

from __future__ import annotations

import shutil
import subprocess
import tempfile
from collections.abc import Sequence
from dataclasses import replace
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from geopandas import GeoDataFrame

from hungary_ge.constraints.constraint_spec import ConstraintSpec
from hungary_ge.ensemble.plan_ensemble import PlanEnsemble
from hungary_ge.graph.adjacency_graph import AdjacencyGraph
from hungary_ge.problem.oevk_problem import OevkProblem
from hungary_ge.sampling.redist_export import (
    ASSIGNMENTS_CSV_NAME,
    export_redist_bundle,
)
from hungary_ge.sampling.sampler_config import SamplerConfig, SamplerResult


class RedistBackendError(RuntimeError):
    """Raised when the `redist` / `Rscript` pipeline fails."""


def default_run_smc_path() -> Path:
    """Path to `r/redist/run_smc.R` relative to the repository root."""
    return Path(__file__).resolve().parents[3] / "r" / "redist" / "run_smc.R"


def run_redist_smc(
    config: SamplerConfig,
    bundle_dir: Path,
    rscript_path: Path | None = None,
) -> SamplerResult:
    """Invoke ``Rscript run_smc.R <bundle_dir>`` and capture logs under ``bundle_dir``."""
    bundle_dir = bundle_dir.resolve()
    script = rscript_path or default_run_smc_path()
    if not script.is_file():
        msg = f"run_smc.R not found at {script}"
        raise RedistBackendError(msg)

    exe = shutil.which("Rscript")
    if exe is None:
        raise RedistBackendError(
            "Rscript not found on PATH; install R or add it to PATH"
        )

    stdout_path = bundle_dir / "redist_stdout.log"
    stderr_path = bundle_dir / "redist_stderr.log"
    cmd = [exe, str(script), str(bundle_dir)]

    with (
        stdout_path.open("w", encoding="utf-8") as out,
        stderr_path.open("w", encoding="utf-8") as err,
    ):
        proc = subprocess.run(
            cmd,
            stdout=out,
            stderr=err,
            check=False,
            shell=False,
        )

    assign_path = bundle_dir / ASSIGNMENTS_CSV_NAME
    assign_ok = assign_path.is_file() and proc.returncode == 0
    return SamplerResult(
        exit_code=proc.returncode,
        stdout_path=stdout_path,
        stderr_path=stderr_path,
        assignments_csv=assign_path if assign_ok else None,
        ensemble=None,
        diagnostics={"cmd": cmd},
    )


def load_ensemble_from_redist_csv(
    path: Path | str,
    unit_ids: Sequence[str],
    *,
    metadata: dict[str, Any] | None = None,
) -> PlanEnsemble:
    """Load long-format ``assignments.csv`` from ``run_smc.R``."""
    p = Path(path)
    df = pd.read_csv(p)
    need = {"unit_index", "draw", "district"}
    if not need.issubset(df.columns):
        msg = f"CSV must contain columns {sorted(need)}, got {list(df.columns)}"
        raise ValueError(msg)

    uid = tuple(unit_ids)
    n_u = len(uid)
    draws = sorted(int(x) for x in df["draw"].unique())
    cols: list[list[int]] = []
    chain_vals: list[int] = []
    have_chain = "chain" in df.columns
    for d in draws:
        sub = df.loc[df["draw"] == d].sort_values("unit_index")
        if len(sub) != n_u:
            msg = f"draw {d}: expected {n_u} rows, got {len(sub)}"
            raise ValueError(msg)
        idx = sub["unit_index"].to_numpy(dtype=np.int64)
        if not np.array_equal(idx, np.arange(n_u, dtype=np.int64)):
            msg = f"draw {d}: unit_index must be 0..{n_u - 1} in order"
            raise ValueError(msg)
        cols.append(sub["district"].astype(int).tolist())
        if have_chain:
            chain_vals.append(int(sub["chain"].iloc[0]))

    ch_tuple = (
        tuple(chain_vals) if have_chain and len(chain_vals) == len(draws) else None
    )
    return PlanEnsemble.from_columns(
        uid,
        cols,
        draw_ids=tuple(draws),
        chain_or_run=ch_tuple,
        metadata=dict(metadata or {}),
    )


def run_redist_pipeline(
    gdf: GeoDataFrame,
    graph: AdjacencyGraph,
    problem: OevkProblem,
    config: SamplerConfig,
    *,
    constraint_spec: ConstraintSpec | None = None,
    bundle_dir: Path | None = None,
    rscript_path: Path | None = None,
) -> PlanEnsemble:
    """Export bundle, run SMC, return :class:`PlanEnsemble` aligned with ``graph.order``."""
    if problem.pop_column is None:
        msg = "OevkProblem.pop_column must be set for redist population balance"
        raise ValueError(msg)

    eff = replace(
        config,
        pop_tol=(
            constraint_spec.elector_balance.max_relative_deviation
            if constraint_spec is not None
            else config.pop_tol
        ),
    )
    run_root = bundle_dir
    if run_root is None:
        run_root = Path(tempfile.mkdtemp(prefix="hungary_ge_redist_"))
    run_root = run_root.resolve()
    run_root.mkdir(parents=True, exist_ok=True)

    export_redist_bundle(
        gdf,
        graph,
        config=eff,
        run_dir=run_root,
        ndists=problem.ndists,
        precinct_id_column=problem.precinct_id_column,
        pop_column=problem.pop_column,
    )

    result = run_redist_smc(eff, run_root, rscript_path=rscript_path)
    if result.exit_code != 0:
        log = ""
        if result.stderr_path and result.stderr_path.is_file():
            log = result.stderr_path.read_text(encoding="utf-8", errors="replace")[
                -4000:
            ]
        raise RedistBackendError(
            f"redist_smc failed (exit {result.exit_code}). stderr tail:\n{log}"
        )
    if result.assignments_csv is None:
        raise RedistBackendError("redist_smc produced no assignments CSV")

    meta = {
        "sampler": "redist_smc",
        "bundle_dir": str(run_root),
        "seed": eff.seed,
        "n_sims": eff.n_sims,
        "n_runs": eff.n_runs,
        "pop_tol": eff.pop_tol,
    }
    return load_ensemble_from_redist_csv(
        result.assignments_csv,
        graph.order.ids,
        metadata=meta,
    )
