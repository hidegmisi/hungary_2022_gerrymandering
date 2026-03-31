"""Typed diagnostics report and JSON writer (Slice 8)."""

from __future__ import annotations

import json
import math
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any


def _json_sanitize(x: Any) -> Any:
    """Replace non-finite floats with ``None`` for strict JSON consumers."""
    if isinstance(x, dict):
        return {k: _json_sanitize(v) for k, v in x.items()}
    if isinstance(x, list):
        return [_json_sanitize(v) for v in x]
    if isinstance(x, float) and not math.isfinite(x):
        return None
    return x


DIAGNOSTICS_SCHEMA_V1 = "hungary_ge.diagnostics/v1"


@dataclass(frozen=True)
class PopulationSummaryBlock:
    """Aggregates for max absolute relative population deviation per draw."""

    ideal_per_district: float
    total_population: float
    per_draw_max_abs_rel_deviation: tuple[float, ...]
    draws_exceeding_pop_tol: tuple[bool, ...] | None = None
    mean_of_max_abs_rel_deviation: float = 0.0
    max_of_max_abs_rel_deviation: float = 0.0
    p95_of_max_abs_rel_deviation: float = 0.0


@dataclass(frozen=True)
class CountySplitsBlock:
    """Per-draw number of counties assigned to more than one district."""

    n_counties_in_frame: int
    per_draw_n_split_counties: tuple[int, ...]
    mean_n_split_counties: float = 0.0
    max_n_split_counties: int = 0


@dataclass(frozen=True)
class ChainRHatBlock:
    """Univariate Gelman–Rubin R-hat on per-draw scalar summaries (optional)."""

    n_chains: int
    r_hat_max_abs_rel_pop_deviation: float | None = None
    r_hat_n_split_counties: float | None = None
    note: str = (
        "Univariate R-hat on per-draw chain segments; not multivariate convergence."
    )


@dataclass(frozen=True)
class EnsembleMixingBlock:
    """Light structure-free mixing / diversity summaries."""

    n_unique_assignment_columns: int
    n_duplicate_assignment_columns: int


@dataclass(frozen=True)
class SmcLogBlock:
    """Best-effort parse of redist SMC logs when paths are in ensemble metadata."""

    parse_status: str
    redist_stderr_path: str | None = None
    redist_stdout_path: str | None = None
    ess_line_hits: int = 0
    log_excerpt_chars: int = 0
    excerpt_suffix: str | None = None


@dataclass(frozen=True)
class DiagnosticsReport:
    """JSON-serializable ensemble diagnostics."""

    schema_version: str = DIAGNOSTICS_SCHEMA_V1
    n_units: int = 0
    n_draws: int = 0
    ndists: int = 0
    population: PopulationSummaryBlock | None = None
    county_splits: CountySplitsBlock | None = None
    chains: ChainRHatBlock | None = None
    ensemble: EnsembleMixingBlock | None = None
    smc_log: SmcLogBlock | None = None
    extra: dict[str, Any] = field(default_factory=dict)

    def to_json_dict(self) -> dict[str, Any]:
        """Nested dict suitable for ``json.dumps``."""
        return asdict(self)


def write_diagnostics_json(path: str | Path, report: DiagnosticsReport) -> None:
    """Write :class:`DiagnosticsReport` as UTF-8 JSON (pretty-printed)."""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    payload = _json_sanitize(report.to_json_dict())
    p.write_text(
        json.dumps(payload, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
