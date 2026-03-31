"""Ensemble diagnostics (effective sample size, split acceptance, R-hat-style summaries).

Mirrors ALARM / redist emphasis on verifying exploration of the constrained
plan space. Implements Python-side summaries on :class:`~hungary_ge.ensemble.PlanEnsemble`;
optional best-effort scraping of redist log paths stored in ``metadata``.
"""

from __future__ import annotations

import math
from collections.abc import Sequence

from hungary_ge.diagnostics.chains import (
    gelman_rubin_rhat_univariate,
    per_chain_scalar_sequences,
)
from hungary_ge.diagnostics.population import (
    build_population_summary_block,
    per_draw_max_abs_rel_pop_deviation,
)
from hungary_ge.diagnostics.report import (
    DIAGNOSTICS_SCHEMA_V1,
    ChainRHatBlock,
    CountySplitsBlock,
    DiagnosticsReport,
    EnsembleMixingBlock,
    PopulationSummaryBlock,
    SmcLogBlock,
    write_diagnostics_json,
)
from hungary_ge.diagnostics.smc import scrape_redist_logs_from_metadata
from hungary_ge.diagnostics.splits import (
    build_county_splits_block,
    per_draw_n_split_counties,
)
from hungary_ge.ensemble.plan_ensemble import PlanEnsemble


def _ensemble_uniqueness_block(ensemble: PlanEnsemble) -> EnsembleMixingBlock:
    seen: dict[tuple[int, ...], int] = {}
    dup = 0
    for j in range(ensemble.n_draws):
        sig = tuple(ensemble.assignments[i][j] for i in range(ensemble.n_units))
        if sig in seen:
            dup += 1
        else:
            seen[sig] = j
    return EnsembleMixingBlock(
        n_unique_assignment_columns=len(seen),
        n_duplicate_assignment_columns=dup,
    )


def summarize_ensemble(
    ensemble: PlanEnsemble,
    *,
    populations: Sequence[float],
    ndists: int,
    pop_tol: float | None = None,
    county_ids: Sequence[str] | None = None,
    include_smc_log_scan: bool = True,
) -> DiagnosticsReport:
    """Compute diagnostics for simulated district assignment columns.

    Args:
        ensemble: Assignment matrix aligned with ``populations`` and optional ``county_ids``.
        populations: Length ``n_units``; use **0** on void / gap nodes so totals match
            the sampling graph (see master-plan Slice 8).
        ndists: Number of districts; used for ideal population per district.
        pop_tol: If set, ``population.draws_exceeding_pop_tol`` flags draws whose
            max absolute relative deviation exceeds this value.
        county_ids: Optional length ``n_units`` county labels for split diagnostics.
        include_smc_log_scan: If True and ``ensemble.metadata`` contains redist log paths,
            run :func:`scrape_redist_logs_from_metadata`.
    """
    if len(populations) != ensemble.n_units:
        msg = f"populations length {len(populations)} != n_units {ensemble.n_units}"
        raise ValueError(msg)

    pop_block = build_population_summary_block(ensemble, populations, ndists, pop_tol)
    county_block: CountySplitsBlock | None = None
    if county_ids is not None:
        county_block = build_county_splits_block(ensemble, county_ids)

    mixing = _ensemble_uniqueness_block(ensemble)

    chain_block: ChainRHatBlock | None = None
    ch = ensemble.chain_or_run
    if ch is not None and len(set(ch)) >= 2:
        per_pop = per_draw_max_abs_rel_pop_deviation(ensemble, populations, ndists)
        seqs_pop = per_chain_scalar_sequences(per_pop, ch)
        r_pop = gelman_rubin_rhat_univariate(seqs_pop)
        r_split: float | None = None
        if county_block is not None:
            assert county_ids is not None
            per_s = per_draw_n_split_counties(ensemble, county_ids).astype(float)
            seqs_s = per_chain_scalar_sequences(per_s, ch)
            r_split = gelman_rubin_rhat_univariate(seqs_s)
        chain_block = ChainRHatBlock(
            n_chains=len(set(ch)),
            r_hat_max_abs_rel_pop_deviation=float(r_pop)
            if math.isfinite(r_pop)
            else None,
            r_hat_n_split_counties=float(r_split)
            if r_split is not None and math.isfinite(r_split)
            else None,
        )

    smc_block: SmcLogBlock | None = None
    if include_smc_log_scan:
        smc_block = scrape_redist_logs_from_metadata(ensemble.metadata)

    return DiagnosticsReport(
        schema_version=DIAGNOSTICS_SCHEMA_V1,
        n_units=ensemble.n_units,
        n_draws=ensemble.n_draws,
        ndists=ndists,
        population=pop_block,
        county_splits=county_block,
        chains=chain_block,
        ensemble=mixing,
        smc_log=smc_block,
    )


__all__ = [
    "DIAGNOSTICS_SCHEMA_V1",
    "ChainRHatBlock",
    "CountySplitsBlock",
    "DiagnosticsReport",
    "EnsembleMixingBlock",
    "PopulationSummaryBlock",
    "SmcLogBlock",
    "gelman_rubin_rhat_univariate",
    "per_chain_scalar_sequences",
    "summarize_ensemble",
    "write_diagnostics_json",
    "scrape_redist_logs_from_metadata",
]
