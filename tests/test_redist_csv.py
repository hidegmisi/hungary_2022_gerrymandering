"""Load PlanEnsemble from redist assignments CSV (no R)."""

from __future__ import annotations

from pathlib import Path

from hungary_ge.ensemble import PlanEnsemble
from hungary_ge.sampling.redist_adapter import load_ensemble_from_redist_csv

FIXTURE = (
    Path(__file__).resolve().parent / "fixtures" / "redist_mini" / "assignments.csv"
)


def test_load_ensemble_from_redist_csv_fixture() -> None:
    unit_ids = ("p0", "p1", "p2")
    ens = load_ensemble_from_redist_csv(FIXTURE, unit_ids)
    assert isinstance(ens, PlanEnsemble)
    assert ens.n_units == 3
    assert ens.n_draws == 3
    assert ens.assignments[0] == (1, 2, 1)
    assert ens.draw_ids == (1, 2, 3)
    assert ens.chain_or_run == (1, 1, 2)
