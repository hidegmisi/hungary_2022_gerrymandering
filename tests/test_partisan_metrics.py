"""Slice 9 partisan metrics (two-bloc reduction, focal vs ensemble)."""

from __future__ import annotations

import pandas as pd
import pytest

from hungary_ge.ensemble.plan_ensemble import PlanEnsemble
from hungary_ge.metrics import (
    focal_vs_ensemble_metrics,
    partisan_metrics,
)
from hungary_ge.metrics.party_coding import (
    PartisanPartyCoding,
    default_partisan_party_coding_path,
    load_partisan_party_coding,
)
from hungary_ge.metrics.report import percentile_rank_inclusive_upper
from hungary_ge.metrics.two_party import (
    district_two_party_totals,
    efficiency_gap_two_party,
    seat_share_a_smd,
)


def test_district_totals_and_eg_toy() -> None:
    # Two districts, perfect balance: A wins both with minimum margin -> high symmetry
    dist = [1, 1, 2, 2]
    va = [60.0, 40.0, 55.0, 45.0]
    vb = [40.0, 60.0, 45.0, 55.0]
    tot = district_two_party_totals(dist, va, vb)
    assert tot[1] == (100.0, 100.0)
    assert tot[2] == (100.0, 100.0)
    seats, n_e = seat_share_a_smd(tot)
    assert n_e == 2
    assert seats == 1.0  # ties in both -> 0.5 + 0.5
    eg, wa, wb, tnat = efficiency_gap_two_party(tot)
    assert tnat == 400.0
    assert wa == pytest.approx(wb)
    assert eg == pytest.approx(0.0, abs=1e-9)


def test_efficiency_gap_sign_and_edge_cases() -> None:
    # A packed blowout: A wastes more than B => positive EG in this implementation.
    eg_a, wa_a, wb_a, t_a = efficiency_gap_two_party({1: (80.0, 20.0)})
    assert t_a == 100.0
    assert wa_a == pytest.approx(30.0)
    assert wb_a == pytest.approx(20.0)
    assert eg_a == pytest.approx(0.1)

    # Swapping A/B vote totals should flip the sign.
    eg_b, wa_b, wb_b, t_b = efficiency_gap_two_party({1: (20.0, 80.0)})
    assert t_b == 100.0
    assert wa_b == pytest.approx(20.0)
    assert wb_b == pytest.approx(30.0)
    assert eg_b == pytest.approx(-0.1)
    assert eg_b == pytest.approx(-eg_a)

    # Ties and zero-turnout districts are handled explicitly.
    eg_tie, wa_tie, wb_tie, t_tie = efficiency_gap_two_party({1: (50.0, 50.0)})
    assert t_tie == 100.0
    assert wa_tie == pytest.approx(0.0)
    assert wb_tie == pytest.approx(0.0)
    assert eg_tie == pytest.approx(0.0)

    eg_zero, wa_zero, wb_zero, t_zero = efficiency_gap_two_party(
        {1: (0.0, 0.0), 2: (80.0, 20.0)}
    )
    assert t_zero == 100.0
    assert wa_zero == pytest.approx(30.0)
    assert wb_zero == pytest.approx(20.0)
    assert eg_zero == pytest.approx(0.1)

    # One-sided district vote should still stay in [-1, 1] and be finite.
    eg_one_sided, _, _, _ = efficiency_gap_two_party({1: (100.0, 0.0)})
    assert eg_one_sided == pytest.approx(0.5)


def test_focal_vs_ensemble_toy() -> None:
    coding = PartisanPartyCoding(
        party_a_columns=("va",),
        party_b_columns=("vb",),
        label_a="A",
        label_b="B",
    )
    # Three precincts: two A-heavy, one B-only. Focal splits A across two districts (one
    # A win, one B win); ensemble piles all into one district (single A win, higher seat rate).
    votes = pd.DataFrame(
        {
            "precinct_id": ["p1", "p2", "p3"],
            "va": [100.0, 100.0, 0.0],
            "vb": [0.0, 0.0, 100.0],
        }
    )
    focal = pd.DataFrame(
        {
            "precinct_id": ["p1", "p2", "p3"],
            "oevk_id_full": ["X", "X", "Y"],
        }
    )
    ens = PlanEnsemble.from_columns(
        ("p1", "p2", "p3"),
        [[1, 1, 1]],
    )
    rep = focal_vs_ensemble_metrics(focal, ens, votes, party_coding=coding)
    assert rep.metrics["seat_share_a"].focal_value == pytest.approx(0.5)
    assert rep.metrics["seat_share_a"].ensemble_mean == pytest.approx(1.0)
    assert rep.metrics["efficiency_gap"].focal_value == pytest.approx(1.0 / 6.0)
    assert rep.metrics["efficiency_gap"].ensemble_mean == pytest.approx(-1.0 / 6.0)
    assert rep.metrics["efficiency_gap"].ensemble_p05 == pytest.approx(-1.0 / 6.0)
    assert rep.metrics["efficiency_gap"].ensemble_p95 == pytest.approx(-1.0 / 6.0)
    assert rep.metrics["efficiency_gap"].percentile_rank == pytest.approx(100.0)
    assert rep.coverage is not None
    assert rep.coverage.n_units == 3


def test_loose_focal_can_change_eg_comparability() -> None:
    coding = PartisanPartyCoding(
        party_a_columns=("va",),
        party_b_columns=("vb",),
    )
    votes = pd.DataFrame(
        {
            "precinct_id": ["p1", "p2"],
            "va": [60.0, 0.0],
            "vb": [40.0, 100.0],
        }
    )
    # p2 has voting data but no focal district label.
    focal = pd.DataFrame({"precinct_id": ["p1"], "oevk_id_full": ["X"]})
    ens = PlanEnsemble.from_columns(("p1", "p2"), [[1, 1]])

    rep = focal_vs_ensemble_metrics(
        focal,
        ens,
        votes,
        party_coding=coding,
        strict_focal_for_voting_units=False,
    )

    assert rep.coverage is not None
    assert rep.coverage.n_voting_units_missing_focal == 1
    assert rep.extra["n_units_in_focal_aggregate"] == 1
    # Draw metric uses both units, focal metric omits p2.
    assert rep.metrics["efficiency_gap"].ensemble_mean == pytest.approx(0.1)
    assert rep.metrics["efficiency_gap"].focal_value == pytest.approx(-0.3)


def test_void_unit_zero_votes_excluded_from_focal_aggregate() -> None:
    coding = PartisanPartyCoding(
        party_a_columns=("va",),
        party_b_columns=("vb",),
    )
    votes = pd.DataFrame(
        {
            "precinct_id": ["p1", "p2", "gap-01-0001"],
            "va": [50.0, 50.0, 0.0],
            "vb": [50.0, 50.0, 0.0],
        }
    )
    focal = pd.DataFrame(
        {
            "precinct_id": ["p1", "p2"],
            "oevk_id_full": ["X", "Y"],
        }
    )
    ens = PlanEnsemble.from_columns(
        ("p1", "p2", "gap-01-0001"),
        [[1, 2, 1]],
    )
    rep = focal_vs_ensemble_metrics(
        focal,
        ens,
        votes,
        party_coding=coding,
        strict_focal_for_voting_units=True,
    )
    # Focal aggregate uses 2 szvk units only; both tied -> 0.5 + 0.5 seat share A
    assert rep.metrics["seat_share_a"].focal_value == pytest.approx(0.5)
    # Ensemble: gap in dist 1 with 0 votes; p1+p2 in dists 1 and 2 both tie
    assert rep.metrics["seat_share_a"].ensemble_mean == pytest.approx(0.5)


def test_percentile_ranks() -> None:
    draws = [0.0, 0.25, 0.5, 0.75, 1.0]
    assert percentile_rank_inclusive_upper(draws, 0.0) == pytest.approx(20.0)
    assert percentile_rank_inclusive_upper(draws, 0.5) == pytest.approx(60.0)
    assert percentile_rank_inclusive_upper(draws, 1.0) == pytest.approx(100.0)


def test_strict_focal_raises_when_votes_without_focal_row() -> None:
    coding = PartisanPartyCoding(party_a_columns=("va",), party_b_columns=("vb",))
    votes = pd.DataFrame(
        {"precinct_id": ["p1", "p2"], "va": [10.0, 20.0], "vb": [5.0, 5.0]}
    )
    focal = pd.DataFrame({"precinct_id": ["p1"], "oevk_id_full": ["X"]})
    ens = PlanEnsemble.from_columns(("p1", "p2"), [[1, 1]])
    with pytest.raises(ValueError, match="positive two-party votes"):
        focal_vs_ensemble_metrics(
            focal, ens, votes, party_coding=coding, strict_focal_for_voting_units=True
        )


def test_load_packaged_party_coding_json() -> None:
    c = load_partisan_party_coding(default_partisan_party_coding_path())
    assert "votes_list_952" in c.party_a_columns


def test_partisan_metrics_wrapper_order() -> None:
    coding = PartisanPartyCoding(party_a_columns=("va",), party_b_columns=("vb",))
    votes = pd.DataFrame(
        {"precinct_id": ["a", "b"], "va": [1.0, 1.0], "vb": [1.0, 1.0]}
    )
    focal = pd.DataFrame({"precinct_id": ["a", "b"], "oevk_id_full": ["X", "Y"]})
    ens = PlanEnsemble.from_columns(("a", "b"), [[1, 2]])
    r1 = focal_vs_ensemble_metrics(focal, ens, votes, party_coding=coding)
    r2 = partisan_metrics(ens, votes, focal=focal, party_coding=coding)
    assert r1.metrics.keys() == r2.metrics.keys()
