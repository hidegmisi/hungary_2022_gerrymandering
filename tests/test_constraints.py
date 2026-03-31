"""Tests for constraint spec JSON and :func:`~hungary_ge.constraints.validate.check_plan`."""

from __future__ import annotations

import pytest

from hungary_ge.constraints.constraint_spec import (
    ConstraintSpec,
    ContiguityConstraint,
    CountyContainmentConstraint,
    ElectorBalanceConstraint,
    SoftConstraintWeight,
    default_constraint_spec,
    spec_from_json,
    spec_to_json,
)
from hungary_ge.constraints.validate import check_plan
from hungary_ge.graph.adjacency_graph import from_neighbor_lists
from hungary_ge.problem.precinct_index_map import PrecinctIndexMap


def _tiny_graph_path4_disconnected_pairs() -> tuple[PrecinctIndexMap, object]:
    """0—1 and 2—3 edges only (two components if merged one district)."""
    order = PrecinctIndexMap(
        ids=("p0", "p1", "p2", "p3"),
        id_column="precinct_id",
    )
    nbr = (
        (1,),
        (0,),
        (3,),
        (2,),
    )
    g = from_neighbor_lists(order, "test", nbr)
    return order, g


def _tiny_graph_chain4() -> tuple[PrecinctIndexMap, object]:
    order = PrecinctIndexMap(
        ids=("p0", "p1", "p2", "p3"),
        id_column="precinct_id",
    )
    nbr = (
        (1,),
        (0, 2),
        (1, 3),
        (2,),
    )
    g = from_neighbor_lists(order, "test", nbr)
    return order, g


def test_spec_json_roundtrip() -> None:
    spec = ConstraintSpec(
        version="test-1",
        elector_balance=ElectorBalanceConstraint(ndists=2, max_relative_deviation=0.15),
        contiguity=ContiguityConstraint(enabled=True),
        county_containment=CountyContainmentConstraint(enabled=False),
        soft_weights=(SoftConstraintWeight(name="compactness", weight=0.1),),
    )
    again = spec_from_json(spec_to_json(spec))
    assert again == spec


def test_default_spec_roundtrip() -> None:
    spec = default_constraint_spec(spec_version="0.2.0")
    again = spec_from_json(spec_to_json(spec))
    assert again == spec


def test_check_plan_elector_violation() -> None:
    _, g = _tiny_graph_chain4()
    spec = ConstraintSpec(
        version="t",
        elector_balance=ElectorBalanceConstraint(ndists=2, max_relative_deviation=0.15),
        contiguity=ContiguityConstraint(enabled=False),
        county_containment=CountyContainmentConstraint(enabled=False),
    )
    pops = [300.0, 0.0, 0.0, 0.0]
    assign = [1, 2, 2, 2]
    rep = check_plan(assign, pops, spec, g)
    assert not rep.is_valid
    assert any(v.code == "elector_deviation" for v in rep.violations)


def test_check_plan_contiguity_violation() -> None:
    _, g = _tiny_graph_path4_disconnected_pairs()
    spec = ConstraintSpec(
        version="t",
        elector_balance=ElectorBalanceConstraint(ndists=1, max_relative_deviation=0.15),
        contiguity=ContiguityConstraint(enabled=True),
        county_containment=CountyContainmentConstraint(enabled=False),
    )
    pops = [50.0, 50.0, 50.0, 50.0]
    assign = [1, 1, 1, 1]
    rep = check_plan(assign, pops, spec, g)
    assert not rep.is_valid
    assert any(v.code == "district_disconnected" for v in rep.violations)


def test_check_plan_county_span_violation() -> None:
    _, g = _tiny_graph_chain4()
    spec = ConstraintSpec(
        version="t",
        elector_balance=ElectorBalanceConstraint(ndists=2, max_relative_deviation=0.15),
        contiguity=ContiguityConstraint(enabled=True),
        county_containment=CountyContainmentConstraint(enabled=True),
    )
    pops = [50.0, 50.0, 50.0, 50.0]
    assign = [1, 1, 2, 2]
    county_ids = ["01", "02", "01", "01"]
    rep = check_plan(assign, pops, spec, g, county_ids=county_ids)
    assert not rep.is_valid
    assert any(v.code == "county_span_violation" for v in rep.violations)


def test_check_plan_happy_path() -> None:
    _, g = _tiny_graph_chain4()
    spec = ConstraintSpec(
        version="t",
        elector_balance=ElectorBalanceConstraint(ndists=2, max_relative_deviation=0.15),
        contiguity=ContiguityConstraint(enabled=True),
        county_containment=CountyContainmentConstraint(enabled=True),
    )
    pops = [50.0, 50.0, 50.0, 50.0]
    assign = [1, 1, 2, 2]
    county_ids = ["01", "01", "01", "01"]
    rep = check_plan(assign, pops, spec, g, county_ids=county_ids)
    assert rep.is_valid


def test_missing_county_ids_when_enabled() -> None:
    _, g = _tiny_graph_chain4()
    spec = ConstraintSpec(
        version="t",
        elector_balance=ElectorBalanceConstraint(ndists=2, max_relative_deviation=0.15),
        contiguity=ContiguityConstraint(enabled=False),
        county_containment=CountyContainmentConstraint(enabled=True),
    )
    rep = check_plan([1, 1, 2, 2], [25.0, 25.0, 25.0, 25.0], spec, g, county_ids=None)
    assert not rep.is_valid
    assert any(v.code == "missing_county_ids" for v in rep.violations)


def test_unknown_schema_version() -> None:
    bad = '{"schema_version": "other", "version": "x", "elector_balance": {"ndists": 2, "max_relative_deviation": 0.15}, "contiguity": {"enabled": true}, "county_containment": {"enabled": false}, "soft_weights": []}'
    with pytest.raises(ValueError, match="unsupported schema_version"):
        spec_from_json(bad)
