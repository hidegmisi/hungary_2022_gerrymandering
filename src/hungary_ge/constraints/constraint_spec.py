"""Frozen constraint specification and JSON serialization (Slice 5).

See ``docs/oevk-constraints.md`` for statutory mapping and v1 encoding.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import Any

from hungary_ge.problem.oevk_problem import DEFAULT_NDISTS

SCHEMA_VERSION = "hungary_ge.constraints/v1"


@dataclass(frozen=True)
class ElectorBalanceConstraint:
    """Eligible-elector balance: each district vs ideal ``total_electors / ndists``.

    ``max_relative_deviation`` is a **hard** bound for simulated plans
    (default ±15%). Void graph nodes should have elector weight 0.
    """

    ndists: int = DEFAULT_NDISTS
    max_relative_deviation: float = 0.15


@dataclass(frozen=True)
class ContiguityConstraint:
    """Each district must induce a connected subgraph of ``AdjacencyGraph``."""

    enabled: bool = True


@dataclass(frozen=True)
class CountyContainmentConstraint:
    """When enabled, each district may include units from only one county bucket."""

    enabled: bool = False


@dataclass(frozen=True)
class SoftConstraintWeight:
    """Optional soft target (e.g. compactness strength) for future samplers."""

    name: str
    weight: float


@dataclass(frozen=True)
class ConstraintSpec:
    """Versioned bundle of hard/soft constraints for ``check_plan`` and Slice 6."""

    version: str
    elector_balance: ElectorBalanceConstraint
    contiguity: ContiguityConstraint
    county_containment: CountyContainmentConstraint
    soft_weights: tuple[SoftConstraintWeight, ...] = ()


def default_constraint_spec(*, spec_version: str = "0.1.0") -> ConstraintSpec:
    """Reasonable defaults: 106 districts, ±15% elector deviation, contiguity on."""
    return ConstraintSpec(
        version=spec_version,
        elector_balance=ElectorBalanceConstraint(),
        contiguity=ContiguityConstraint(),
        county_containment=CountyContainmentConstraint(),
        soft_weights=(),
    )


def _spec_to_dict(spec: ConstraintSpec) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "version": spec.version,
        "elector_balance": asdict(spec.elector_balance),
        "contiguity": asdict(spec.contiguity),
        "county_containment": asdict(spec.county_containment),
        "soft_weights": [asdict(sw) for sw in spec.soft_weights],
    }


def spec_to_json(spec: ConstraintSpec, *, indent: int | None = 2) -> str:
    """Serialize ``ConstraintSpec`` to JSON (stdlib only)."""
    return json.dumps(_spec_to_dict(spec), indent=indent)


def spec_from_json(s: str) -> ConstraintSpec:
    """Deserialize JSON produced by :func:`spec_to_json`."""
    d = json.loads(s)
    sv = d.get("schema_version")
    if sv is not None and sv != SCHEMA_VERSION:
        msg = f"unsupported schema_version {sv!r}, expected {SCHEMA_VERSION!r}"
        raise ValueError(msg)
    eb = d["elector_balance"]
    ct = d["contiguity"]
    cc = d["county_containment"]
    sw_raw = d.get("soft_weights") or []
    soft_weights = tuple(
        SoftConstraintWeight(name=x["name"], weight=float(x["weight"])) for x in sw_raw
    )
    return ConstraintSpec(
        version=str(d["version"]),
        elector_balance=ElectorBalanceConstraint(
            ndists=int(eb["ndists"]),
            max_relative_deviation=float(eb["max_relative_deviation"]),
        ),
        contiguity=ContiguityConstraint(enabled=bool(ct["enabled"])),
        county_containment=CountyContainmentConstraint(enabled=bool(cc["enabled"])),
        soft_weights=soft_weights,
    )
