"""Hungarian OEVK and sampler constraints (hard vs soft, Slice 5).

See ``docs/oevk-constraints.md`` and ``docs/alarm-methodology.md``.
"""

from __future__ import annotations

from hungary_ge.constraints.constraint_spec import (
    SCHEMA_VERSION,
    ConstraintSpec,
    ContiguityConstraint,
    CountyContainmentConstraint,
    ElectorBalanceConstraint,
    SoftConstraintWeight,
    default_constraint_spec,
    spec_from_json,
    spec_to_json,
)
from hungary_ge.constraints.validate import (
    ConstraintViolation,
    ConstraintViolationReport,
    check_plan,
)

__all__ = [
    "SCHEMA_VERSION",
    "ConstraintSpec",
    "ConstraintViolation",
    "ConstraintViolationReport",
    "ContiguityConstraint",
    "CountyContainmentConstraint",
    "ElectorBalanceConstraint",
    "SoftConstraintWeight",
    "check_plan",
    "default_constraint_spec",
    "spec_from_json",
    "spec_to_json",
]
