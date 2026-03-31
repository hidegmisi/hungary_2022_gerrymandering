"""Validate a single districting plan against :class:`ConstraintSpec` (Slice 5)."""

from __future__ import annotations

from collections import deque
from collections.abc import Sequence
from dataclasses import dataclass

import numpy as np

from hungary_ge.constraints.constraint_spec import ConstraintSpec
from hungary_ge.graph.adjacency_graph import AdjacencyGraph


@dataclass(frozen=True)
class ConstraintViolation:
    """One failed rule."""

    code: str
    message: str
    district: int | None = None


@dataclass(frozen=True)
class ConstraintViolationReport:
    """Result of :func:`check_plan`."""

    violations: tuple[ConstraintViolation, ...]

    @property
    def is_valid(self) -> bool:
        return len(self.violations) == 0


def check_plan(
    assignments: Sequence[int],
    populations: Sequence[float] | np.ndarray,
    spec: ConstraintSpec,
    graph: AdjacencyGraph,
    *,
    county_ids: Sequence[str] | None = None,
) -> ConstraintViolationReport:
    violations: list[ConstraintViolation] = []
    n = graph.n_nodes
    nd = spec.elector_balance.ndists
    assign = np.asarray(assignments, dtype=np.int64)
    pops = np.asarray(populations, dtype=np.float64)

    if assign.shape != (n,) or pops.shape != (n,):
        violations.append(
            ConstraintViolation(
                code="shape_mismatch",
                message=f"expected length {n}, got assignments {assign.shape} populations {pops.shape}",
            )
        )
        return ConstraintViolationReport(tuple(violations))

    if county_ids is not None and len(county_ids) != n:
        violations.append(
            ConstraintViolation(
                code="county_ids_length",
                message=f"county_ids length {len(county_ids)} != n_units {n}",
            )
        )
        return ConstraintViolationReport(tuple(violations))

    if spec.county_containment.enabled and county_ids is None:
        violations.append(
            ConstraintViolation(
                code="missing_county_ids",
                message="county_containment.enabled but county_ids is None",
            )
        )
        return ConstraintViolationReport(tuple(violations))

    total_electors = float(pops.sum())
    if total_electors <= 0:
        violations.append(
            ConstraintViolation(
                code="zero_total_electors",
                message="sum(populations) must be positive for elector balance checks",
            )
        )
        return ConstraintViolationReport(tuple(violations))

    ideal = total_electors / nd

    for i in range(n):
        lab = int(assign[i])
        if lab < 1 or lab > nd:
            violations.append(
                ConstraintViolation(
                    code="invalid_district_label",
                    message=f"node {i}: assignment {lab} not in [1, {nd}]",
                    district=None,
                )
            )
    if violations:
        return ConstraintViolationReport(tuple(violations))

    used = set(int(x) for x in assign.tolist())
    expected = set(range(1, nd + 1))
    if used != expected:
        missing = sorted(expected - used)
        extra = sorted(used - expected)
        violations.append(
            ConstraintViolation(
                code="district_label_coverage",
                message=f"labels used must be exactly 1..{nd}; missing={missing!s} extra={extra!s}",
            )
        )
        return ConstraintViolationReport(tuple(violations))

    tol = spec.elector_balance.max_relative_deviation
    for d in range(1, nd + 1):
        mask = assign == d
        district_sum = float(pops[mask].sum())
        rel = abs(district_sum - ideal) / ideal if ideal > 0 else 0.0
        if rel > tol + 1e-12:
            violations.append(
                ConstraintViolation(
                    code="elector_deviation",
                    message=(
                        f"district {d}: electors={district_sum:.6g}, ideal={ideal:.6g}, "
                        f"relative_deviation={rel:.6g} > max {tol}"
                    ),
                    district=d,
                )
            )

    if spec.contiguity.enabled:
        nbr = graph.neighbor_lists
        for d in range(1, nd + 1):
            nodes = [i for i in range(n) if int(assign[i]) == d]
            node_set = set(nodes)
            start = nodes[0]
            seen: set[int] = {start}
            dq: deque[int] = deque([start])
            while dq:
                u = dq.popleft()
                for v in nbr[u]:
                    if v in node_set and v not in seen:
                        seen.add(v)
                        dq.append(v)
            if seen != node_set:
                violations.append(
                    ConstraintViolation(
                        code="district_disconnected",
                        message=(
                            f"district {d}: induced subgraph has {len(node_set) - len(seen)} "
                            f"unreachable nodes from component size {len(seen)}"
                        ),
                        district=d,
                    )
                )

    if spec.county_containment.enabled and county_ids is not None:
        for d in range(1, nd + 1):
            counties = {county_ids[i] for i in range(n) if int(assign[i]) == d}
            if len(counties) > 1:
                violations.append(
                    ConstraintViolation(
                        code="county_span_violation",
                        message=f"district {d} spans counties {sorted(counties)!r}",
                        district=d,
                    )
                )

    return ConstraintViolationReport(tuple(violations))
