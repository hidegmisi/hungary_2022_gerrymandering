"""Simulated plan ensemble (redist_plans analogue)."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field


@dataclass
class PlanEnsemble:
    """Ensemble of district assignments over geographic units.

    Conceptual counterpart of R **redist**'s ``redist_plans``: each column is
    one simulated draw; each row is one unit (precinct), in fixed order.

    ``assignments[row][col]`` is the district label (integer 1 … ``ndists`` or
    official OEVK codes) for unit ``unit_ids[row]`` in draw ``col``.

    Attributes:
        unit_ids: Precinct identifiers, length ``n_units``, matching row order.
        assignments: Nested sequence of shape ``(n_units, n_draws)``.
        draw_ids: Optional draw labels (length ``n_draws``).
        chain_or_run: Optional parallel SMC chain / run id per draw.
        metadata: Optional key-value metadata (sampler version, seed, etc.).
    """

    unit_ids: tuple[str, ...]
    assignments: tuple[tuple[int, ...], ...]
    draw_ids: tuple[int, ...] | None = None
    chain_or_run: tuple[int, ...] | None = None
    metadata: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        n_units = len(self.unit_ids)
        if len(self.assignments) != n_units:
            msg = (
                f"assignments must have {n_units} rows (one per unit_id), "
                f"got {len(self.assignments)}"
            )
            raise ValueError(msg)
        if n_units == 0:
            return
        n_draws = len(self.assignments[0])
        for i, row in enumerate(self.assignments):
            if len(row) != n_draws:
                msg = f"row {i} has length {len(row)}, expected {n_draws}"
                raise ValueError(msg)
        if self.draw_ids is not None and len(self.draw_ids) != n_draws:
            msg = f"draw_ids length {len(self.draw_ids)} != n_draws {n_draws}"
            raise ValueError(msg)
        if self.chain_or_run is not None and len(self.chain_or_run) != n_draws:
            msg = f"chain_or_run length {len(self.chain_or_run)} != n_draws {n_draws}"
            raise ValueError(msg)

    @property
    def n_units(self) -> int:
        return len(self.unit_ids)

    @property
    def n_draws(self) -> int:
        if not self.assignments:
            return 0
        return len(self.assignments[0])

    @classmethod
    def from_columns(
        cls,
        unit_ids: Sequence[str],
        plan_columns: Sequence[Sequence[int]],
        draw_ids: Sequence[int] | None = None,
        chain_or_run: Sequence[int] | None = None,
        metadata: Mapping[str, object] | None = None,
    ) -> PlanEnsemble:
        """Build from ``n_draws`` columns, each length ``n_units``."""
        uid = tuple(unit_ids)
        n_u = len(uid)
        if not plan_columns:
            rows: tuple[tuple[int, ...], ...] = tuple(() for _ in range(n_u))
            return cls(
                uid,
                rows,
                draw_ids=tuple(draw_ids) if draw_ids is not None else None,
                chain_or_run=tuple(chain_or_run) if chain_or_run is not None else None,
                metadata=dict(metadata or {}),
            )
        n_d = len(plan_columns)
        for j, col in enumerate(plan_columns):
            if len(col) != n_u:
                msg = f"plan column {j} length {len(col)} != n_units {n_u}"
                raise ValueError(msg)
        rows = tuple(tuple(plan_columns[j][i] for j in range(n_d)) for i in range(n_u))
        d_ids = tuple(draw_ids) if draw_ids is not None else None
        ch = tuple(chain_or_run) if chain_or_run is not None else None
        return cls(
            uid, rows, draw_ids=d_ids, chain_or_run=ch, metadata=dict(metadata or {})
        )
