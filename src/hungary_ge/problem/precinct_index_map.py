"""Stable precinct row order and problem-frame validation (Slice 2)."""

from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd
from geopandas import GeoDataFrame
from pyproj import CRS as ProjCRS

from hungary_ge.problem.oevk_problem import (
    DEFAULT_PRECINCT_ID_COLUMN,
    OevkProblem,
)


class ProblemFrameValidationError(ValueError):
    """Raised when a GeoDataFrame does not satisfy :class:`OevkProblem` constraints."""

    def __init__(self, message: str, *, details: list[str] | None = None) -> None:
        super().__init__(message)
        self.details = details or []


@dataclass(frozen=True)
class PrecinctIndexMap:
    """Maps row index ``i`` to ``precinct_id`` in **lexicographic** sort order.

    Row ``i`` of a frame prepared with :meth:`from_frame` matches :meth:`id_at`
    for that index. Use :func:`prepare_precinct_layer` for a single supported
    pipeline from raw :class:`~geopandas.GeoDataFrame` to sorted frame + map.
    """

    ids: tuple[str, ...]
    id_column: str
    _index_by_id: dict[str, int] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "_index_by_id",
            {pid: i for i, pid in enumerate(self.ids)},
        )

    @property
    def n_units(self) -> int:
        return len(self.ids)

    def id_at(self, i: int) -> str:
        return self.ids[i]

    def index_of(self, precinct_id: str) -> int:
        try:
            return self._index_by_id[precinct_id]
        except KeyError as e:
            msg = f"unknown precinct_id: {precinct_id!r}"
            raise KeyError(msg) from e

    @classmethod
    def from_frame(
        cls,
        gdf: GeoDataFrame,
        *,
        id_column: str | None = None,
    ) -> tuple[PrecinctIndexMap, GeoDataFrame]:
        """Sort rows by ``id_column`` (lexicographic), reset index, build the map.

        Does not mutate the input frame. Raises :exc:`ProblemFrameValidationError`
        on missing column, null ids, or duplicate ids.
        """
        col = id_column if id_column is not None else DEFAULT_PRECINCT_ID_COLUMN
        if col not in gdf.columns:
            msg = f"missing precinct id column {col!r}"
            raise ProblemFrameValidationError(msg)

        work = gdf.copy()
        series = work[col]
        if series.isna().any():
            msg = f"null values in {col!r}"
            raise ProblemFrameValidationError(msg)

        dup = series.duplicated()
        if dup.any():
            msg = f"duplicate values in {col!r}"
            raise ProblemFrameValidationError(msg)

        sorted_gdf = work.sort_values(col, kind="mergesort").reset_index(drop=True)
        ids = tuple(sorted_gdf[col].astype(str).tolist())
        return cls(ids=ids, id_column=col), sorted_gdf


def validate_problem_frame(gdf: GeoDataFrame, problem: OevkProblem) -> None:
    """Check that ``gdf`` matches ``problem`` column and CRS expectations.

    Assumes rows are already in canonical order if you rely on index alignment;
    callers should run :func:`prepare_precinct_layer` or :meth:`PrecinctIndexMap.from_frame`
    first. Raises :exc:`ProblemFrameValidationError` with aggregated ``details``.
    """
    errors: list[str] = []
    pid_col = problem.precinct_id_column

    if pid_col not in gdf.columns:
        errors.append(f"missing column {pid_col!r}")

    if gdf.empty:
        errors.append("GeoDataFrame is empty")

    if len(gdf) > 0:
        if gdf.geometry.is_empty.all():
            errors.append("all geometries are empty")
        if pid_col in gdf.columns and gdf[pid_col].duplicated().any():
            errors.append(f"duplicate {pid_col!r} values")

    if problem.county_column is not None and problem.county_column not in gdf.columns:
        errors.append(f"missing county column {problem.county_column!r}")

    if (
        problem.settlement_column is not None
        and problem.settlement_column not in gdf.columns
    ):
        errors.append(f"missing settlement column {problem.settlement_column!r}")

    if problem.pop_column is not None:
        pc = problem.pop_column
        if pc not in gdf.columns:
            errors.append(f"missing population column {pc!r}")
        elif not pd.api.types.is_numeric_dtype(gdf[pc]):
            errors.append(f"population column {pc!r} must be numeric")

    if problem.crs is not None:
        if gdf.crs is None:
            errors.append("GeoDataFrame has no CRS but problem.crs is set")
        else:
            try:
                left = ProjCRS.from_user_input(gdf.crs)
                right = ProjCRS.from_user_input(problem.crs)
                if left != right:
                    errors.append(
                        f"CRS mismatch: frame {gdf.crs!r} vs problem {problem.crs!r}",
                    )
            except Exception as e:
                errors.append(f"CRS comparison failed: {e}")

    if errors:
        msg = "problem frame validation failed: " + "; ".join(errors)
        raise ProblemFrameValidationError(msg, details=errors)


def prepare_precinct_layer(
    gdf: GeoDataFrame,
    problem: OevkProblem,
) -> tuple[GeoDataFrame, PrecinctIndexMap]:
    """Return a CRS/column-validated frame sorted by ``precinct_id`` and its index map.

    Order of operations: :meth:`PrecinctIndexMap.from_frame` (sort + unique ids),
    then :func:`validate_problem_frame` on the sorted copy.
    """
    pmap, sorted_gdf = PrecinctIndexMap.from_frame(
        gdf,
        id_column=problem.precinct_id_column,
    )
    validate_problem_frame(sorted_gdf, problem)
    return sorted_gdf, pmap
