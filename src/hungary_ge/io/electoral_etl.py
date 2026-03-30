"""Electoral ETL: list votes and focal OEVK ids from ``szavkor_topo`` JSON.

Produces ``precinct_votes.parquet`` and ``focal_oevk_assignments.parquet``.
See ``docs/data-model.md`` (Electoral tables section).
"""

from __future__ import annotations

import json
import logging
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd
from geopandas import GeoDataFrame

from hungary_ge.io.geoio import load_szavkor_settlement_json
from hungary_ge.io.precinct_etl import iter_settlement_json_paths
from hungary_ge.io.szavkor_parse import composite_precinct_id
from hungary_ge.problem import DEFAULT_PRECINCT_ID_COLUMN

logger = logging.getLogger(__name__)

DEFAULT_LIST_MAP_NAME = "election_2022_list_map.json"


def _optional_int(val: Any) -> int | None:
    if val is None or val == "":
        return None
    if isinstance(val, bool):
        return int(val)
    if isinstance(val, int):
        return val
    if isinstance(val, float):
        return int(val)
    return int(str(val).strip())


# Columns merged from votes table that are not “ballot count” fields for void masking
_VOTES_METADATA_COLUMNS: frozenset[str] = frozenset(
    {
        DEFAULT_PRECINCT_ID_COLUMN,
        "maz",
        "taz",
        "szk",
        "election_year",
        "header_vl_id",
        "header_nvv_id",
    }
)

_VOTES_JOIN_DROP: frozenset[str] = frozenset({"maz", "taz", "szk"})


@dataclass(frozen=True)
class ListPartyMap:
    """Maps ``listVotes`` string keys to stable Parquet column names."""

    election_year: int | None
    list_id_to_column: dict[str, str]


@dataclass
class ElectoralBuildStats:
    """Counts and warnings from :func:`build_electoral_tables`."""

    n_files_read: int = 0
    n_records_in: int = 0
    n_rows_votes: int = 0
    n_rows_focal: int = 0
    n_duplicate_precinct_id: int = 0
    n_records_missing_oevk_full: int = 0
    warnings: list[str] = field(default_factory=list)
    unknown_list_vote_keys: Counter[str] = field(default_factory=Counter)

    def add_warning(self, msg: str) -> None:
        self.warnings.append(msg)
        logger.warning("%s", msg)


def default_list_party_map_path() -> Path:
    """Default stub map next to this module (``io/data/election_2022_list_map.json``)."""
    return Path(__file__).resolve().parent / "data" / DEFAULT_LIST_MAP_NAME


def load_list_party_map(path: str | Path) -> ListPartyMap:
    """Load list-id mapping from JSON (``lists`` object with ``column`` per entry)."""
    path = Path(path)
    raw = json.loads(path.read_text(encoding="utf-8"))
    lists_raw = raw.get("lists") or {}
    out: dict[str, str] = {}
    for key, spec in lists_raw.items():
        kid = str(key).strip()
        if isinstance(spec, str):
            out[kid] = spec
        elif isinstance(spec, dict):
            col = spec.get("column")
            if not col or not isinstance(col, str):
                msg = f"list entry {kid!r} must have string 'column'"
                raise ValueError(msg)
            out[kid] = col
        else:
            msg = f"list entry {kid!r} must be string or object with 'column'"
            raise ValueError(msg)
    year = raw.get("election_year")
    election_year: int | None
    if year is None:
        election_year = None
    elif isinstance(year, int):
        election_year = year
    else:
        try:
            election_year = int(year)
        except (TypeError, ValueError) as exc:
            msg = f"election_year must be int or null, got {year!r}"
            raise ValueError(msg) from exc
    return ListPartyMap(election_year=election_year, list_id_to_column=out)


def build_electoral_tables(
    szavkor_root: Path,
    party_map_path: str | Path,
    *,
    strict_unknown_lists: bool = False,
) -> tuple[pd.DataFrame, pd.DataFrame, ElectoralBuildStats]:
    """Parse all settlement JSON files into votes and focal assignment tables.

    Args:
        szavkor_root: e.g. ``data/raw/szavkor_topo``.
        party_map_path: JSON file with ``lists`` mapping (see default under ``io/data/``).
        strict_unknown_lists: If True, raise when ``listVotes`` contains an id
            not present in the party map.

    Returns:
        ``(votes_df, focal_df, stats)``. Both tables use ``precinct_id`` as row
        identifier; focal may have fewer rows if ``oevk_id_full`` is missing.
    """
    root = Path(szavkor_root)
    pmap = load_list_party_map(party_map_path)
    stats = ElectoralBuildStats()
    votes_rows: list[dict[str, Any]] = []
    focal_rows: list[dict[str, Any]] = []
    seen_ids: dict[str, str] = {}

    for path in iter_settlement_json_paths(root):
        stats.n_files_read += 1
        data = load_szavkor_settlement_json(path)
        header = data.get("header") or {}
        vl_id = header.get("vl_id")
        nvv_id = header.get("nvv_id")
        for raw in data.get("list", []):
            stats.n_records_in += 1
            maz = str(raw.get("maz", "")).strip()
            taz = str(raw.get("taz", "")).strip()
            szk = str(raw.get("szk", "")).strip()
            pid = composite_precinct_id(maz, taz, szk)

            if pid in seen_ids:
                stats.n_duplicate_precinct_id += 1
                stats.add_warning(
                    f"duplicate precinct_id {pid}: keeping first from {seen_ids[pid]!s}, "
                    f"skipping {path}"
                )
                continue
            seen_ids[pid] = str(path)

            voters_val = raw.get("voters")
            voters: int | None
            if voters_val is None or voters_val == "":
                voters = None
            else:
                voters = int(voters_val)

            list_votes = raw.get("listVotes") or {}
            if not isinstance(list_votes, dict):
                msg = (
                    f"precinct {pid}: listVotes must be object, got {type(list_votes)}"
                )
                raise TypeError(msg)

            row_vote: dict[str, Any] = {
                DEFAULT_PRECINCT_ID_COLUMN: pid,
                "maz": maz,
                "taz": taz,
                "szk": szk,
                "voters": voters,
                "election_year": pmap.election_year,
                "header_vl_id": _optional_int(vl_id),
                "header_nnv_id": _optional_int(nvv_id),
            }

            for lid, count in list_votes.items():
                key = str(lid).strip()
                col = pmap.list_id_to_column.get(key)
                if col is None:
                    stats.unknown_list_vote_keys[key] += 1
                    if strict_unknown_lists:
                        msg = f"precinct {pid}: unknown listVotes key {key!r} not in party map"
                        raise ValueError(msg)
                    continue
                if col in row_vote:
                    msg = f"duplicate column {col!r} from list id collision"
                    raise ValueError(msg)
                row_vote[col] = int(count)

            votes_rows.append(row_vote)

            oevk_full = raw.get("oevk_id_full")
            oevk_local = raw.get("oevk_id")
            if oevk_full is None or oevk_full == "":
                stats.n_records_missing_oevk_full += 1
            else:
                focal_rows.append(
                    {
                        DEFAULT_PRECINCT_ID_COLUMN: pid,
                        "oevk_id_full": str(oevk_full).strip(),
                        "oevk_id": None
                        if oevk_local is None or oevk_local == ""
                        else str(oevk_local).strip(),
                        "maz": maz,
                    }
                )

    if not votes_rows:
        msg = f"no electoral rows from {root}"
        raise ValueError(msg)

    votes_df = pd.DataFrame(votes_rows)
    # Stable column order: metadata first, then vote columns sorted
    meta = [
        c
        for c in (
            DEFAULT_PRECINCT_ID_COLUMN,
            "maz",
            "taz",
            "szk",
            "voters",
            "election_year",
            "header_vl_id",
            "header_nnv_id",
        )
        if c in votes_df.columns
    ]
    vote_cols = sorted(c for c in votes_df.columns if c not in meta)
    votes_df = votes_df[meta + vote_cols]

    for key, n in stats.unknown_list_vote_keys.items():
        stats.add_warning(
            f"unknown listVotes key {key!r} skipped in {n} precinct rows; add to party map"
        )

    stats.n_rows_votes = len(votes_df)

    if not focal_rows:
        focal_df = pd.DataFrame(
            columns=[
                DEFAULT_PRECINCT_ID_COLUMN,
                "oevk_id_full",
                "oevk_id",
                "maz",
            ]
        )
    else:
        focal_df = pd.DataFrame(focal_rows)
    if not focal_df.empty:
        dup = focal_df[DEFAULT_PRECINCT_ID_COLUMN].duplicated()
        if dup.any():
            msg = f"focal table has duplicate precinct_id ({dup.sum()} rows)"
            raise ValueError(msg)
    stats.n_rows_focal = len(focal_df)

    return votes_df, focal_df, stats


def write_electoral_parquets(
    votes_df: pd.DataFrame,
    focal_df: pd.DataFrame,
    votes_path: str | Path,
    focal_path: str | Path,
) -> None:
    """Write electoral DataFrames to Parquet (creates parent dirs)."""
    vp = Path(votes_path)
    fp = Path(focal_path)
    vp.parent.mkdir(parents=True, exist_ok=True)
    fp.parent.mkdir(parents=True, exist_ok=True)
    votes_df.to_parquet(vp, index=False)
    focal_df.to_parquet(fp, index=False)


def load_votes_table(path: str | Path) -> pd.DataFrame:
    """Load ``precinct_votes.parquet`` (or any compatible parquet) as a DataFrame."""
    return pd.read_parquet(Path(path))


def load_focal_assignments(path: str | Path) -> pd.DataFrame:
    """Load ``focal_oevk_assignments.parquet`` as a DataFrame."""
    return pd.read_parquet(Path(path))


def assert_focal_assignments_valid(df: pd.DataFrame) -> None:
    """Raise ``ValueError`` if ``precinct_id`` missing, duplicated, or ``oevk_id_full`` missing."""
    if DEFAULT_PRECINCT_ID_COLUMN not in df.columns:
        msg = f"focal table missing {DEFAULT_PRECINCT_ID_COLUMN!r}"
        raise ValueError(msg)
    if "oevk_id_full" not in df.columns:
        msg = "focal table missing 'oevk_id_full'"
        raise ValueError(msg)
    dup = df[DEFAULT_PRECINCT_ID_COLUMN].duplicated()
    if dup.any():
        msg = f"duplicate precinct_id in focal table: {int(dup.sum())} rows"
        raise ValueError(msg)
    if df["oevk_id_full"].isna().any():
        msg = "focal table has null oevk_id_full"
        raise ValueError(msg)


def electoral_vote_columns(votes_df: pd.DataFrame) -> list[str]:
    """Return ballot count column names (excluding id / provenance metadata).

    Sorted lexicographically for stable iteration and masking.
    """
    return sorted(c for c in votes_df.columns if c not in _VOTES_METADATA_COLUMNS)


def join_electoral_to_gdf(
    gdf: GeoDataFrame,
    votes_df: pd.DataFrame,
    *,
    on: str = DEFAULT_PRECINCT_ID_COLUMN,
    how: str = "left",
    unit_kind_column: str = "unit_kind",
    void_unit_kind: str = "void",
    clear_vote_columns_on_void: bool = True,
) -> GeoDataFrame:
    """Left-join vote columns onto a precinct GeoDataFrame.

    ``maz`` / ``taz`` / ``szk`` are not merged from ``votes_df`` (they should
    already match the layer). For rows with ``unit_kind == void`` (when that
    column exists), ballot count columns are set to NA so void units never carry
    imputed party votes.
    """
    right_cols = [on] + [
        c for c in votes_df.columns if c not in _VOTES_JOIN_DROP and c != on
    ]
    merged = gdf.merge(votes_df[right_cols], on=on, how=how)
    if clear_vote_columns_on_void and unit_kind_column in merged.columns:
        void_mask = merged[unit_kind_column].astype(str) == void_unit_kind
        if void_mask.any():
            for col in electoral_vote_columns(votes_df):
                if col in merged.columns:
                    merged.loc[void_mask, col] = pd.NA
    return merged
