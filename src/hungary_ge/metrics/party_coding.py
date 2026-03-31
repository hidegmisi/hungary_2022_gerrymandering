"""Two-bloc party column configuration for partisan metrics (Slice 9)."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

PARTISAN_PARTY_CODING_SCHEMA_V1 = "hungary_ge.metrics.party_coding/v1"


@dataclass(frozen=True)
class PartisanPartyCoding:
    """Maps precinct vote columns into two blocs **A** vs **B** (sums).

    Multiparty list votes are reduced to two totals per precinct; downstream
    metrics (seats, efficiency gap) use those totals only.
    """

    party_a_columns: tuple[str, ...]
    party_b_columns: tuple[str, ...]
    label_a: str = "party_a"
    label_b: str = "party_b"
    schema_version: str = PARTISAN_PARTY_CODING_SCHEMA_V1
    description: str = ""

    def __post_init__(self) -> None:
        overlap = set(self.party_a_columns) & set(self.party_b_columns)
        if overlap:
            msg = f"party_a and party_b share columns: {sorted(overlap)}"
            raise ValueError(msg)
        if not self.party_a_columns and not self.party_b_columns:
            raise ValueError("at least one column in party_a or party_b")

    @property
    def all_vote_columns(self) -> tuple[str, ...]:
        return tuple(dict.fromkeys((*self.party_a_columns, *self.party_b_columns)))


def load_partisan_party_coding(path: str | Path) -> PartisanPartyCoding:
    """Load from UTF-8 JSON (see packaged ``data/partisan_party_coding.json``)."""
    p = Path(path)
    raw: dict[str, Any] = json.loads(p.read_text(encoding="utf-8"))
    return partisan_party_coding_from_dict(raw)


def partisan_party_coding_from_dict(raw: dict[str, Any]) -> PartisanPartyCoding:
    """Parse dict (e.g. from JSON)."""
    sv = raw.get("schema_version")
    if sv != PARTISAN_PARTY_CODING_SCHEMA_V1:
        msg = f"unexpected schema_version {sv!r}; expected {PARTISAN_PARTY_CODING_SCHEMA_V1!r}"
        raise ValueError(msg)
    a = raw.get("party_a_columns") or []
    b = raw.get("party_b_columns") or []
    if not isinstance(a, list) or not isinstance(b, list):
        raise TypeError("party_a_columns and party_b_columns must be lists")
    return PartisanPartyCoding(
        party_a_columns=tuple(str(x) for x in a),
        party_b_columns=tuple(str(x) for x in b),
        label_a=str(raw.get("label_a") or "party_a"),
        label_b=str(raw.get("label_b") or "party_b"),
        schema_version=str(sv),
        description=str(raw.get("description") or ""),
    )


def default_partisan_party_coding_path() -> Path:
    """Path to the packaged example JSON (may be copied to ``data/processed``)."""
    return Path(__file__).resolve().parent / "data" / "partisan_party_coding.json"


def list_map_vote_columns(list_map_path: str | Path) -> list[str]:
    """Return ``votes_*`` column names from ``election_2022_list_map.json`` (no bloc assignment)."""
    p = Path(list_map_path)
    raw = json.loads(p.read_text(encoding="utf-8"))
    lists = raw.get("lists") or {}
    cols: list[str] = []
    for _k, v in sorted(lists.items()):
        if isinstance(v, dict) and "column" in v:
            cols.append(str(v["column"]))
    return cols
