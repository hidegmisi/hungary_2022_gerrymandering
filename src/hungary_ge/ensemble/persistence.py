"""Parquet + JSON persistence for :class:`PlanEnsemble` (Slice 7)."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Sequence
from pathlib import Path
from typing import Any, Literal

import numpy as np
import pandas as pd
import pyarrow.dataset as pads

from hungary_ge.diagnostics.report import DiagnosticsReport, write_diagnostics_json
from hungary_ge.ensemble.plan_ensemble import PlanEnsemble

ENSEMBLE_MANIFEST_SCHEMA_V1 = "hungary_ge.ensemble/v1"

COL_PRECINCT_ID = "precinct_id"
COL_DRAW = "draw"
COL_DISTRICT = "district"
COL_CHAIN = "chain"

Layout = Literal["long", "wide"]

MAX_WIDE_DRAWS_DEFAULT = 1024

_WIDE_COL_RE = re.compile(r"^d(\d{6})$")


def _default_manifest_path(parquet_path: Path) -> Path:
    return parquet_path.with_suffix(".meta.json")


def default_diagnostics_json_path(parquet_path: str | Path) -> Path:
    """Default diagnostics path: same directory as parquet, ``{stem}_diagnostics.json``."""
    p = Path(parquet_path)
    return p.with_name(f"{p.stem}_diagnostics.json")


def _wide_column_name(j: int) -> str:
    return f"d{j + 1:06d}"


def _hash_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def save_plan_ensemble(
    ensemble: PlanEnsemble,
    parquet_path: str | Path,
    *,
    manifest_path: str | Path | None = None,
    layout: Layout = "long",
    compression: str = "zstd",
    max_wide_draws: int = MAX_WIDE_DRAWS_DEFAULT,
    write_sha256: bool = True,
    diagnostics_report: DiagnosticsReport | None = None,
    diagnostics_path: str | Path | None = None,
) -> None:
    """Write assignments to Parquet and a JSON sidecar manifest.

    If ``diagnostics_report`` is set, also writes UTF-8 JSON diagnostics (Slice 8)
    next to the Parquet (see :func:`default_diagnostics_json_path`) unless
    ``diagnostics_path`` overrides the location. The manifest may include a
    ``diagnostics_file`` basename when written.
    """
    parquet_path = Path(parquet_path)
    parquet_path.parent.mkdir(parents=True, exist_ok=True)

    if layout == "wide":
        if ensemble.n_draws > max_wide_draws:
            msg = (
                f"wide layout allows at most {max_wide_draws} draws, "
                f"got {ensemble.n_draws}; use layout='long'"
            )
            raise ValueError(msg)
        wide_cols = [_wide_column_name(j) for j in range(ensemble.n_draws)]
        data: dict[str, Any] = {COL_PRECINCT_ID: list(ensemble.unit_ids)}
        for j, wn in enumerate(wide_cols):
            data[wn] = [ensemble.assignments[i][j] for i in range(ensemble.n_units)]
        df = pd.DataFrame(data)
    else:
        df = ensemble.to_long_frame()

    df.to_parquet(parquet_path, index=False, compression=compression)

    meta_path = (
        Path(manifest_path)
        if manifest_path is not None
        else _default_manifest_path(parquet_path)
    )
    meta_path.parent.mkdir(parents=True, exist_ok=True)

    unit_ids = list(ensemble.unit_ids)
    manifest: dict[str, Any] = {
        "schema_version": ENSEMBLE_MANIFEST_SCHEMA_V1,
        "layout": layout,
        "assignments_file": parquet_path.name,
        "precinct_id_column": COL_PRECINCT_ID,
        "n_units": ensemble.n_units,
        "n_draws": ensemble.n_draws,
        "unit_ids": unit_ids,
        "column_map": {
            "draw": COL_DRAW,
            "district": COL_DISTRICT,
            "chain": COL_CHAIN,
        },
    }
    if ensemble.draw_ids is not None:
        manifest["draw_ids"] = list(ensemble.draw_ids)
    if ensemble.chain_or_run is not None:
        manifest["chain_per_draw"] = list(ensemble.chain_or_run)
    if ensemble.metadata:
        manifest["metadata"] = dict(ensemble.metadata)
    if layout == "wide":
        manifest["wide_draw_columns"] = [
            _wide_column_name(j) for j in range(ensemble.n_draws)
        ]
    if write_sha256 and parquet_path.is_file():
        manifest["sha256"] = _hash_file(parquet_path)

    if diagnostics_report is not None:
        dpath = (
            Path(diagnostics_path)
            if diagnostics_path is not None
            else default_diagnostics_json_path(parquet_path)
        )
        write_diagnostics_json(dpath, diagnostics_report)
        manifest["diagnostics_file"] = dpath.name

    meta_path.write_text(
        json.dumps(manifest, indent=2, default=str) + "\n",
        encoding="utf-8",
    )


def _load_manifest(
    parquet_path: Path,
    manifest_path: Path | None,
) -> dict[str, Any] | None:
    mp = (
        Path(manifest_path)
        if manifest_path is not None
        else _default_manifest_path(parquet_path)
    )
    if not mp.is_file():
        return None
    return json.loads(mp.read_text(encoding="utf-8"))


def _plan_from_long_df(
    df: pd.DataFrame,
    meta: dict[str, Any] | None,
) -> PlanEnsemble:
    pcol = COL_PRECINCT_ID
    if meta and meta.get("precinct_id_column"):
        pcol = str(meta["precinct_id_column"])

    draw_col, dist_col, chain_col = COL_DRAW, COL_DISTRICT, COL_CHAIN
    if meta and isinstance(meta.get("column_map"), dict):
        cm = meta["column_map"]
        draw_col = str(cm.get("draw", draw_col))
        dist_col = str(cm.get("district", dist_col))
        chain_col = str(cm.get("chain", chain_col))

    for c in (pcol, draw_col, dist_col):
        if c not in df.columns:
            msg = f"long parquet missing column {c!r}"
            raise ValueError(msg)

    if meta and "unit_ids" in meta:
        unit_ids = tuple(str(x) for x in meta["unit_ids"])
    else:
        unit_ids = tuple(sorted(df[pcol].astype(str).unique()))

    n_u = len(unit_ids)

    work = df[[pcol, draw_col, dist_col]].copy()
    work["_pid"] = work[pcol].astype(str)
    work["_draw"] = pd.to_numeric(work[draw_col], errors="coerce")
    if work["_draw"].isna().any():
        msg = f"long parquet: non-numeric values in {draw_col!r}"
        raise ValueError(msg)
    work["_draw"] = work["_draw"].astype(np.int64)
    work[dist_col] = pd.to_numeric(work[dist_col], errors="coerce")
    if work[dist_col].isna().any():
        msg = f"long parquet: non-numeric values in {dist_col!r}"
        raise ValueError(msg)

    piv = work.pivot_table(
        index="_pid",
        columns="_draw",
        values=dist_col,
        aggfunc="first",
    )
    draws_tuple = tuple(sorted(int(c) for c in piv.columns))
    n_d = len(draws_tuple)

    piv = piv.reindex(index=list(unit_ids), columns=list(draws_tuple))
    if piv.isna().any().any():
        msg = (
            "long parquet: incomplete (precinct_id, draw) grid after pivot "
            f"(expected {n_u} units × {n_d} draws; check duplicates or missing pairs)"
        )
        raise ValueError(msg)

    arr = piv.to_numpy(dtype=np.int64, copy=False)
    plan_columns = [arr[:, j].astype(int).tolist() for j in range(n_d)]

    chain_in_df = chain_col in df.columns
    ch: tuple[int, ...] | None = None
    if chain_in_df:
        g = df.assign(
            _draw_norm=pd.to_numeric(df[draw_col], errors="coerce").astype(np.int64)
        ).groupby("_draw_norm", sort=False)[chain_col]
        first_by_draw = g.first()
        chain_vals = [int(first_by_draw.loc[d]) for d in draws_tuple]
        ch = tuple(chain_vals)

    meta_d: dict[str, object] = (
        dict(meta["metadata"]) if meta and meta.get("metadata") else {}
    )
    return PlanEnsemble.from_columns(
        unit_ids,
        plan_columns,
        draw_ids=draws_tuple,
        chain_or_run=ch,
        metadata=meta_d,
    )


def _plan_from_wide_df(df: pd.DataFrame, meta: dict[str, Any]) -> PlanEnsemble:
    pcol = str(meta.get("precinct_id_column", COL_PRECINCT_ID))
    if pcol not in df.columns:
        msg = f"wide parquet missing {pcol!r}"
        raise ValueError(msg)

    wide_names = meta.get("wide_draw_columns")
    if not wide_names:
        wide_names = [c for c in df.columns if c != pcol and _WIDE_COL_RE.match(str(c))]

        def _wide_sort_key(name: object) -> int:
            m = _WIDE_COL_RE.match(str(name))
            return int(m.group(1)) if m else 0

        wide_names = sorted(wide_names, key=_wide_sort_key)
    unit_ids_list = df[pcol].astype(str).tolist()
    uid = tuple(unit_ids_list)
    if meta.get("unit_ids") is not None:
        uid_meta = tuple(str(x) for x in meta["unit_ids"])
        if uid_meta != uid:
            msg = "wide parquet precinct order does not match manifest unit_ids"
            raise ValueError(msg)

    plan_columns: list[list[int]] = []
    for wn in wide_names:
        plan_columns.append([int(x) for x in df[wn].tolist()])

    d_ids = meta.get("draw_ids")
    draw_ids = tuple(int(x) for x in d_ids) if d_ids is not None else None
    ch_raw = meta.get("chain_per_draw")
    ch = tuple(int(x) for x in ch_raw) if ch_raw is not None else None
    meta_d = dict(meta["metadata"]) if meta.get("metadata") else {}
    return PlanEnsemble.from_columns(
        uid,
        plan_columns,
        draw_ids=draw_ids,
        chain_or_run=ch,
        metadata=meta_d,
    )


def load_plan_ensemble(
    parquet_path: str | Path,
    *,
    manifest_path: str | Path | None = None,
) -> PlanEnsemble:
    """Load :class:`PlanEnsemble` from Parquet; uses sidecar manifest when present."""
    parquet_path = Path(parquet_path)
    meta = _load_manifest(parquet_path, Path(manifest_path) if manifest_path else None)
    df = pd.read_parquet(parquet_path)

    layout: Layout = "long"
    if meta:
        if meta.get("schema_version") != ENSEMBLE_MANIFEST_SCHEMA_V1:
            sv = meta.get("schema_version")
            msg = f"unsupported ensemble manifest schema_version: {sv!r}"
            raise ValueError(msg)
        layout = str(meta.get("layout", "long"))  # type: ignore[assignment]
        if layout not in ("long", "wide"):
            msg = f"invalid layout {layout!r}"
            raise ValueError(msg)

    if layout == "wide":
        if meta is None:
            msg = "wide ensemble parquet requires manifest"
            raise ValueError(msg)
        return _plan_from_wide_df(df, meta)

    return _plan_from_long_df(df, meta)


def load_plan_ensemble_draw_column(
    parquet_path: str | Path,
    draw: int,
    *,
    manifest_path: str | Path | None = None,
    unit_ids: Sequence[str] | None = None,
) -> np.ndarray:
    """Load district assignments for a single ``draw`` (long layout; filtered read)."""
    parquet_path = Path(parquet_path)
    meta = _load_manifest(parquet_path, Path(manifest_path) if manifest_path else None)
    if meta and str(meta.get("layout", "long")) != "long":
        msg = "load_plan_ensemble_draw_column only supports long layout"
        raise ValueError(msg)

    draw_col, dist_col, pcol = COL_DRAW, COL_DISTRICT, COL_PRECINCT_ID
    if meta and isinstance(meta.get("column_map"), dict):
        cm = meta["column_map"]
        draw_col = str(cm.get("draw", draw_col))
        dist_col = str(cm.get("district", dist_col))
        pcol = str(meta.get("precinct_id_column", pcol))

    uid: tuple[str, ...]
    if unit_ids is not None:
        uid = tuple(unit_ids)
    elif meta and meta.get("unit_ids"):
        uid = tuple(str(x) for x in meta["unit_ids"])
    else:
        msg = "unit_ids required when manifest has no unit_ids"
        raise ValueError(msg)

    dataset = pads.dataset(parquet_path, format="parquet")
    table = dataset.to_table(
        filter=(pads.field(draw_col) == draw), columns=[pcol, dist_col]
    )
    sub = table.to_pandas()
    sub_u = sub.drop_duplicates(subset=[pcol], keep="first")
    if len(sub_u) != len(uid):
        msg = (
            f"draw {draw}: expected {len(uid)} unique {pcol!r} rows, got {len(sub_u)} "
            f"(raw rows {len(sub)})"
        )
        raise ValueError(msg)

    s = sub_u.set_index(sub_u[pcol].astype(str))[dist_col]
    aligned = s.reindex(list(uid))
    if aligned.isna().any():
        msg = f"draw {draw}: precinct_id set does not match manifest unit_ids order"
        raise ValueError(msg)
    return aligned.astype(np.int32).to_numpy()
