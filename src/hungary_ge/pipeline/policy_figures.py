"""Policy memo figures generated from county reports and rollups."""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from tqdm import tqdm

from hungary_ge.config import (
    COUNTY_DIAGNOSTICS_JSON,
    COUNTY_PARTISAN_REPORT_JSON,
    ENSEMBLE_ASSIGNMENTS_PARQUET,
    ProcessedPaths,
)
from hungary_ge.ensemble.persistence import load_plan_ensemble
from hungary_ge.ensemble.plan_ensemble import PlanEnsemble
from hungary_ge.io.electoral_etl import load_focal_assignments, load_votes_table
from hungary_ge.metrics.balance import apply_two_bloc_vote_balance
from hungary_ge.metrics.compare import metrics_for_assignment
from hungary_ge.metrics.party_coding import (
    PartisanPartyCoding,
    default_partisan_party_coding_path,
    load_partisan_party_coding,
)
from hungary_ge.metrics.policy import DEFAULT_METRIC_COMPUTATION_POLICY
from hungary_ge.pipeline.progress import county_progress_disabled
from hungary_ge.problem import DEFAULT_PRECINCT_ID_COLUMN

STYLE_CHOICES = ("memo-light", "memo-print")
DEFAULT_STYLE = "memo-light"

_METRICS_FOR_PLOTS: tuple[str, ...] = (
    "seat_share_a",
    "efficiency_gap",
    "mean_median_a_share_diff",
)

PERCENTILE_HEATMAP_SUBTITLE = (
    "Higher percentile means the enacted map is higher in the draw distribution for that "
    "metric; direction depends on each metric's sign convention."
)

EFFICIENCY_GAP_SIGN_NOTE = "Efficiency gap (percentage points, positive favors bloc B)"

_COUNTY_NAME_BY_MAZ: dict[str, str] = {
    "01": "Budapest",
    "02": "Baranya",
    "03": "Bacs-Kiskun",
    "04": "Bekes",
    "05": "Borsod-Abauj-Zemplen",
    "06": "Csongrad-Csanad",
    "07": "Fejer",
    "08": "Gyor-Moson-Sopron",
    "09": "Hajdu-Bihar",
    "10": "Heves",
    "11": "Jasz-Nagykun-Szolnok",
    "12": "Komarom-Esztergom",
    "13": "Nograd",
    "14": "Pest",
    "15": "Somogy",
    "16": "Szabolcs-Szatmar-Bereg",
    "17": "Tolna",
    "18": "Vas",
    "19": "Veszprem",
    "20": "Zala",
}


@dataclass(frozen=True)
class StylePreset:
    name: str
    colors: dict[str, str]
    figsize_single: tuple[float, float]
    figsize_landscape: tuple[float, float]
    figsize_panels: tuple[float, float]


@dataclass(frozen=True)
class FigureSpec:
    filename: str
    title: str
    section: str
    source: str
    takeaway: str


@dataclass(frozen=True)
class CountyDrawSeries:
    draws: np.ndarray
    focal: float
    n_draws: int


_STYLE_PRESETS: dict[str, StylePreset] = {
    "memo-light": StylePreset(
        name="memo-light",
        colors={
            "focal": "#1b4d8c",
            "ensemble": "#8f98a3",
            "interval": "#d8dee6",
            "warning": "#c55a11",
            "accent": "#2b7a78",
            "text": "#1a1a1a",
            "grid": "#d0d7de",
        },
        figsize_single=(6.8, 4.2),
        figsize_landscape=(10.5, 5.6),
        figsize_panels=(10.5, 7.2),
    ),
    "memo-print": StylePreset(
        name="memo-print",
        colors={
            "focal": "#0f3057",
            "ensemble": "#767676",
            "interval": "#cbcbcb",
            "warning": "#7f3b08",
            "accent": "#2f4858",
            "text": "#111111",
            "grid": "#b9b9b9",
        },
        figsize_single=(6.8, 4.2),
        figsize_landscape=(10.5, 5.6),
        figsize_panels=(10.5, 7.2),
    ),
}


def _mpl(style_name: str):
    import matplotlib

    matplotlib.use("Agg", force=True)
    import matplotlib.pyplot as plt
    from matplotlib.ticker import FuncFormatter, PercentFormatter

    if style_name not in _STYLE_PRESETS:
        raise ValueError(f"unsupported style: {style_name!r}")
    preset = _STYLE_PRESETS[style_name]
    plt.rcParams.update(
        {
            "font.size": 10,
            "axes.titlesize": 12,
            "axes.labelsize": 10,
            "xtick.labelsize": 9,
            "ytick.labelsize": 9,
            "legend.fontsize": 9,
            "axes.grid": True,
            "grid.alpha": 0.35,
            "grid.color": preset.colors["grid"],
            "grid.linewidth": 0.7,
            "axes.facecolor": "white",
            "figure.facecolor": "white",
            "axes.spines.top": False,
            "axes.spines.right": False,
            "legend.frameon": False,
            "savefig.dpi": 300,
        },
    )
    return plt, PercentFormatter, FuncFormatter, preset


def _phase_log(prefix: str, message: str) -> None:
    """Always-visible heartbeat on stdout (tqdm often targets stderr and may be disabled)."""
    line = f"{prefix}{message}" if prefix else f"policy_figures: {message}"
    print(line, flush=True)  # noqa: T201


def style_preset(style_name: str) -> StylePreset:
    if style_name not in _STYLE_PRESETS:
        raise ValueError(f"unsupported style: {style_name!r}")
    return _STYLE_PRESETS[style_name]


def load_rollup(paths: ProcessedPaths, run_id: str) -> dict[str, Any]:
    p = paths.national_report_path(run_id)
    if not p.is_file():
        raise FileNotFoundError(f"missing national rollup: {p}")
    return json.loads(p.read_text(encoding="utf-8"))


def _county_code_list(national_report: dict[str, Any]) -> list[str]:
    seat_block = (
        national_report.get("partisan", {})
        .get("metrics", {})
        .get("seat_share_a", {})
        .get("by_county", [])
    )
    out: list[str] = []
    for row in seat_block:
        m = str(row.get("maz", "")).strip()
        if m:
            out.append(m)
    return out


def load_county_reports(
    paths: ProcessedPaths,
    run_id: str,
    maz_list: list[str],
    *,
    no_progress: bool = False,
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    partisan: dict[str, dict[str, Any]] = {}
    diagnostics: dict[str, dict[str, Any]] = {}
    disable = county_progress_disabled(no_progress=no_progress)
    iterator = tqdm(
        maz_list,
        desc="policy_figures load reports",
        unit="county",
        file=sys.stderr,
        disable=disable,
    )
    for maz in iterator:
        rep_dir = paths.county_reports_dir(run_id, maz)
        p_part = rep_dir / COUNTY_PARTISAN_REPORT_JSON
        p_diag = rep_dir / COUNTY_DIAGNOSTICS_JSON
        if not p_part.is_file() or not p_diag.is_file():
            raise FileNotFoundError(f"missing county reports for {maz}: {rep_dir}")
        partisan[maz] = json.loads(p_part.read_text(encoding="utf-8"))
        diagnostics[maz] = json.loads(p_diag.read_text(encoding="utf-8"))
    return partisan, diagnostics


def _national_metric_df(national_report: dict[str, Any], metric_name: str) -> pd.DataFrame:
    rows = (
        national_report.get("partisan", {})
        .get("metrics", {})
        .get(metric_name, {})
        .get("by_county", [])
    )
    out = pd.DataFrame(rows)
    if out.empty:
        raise ValueError(f"national report has no rows for metric {metric_name!r}")
    return out


def select_focus_counties(national_report: dict[str, Any], n: int = 4) -> list[str]:
    seat = _national_metric_df(national_report, "seat_share_a").copy()
    seat["delta_abs"] = (
        pd.to_numeric(seat["focal_value"], errors="coerce")
        - pd.to_numeric(seat["ensemble_mean"], errors="coerce")
    ).abs()
    seat["weight"] = pd.to_numeric(seat["weight"], errors="coerce")
    seat = seat.sort_values(["delta_abs", "weight"], ascending=[False, False])

    selected: list[str] = []
    for fixed in ("01", "14"):
        if fixed in set(seat["maz"].astype(str)):
            selected.append(fixed)
    for maz in seat["maz"].astype(str).tolist():
        if maz not in selected:
            selected.append(maz)
        if len(selected) >= n:
            break
    return selected[:n]


def _aligned_votes(
    unit_ids: tuple[str, ...],
    votes: pd.DataFrame,
    coding: PartisanPartyCoding,
) -> tuple[np.ndarray, np.ndarray]:
    sub = votes.drop_duplicates(subset=[DEFAULT_PRECINCT_ID_COLUMN]).copy()
    sub["_pid"] = sub[DEFAULT_PRECINCT_ID_COLUMN].astype(str)
    lookup = sub.set_index("_pid", drop=True)
    va = np.zeros(len(unit_ids), dtype=float)
    vb = np.zeros(len(unit_ids), dtype=float)
    for i, pid in enumerate(unit_ids):
        sp = str(pid)
        if sp not in lookup.index:
            continue
        row = lookup.loc[sp]
        va[i] = sum(float(row.get(c) or 0.0) for c in coding.party_a_columns)
        vb[i] = sum(float(row.get(c) or 0.0) for c in coding.party_b_columns)
    return va, vb


def _aligned_focal_labels(unit_ids: tuple[str, ...], focal_df: pd.DataFrame) -> list[Any | None]:
    if "oevk_id_full" not in focal_df.columns:
        raise ValueError("focal assignments missing 'oevk_id_full'")
    sub = focal_df.drop_duplicates(subset=[DEFAULT_PRECINCT_ID_COLUMN]).copy()
    sub["_pid"] = sub[DEFAULT_PRECINCT_ID_COLUMN].astype(str)
    lookup = sub.set_index("_pid", drop=True)["oevk_id_full"]
    labels: list[Any | None] = []
    for pid in unit_ids:
        sp = str(pid)
        if sp not in lookup.index:
            labels.append(None)
            continue
        val = lookup.loc[sp]
        labels.append(None if pd.isna(val) else val)
    return labels


def _load_party_coding(path: Path | None) -> PartisanPartyCoding:
    if path is None:
        return load_partisan_party_coding(default_partisan_party_coding_path())
    return load_partisan_party_coding(path)


def _load_ensemble_and_vote_arrays(
    *,
    paths: ProcessedPaths,
    run_id: str,
    maz: str,
    votes_parquet: Path,
    focal_parquet: Path,
    party_coding_path: Path | None,
    log_prefix: str,
) -> tuple[PlanEnsemble, np.ndarray, np.ndarray, list[Any | None]]:
    """Load one county ensemble Parquet and align votes/focal to ``unit_ids``."""
    ens_path = paths.county_ensemble_dir(run_id, maz) / ENSEMBLE_ASSIGNMENTS_PARQUET
    if not ens_path.is_file():
        raise FileNotFoundError(f"missing ensemble assignments for {maz}: {ens_path}")

    size_mb = ens_path.stat().st_size / (1024 * 1024)
    _phase_log(
        log_prefix,
        f"policy_figures: loading ensemble Parquet maz={maz} ({size_mb:.1f} MiB) — "
        "Parquet is small on disk but long-layout rebuild + Python loops can take tens of seconds; "
        "no draw progress until load finishes.",
    )
    ensemble = load_plan_ensemble(ens_path)
    uid_set = frozenset(str(x) for x in ensemble.unit_ids)

    votes_all = load_votes_table(votes_parquet)
    votes = votes_all[
        votes_all[DEFAULT_PRECINCT_ID_COLUMN].astype(str).isin(uid_set)
    ].copy()
    focal_all = load_focal_assignments(focal_parquet)
    focal = focal_all[
        focal_all[DEFAULT_PRECINCT_ID_COLUMN].astype(str).isin(uid_set)
    ].copy()

    coding = _load_party_coding(party_coding_path)
    va, vb = _aligned_votes(ensemble.unit_ids, votes, coding)
    focal_lbl = _aligned_focal_labels(ensemble.unit_ids, focal)
    return ensemble, va, vb, focal_lbl


def _draw_series_for_metrics(
    *,
    ensemble: PlanEnsemble,
    va: np.ndarray,
    vb: np.ndarray,
    focal_lbl: list[Any | None],
    metric_names: tuple[str, ...],
    maz: str,
    no_progress: bool,
    draw_pbar_desc: str | None,
) -> dict[str, CountyDrawSeries]:
    for name in metric_names:
        if name not in _METRICS_FOR_PLOTS:
            raise ValueError(f"unsupported metric for draw series: {name!r}")

    policy = DEFAULT_METRIC_COMPUTATION_POLICY
    va_b, vb_b, _bal = apply_two_bloc_vote_balance(va, vb, policy)

    disable_draws = county_progress_disabled(no_progress=no_progress)
    desc = draw_pbar_desc or f"{maz} " + "+".join(metric_names)
    draw_cols: dict[str, list[float]] = {n: [] for n in metric_names}
    for j in tqdm(
        range(ensemble.n_draws),
        desc=desc,
        unit="draw",
        file=sys.stderr,
        leave=False,
        disable=disable_draws,
    ):
        assign = [ensemble.assignments[i][j] for i in range(ensemble.n_units)]
        met = metrics_for_assignment(
            assign,
            va_b,
            vb_b,
            metric_policy=policy,
            balance_already_applied=True,
        )
        for name in metric_names:
            draw_cols[name].append(float(met[name]))

    idx = [i for i, lab in enumerate(focal_lbl) if lab is not None]
    focal_assign = [focal_lbl[i] for i in idx]
    focal_va = va_b[idx]
    focal_vb = vb_b[idx]
    focal_met = metrics_for_assignment(
        focal_assign,
        focal_va,
        focal_vb,
        metric_policy=policy,
        balance_already_applied=True,
    )

    out: dict[str, CountyDrawSeries] = {}
    for name in metric_names:
        out[name] = CountyDrawSeries(
            draws=np.asarray(draw_cols[name]),
            focal=float(focal_met[name]),
            n_draws=ensemble.n_draws,
        )
    return out


def compute_draw_metric_series(
    *,
    paths: ProcessedPaths,
    run_id: str,
    maz: str,
    metric_name: str,
    votes_parquet: Path,
    focal_parquet: Path,
    party_coding_path: Path | None,
    log_prefix: str = "",
    no_progress: bool = False,
    draw_pbar_desc: str | None = None,
) -> CountyDrawSeries:
    ensemble, va, vb, focal_lbl = _load_ensemble_and_vote_arrays(
        paths=paths,
        run_id=run_id,
        maz=maz,
        votes_parquet=votes_parquet,
        focal_parquet=focal_parquet,
        party_coding_path=party_coding_path,
        log_prefix=log_prefix,
    )
    multi = _draw_series_for_metrics(
        ensemble=ensemble,
        va=va,
        vb=vb,
        focal_lbl=focal_lbl,
        metric_names=(metric_name,),
        maz=maz,
        no_progress=no_progress,
        draw_pbar_desc=draw_pbar_desc,
    )
    return multi[metric_name]


def _add_caption(fig, *, title: str, subtitle: str, source: str, takeaway: str) -> None:
    fig.suptitle(title, x=0.02, y=0.98, ha="left", va="top", fontweight="bold")
    fig.text(0.02, 0.93, subtitle, ha="left", va="top", fontsize=10)
    fig.text(0.02, 0.02, f"Source: {source}", ha="left", va="bottom", fontsize=8)
    fig.text(0.98, 0.02, takeaway, ha="right", va="bottom", fontsize=8)


def _save(fig, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=300, bbox_inches="tight")


def _metric_df_from_national(national: dict[str, Any], metric: str) -> pd.DataFrame:
    df = _national_metric_df(national, metric).copy()
    for c in ("weight", "focal_value", "ensemble_mean", "percentile_rank"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def _efficiency_gap_plot_frame(
    county_partisan: dict[str, dict[str, Any]],
    national_report: dict[str, Any],
) -> pd.DataFrame:
    w = _metric_df_from_national(national_report, "efficiency_gap")[["maz", "weight"]]
    rows: list[dict[str, Any]] = []
    for maz, payload in county_partisan.items():
        block = payload.get("metrics", {}).get("efficiency_gap", {})
        vote_share_block = payload.get("metrics", {}).get("vote_share_a", {})
        rows.append(
            {
                "maz": maz,
                "focal_value": pd.to_numeric(block.get("focal_value"), errors="coerce"),
                "ensemble_p05": pd.to_numeric(block.get("ensemble_p05"), errors="coerce"),
                "ensemble_p95": pd.to_numeric(block.get("ensemble_p95"), errors="coerce"),
                "vote_share_a": pd.to_numeric(
                    vote_share_block.get("focal_value"),
                    errors="coerce",
                ),
            }
        )
    df = pd.DataFrame(rows).merge(w, on="maz", how="left")
    df["county_label"] = df["maz"].map(_county_display_label)
    return df.sort_values(["vote_share_a", "weight"], ascending=[False, False]).reset_index(
        drop=True
    )


def _county_display_label(maz: str) -> str:
    code = str(maz).strip().zfill(2)
    name = _COUNTY_NAME_BY_MAZ.get(code)
    if name:
        return name
    return f"maz {code}"


def _compact_party_label(label: str) -> str:
    s = str(label).strip()
    if "(" in s:
        s = s.split("(", 1)[0].strip()
    return s


def render_plot_01(
    out: Path,
    *,
    national_report: dict[str, Any],
    style_name: str,
    spec: FigureSpec,
) -> None:
    plt, _, _, preset = _mpl(style_name)
    metrics = _METRICS_FOR_PLOTS
    labels = ["Seat Share (A)", "Efficiency Gap", "Mean-Median Diff"]
    focal = []
    ens = []
    for m in metrics:
        block = national_report["partisan"]["metrics"][m]
        focal.append(float(block["weighted_mean_focal"]) * 100.0)
        ens.append(float(block["weighted_mean_ensemble_mean"]) * 100.0)

    x = np.arange(len(metrics))
    width = 0.36
    fig, ax = plt.subplots(figsize=preset.figsize_single)
    ax.bar(x - width / 2, focal, width, color=preset.colors["focal"], label="Enacted")
    ax.bar(x + width / 2, ens, width, color=preset.colors["ensemble"], label="Ensemble mean")
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.set_ylabel("Percentage points")
    ax.legend(loc="upper right")
    ax.axhline(0.0, color=preset.colors["text"], linewidth=0.8)
    _add_caption(
        fig,
        title=spec.title,
        subtitle="Weighted national summary (district-count weights).",
        source=spec.source,
        takeaway=spec.takeaway,
    )
    fig.subplots_adjust(top=0.82, bottom=0.16)
    _save(fig, out)
    plt.close(fig)


def render_plot_02(
    out: Path,
    *,
    national_report: dict[str, Any],
    style_name: str,
    spec: FigureSpec,
) -> None:
    plt, _, _, preset = _mpl(style_name)
    metrics = list(_METRICS_FOR_PLOTS)
    frames = []
    for m in metrics:
        df = _metric_df_from_national(national_report, m)[["maz", "weight", "percentile_rank"]]
        df = df.rename(columns={"percentile_rank": m})
        frames.append(df)
    merged = frames[0]
    for df in frames[1:]:
        merged = merged.merge(df[["maz", df.columns[-1]]], on="maz", how="inner")
    merged = merged.sort_values("weight", ascending=False).reset_index(drop=True)
    matrix = merged[metrics].to_numpy(dtype=float)

    fig, ax = plt.subplots(figsize=preset.figsize_landscape)
    im = ax.imshow(matrix, cmap="viridis", aspect="auto", vmin=0, vmax=100)
    ax.set_yticks(np.arange(len(merged)))
    ax.set_yticklabels(merged["maz"].tolist())
    ax.set_xticks(np.arange(len(metrics)))
    ax.set_xticklabels(["Seat Share", "Eff Gap", "Mean-Median"])
    for i in range(matrix.shape[0]):
        for j in range(matrix.shape[1]):
            val = matrix[i, j]
            if np.isfinite(val):
                ax.text(j, i, f"{val:.0f}", ha="center", va="center", fontsize=8, color="white")
    cbar = fig.colorbar(im, ax=ax, fraction=0.024, pad=0.02)
    cbar.set_label("Percentile rank of enacted map")
    _add_caption(
        fig,
        title=spec.title,
        subtitle=PERCENTILE_HEATMAP_SUBTITLE,
        source=spec.source,
        takeaway=spec.takeaway,
    )
    fig.subplots_adjust(top=0.82, bottom=0.14)
    _save(fig, out)
    plt.close(fig)


def render_plot_03(
    out: Path,
    *,
    national_report: dict[str, Any],
    style_name: str,
    spec: FigureSpec,
) -> None:
    plt, _, _, preset = _mpl(style_name)
    df = _metric_df_from_national(national_report, "seat_share_a").copy()
    df["delta_pp"] = (df["focal_value"] - df["ensemble_mean"]) * 100.0
    df = df.sort_values("delta_pp")
    y = np.arange(len(df))
    sizes = 40.0 + 220.0 * df["weight"].fillna(0.0).to_numpy(dtype=float)

    fig, ax = plt.subplots(figsize=preset.figsize_landscape)
    ax.hlines(y, 0.0, df["delta_pp"], color=preset.colors["interval"], linewidth=2)
    ax.scatter(df["delta_pp"], y, s=sizes, color=preset.colors["focal"], alpha=0.95)
    ax.set_yticks(y)
    ax.set_yticklabels(df["maz"].astype(str))
    ax.set_xlabel("Enacted - Ensemble mean (seat share, percentage points)")
    ax.axvline(0.0, color=preset.colors["text"], linewidth=0.9)
    _add_caption(
        fig,
        title=spec.title,
        subtitle="Positive values indicate enacted map yields higher bloc-A seat share than neutral baseline.",
        source=spec.source,
        takeaway=spec.takeaway,
    )
    fig.subplots_adjust(top=0.82, bottom=0.15)
    _save(fig, out)
    plt.close(fig)


def render_plot_04(
    out: Path,
    *,
    county_partisan: dict[str, dict[str, Any]],
    national_report: dict[str, Any],
    style_name: str,
    spec: FigureSpec,
) -> None:
    plt, _, _, preset = _mpl(style_name)
    labels_nat = national_report.get("partisan", {})
    party_label_a = str(labels_nat.get("party_label_a") or "Bloc A")
    party_label_b = str(labels_nat.get("party_label_b") or "Bloc B")
    party_label_a_compact = _compact_party_label(party_label_a)
    party_label_b_compact = _compact_party_label(party_label_b)
    df = _efficiency_gap_plot_frame(county_partisan, national_report)
    y = np.arange(len(df))

    fig, ax = plt.subplots(figsize=preset.figsize_landscape)
    ax.hlines(
        y,
        df["ensemble_p05"] * 100.0,
        df["ensemble_p95"] * 100.0,
        color=preset.colors["interval"],
        linewidth=4,
        label="Ensemble p05-p95",
    )
    ax.scatter(
        df["focal_value"] * 100.0,
        y,
        color=preset.colors["focal"],
        s=42,
        label="Enacted map",
        zorder=3,
    )
    ax.axvline(0.0, color=preset.colors["text"], linewidth=0.9)
    ax.set_yticks(y)
    ax.set_yticklabels(df["county_label"].astype(str))
    ax.set_xlabel(EFFICIENCY_GAP_SIGN_NOTE)
    ax.text(
        0.01,
        0.98,
        f"Bloc A: {party_label_a_compact}\nBloc B: {party_label_b_compact}",
        transform=ax.transAxes,
        ha="left",
        va="top",
        fontsize=8,
        color=preset.colors["text"],
    )
    ax.legend(loc="lower right")
    _add_caption(
        fig,
        title=spec.title,
        subtitle=(
            "Counties sorted by bloc-A vote share (descending). "
            "Dots outside intervals indicate atypical enacted efficiency-gap outcomes."
        ),
        source=spec.source,
        takeaway=spec.takeaway,
    )
    fig.subplots_adjust(top=0.82, bottom=0.15)
    _save(fig, out)
    plt.close(fig)


def _panel_hist(
    out: Path,
    *,
    series_by_county: dict[str, CountyDrawSeries],
    style_name: str,
    spec: FigureSpec,
    metric_label: str,
    to_percent_points: bool,
) -> None:
    plt, _, _, preset = _mpl(style_name)
    fig, axes = plt.subplots(2, 2, figsize=preset.figsize_panels, sharex=False, sharey=False)
    axes_f = axes.flatten()
    counties = sorted(series_by_county.keys())
    for i, maz in enumerate(counties):
        ax = axes_f[i]
        s = series_by_county[maz]
        draws = s.draws * (100.0 if to_percent_points else 1.0)
        focal = s.focal * (100.0 if to_percent_points else 1.0)
        ax.hist(draws, bins=16, color=preset.colors["ensemble"], alpha=0.8)
        ax.axvline(focal, color=preset.colors["focal"], linewidth=2.2)
        ax.set_title(f"maz {maz} (n_draws={s.n_draws})")
        ax.set_xlabel(metric_label)
        ax.set_ylabel("Draw count")
    for j in range(len(counties), 4):
        axes_f[j].axis("off")
    _add_caption(
        fig,
        title=spec.title,
        subtitle="Vertical line is enacted map; histogram is neutral draw distribution.",
        source=spec.source,
        takeaway=spec.takeaway,
    )
    fig.subplots_adjust(top=0.82, bottom=0.14, hspace=0.35, wspace=0.22)
    _save(fig, out)
    plt.close(fig)


def render_plot_05(
    out: Path,
    *,
    seat_draws: dict[str, CountyDrawSeries],
    style_name: str,
    spec: FigureSpec,
) -> None:
    _panel_hist(
        out,
        series_by_county=seat_draws,
        style_name=style_name,
        spec=spec,
        metric_label="Seat share A (percentage points)",
        to_percent_points=True,
    )


def render_plot_06(
    out: Path,
    *,
    eg_draws: dict[str, CountyDrawSeries],
    style_name: str,
    spec: FigureSpec,
) -> None:
    _panel_hist(
        out,
        series_by_county=eg_draws,
        style_name=style_name,
        spec=spec,
        metric_label="Efficiency gap (percentage points)",
        to_percent_points=True,
    )


def render_plot_07(
    out: Path,
    *,
    county_diagnostics: dict[str, dict[str, Any]],
    focus_counties: list[str],
    style_name: str,
    spec: FigureSpec,
) -> None:
    plt, _, _, preset = _mpl(style_name)
    fig, axes = plt.subplots(2, 2, figsize=preset.figsize_panels, sharex=False, sharey=False)
    axes_f = axes.flatten()
    for i, maz in enumerate(focus_counties):
        if i >= 4:
            break
        ax = axes_f[i]
        d = county_diagnostics[maz]
        pop = d.get("population", {})
        vals = np.asarray(pop.get("per_draw_max_abs_rel_deviation") or [], dtype=float)
        mean_val = float(pd.to_numeric(pop.get("mean_of_max_abs_rel_deviation"), errors="coerce"))
        if vals.size > 0:
            ax.hist(vals * 100.0, bins=16, color=preset.colors["ensemble"], alpha=0.8)
        ax.axvline(mean_val * 100.0, color=preset.colors["warning"], linewidth=2.2)
        ax.set_title(f"maz {maz}")
        ax.set_xlabel("Max abs relative pop deviation (%)")
        ax.set_ylabel("Draw count")
    for j in range(len(focus_counties), 4):
        axes_f[j].axis("off")
    _add_caption(
        fig,
        title=spec.title,
        subtitle="Shows how tightly each county's draws meet population-balance targets.",
        source=spec.source,
        takeaway=spec.takeaway,
    )
    fig.subplots_adjust(top=0.82, bottom=0.14, hspace=0.35, wspace=0.22)
    _save(fig, out)
    plt.close(fig)


def render_plot_08(
    out: Path,
    *,
    national_report: dict[str, Any],
    style_name: str,
    spec: FigureSpec,
) -> None:
    plt, _, _, preset = _mpl(style_name)
    by_county = pd.DataFrame(
        national_report.get("diagnostics_summary", {}).get("by_county", [])
    ).copy()
    if by_county.empty:
        raise ValueError("national report missing diagnostics_summary.by_county")
    by_county["n_draws"] = pd.to_numeric(by_county["n_draws"], errors="coerce")
    by_county["ensemble_n_unique_draws"] = pd.to_numeric(
        by_county["ensemble_n_unique_draws"], errors="coerce"
    )
    by_county["weight"] = pd.to_numeric(by_county["weight"], errors="coerce")
    by_county["unique_frac"] = (
        by_county["ensemble_n_unique_draws"] / by_county["n_draws"]
    ).fillna(0.0)
    by_county = by_county.sort_values("unique_frac", ascending=True)

    fig, ax = plt.subplots(figsize=preset.figsize_landscape)
    bars = ax.bar(
        by_county["maz"].astype(str),
        by_county["unique_frac"] * 100.0,
        color=preset.colors["accent"],
    )
    ax.set_ylabel("Unique draw fraction (%)")
    ax.set_xlabel("County (maz)")
    for bar, w in zip(bars, by_county["weight"].fillna(0.0).tolist(), strict=False):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 0.8,
            f"w={100*w:.1f}%",
            ha="center",
            va="bottom",
            fontsize=7,
            rotation=90,
        )
    _add_caption(
        fig,
        title=spec.title,
        subtitle="Lower bars indicate higher duplicate-share risk in sampled plans.",
        source=spec.source,
        takeaway=spec.takeaway,
    )
    fig.subplots_adjust(top=0.82, bottom=0.25)
    _save(fig, out)
    plt.close(fig)


def render_plot_09(
    out: Path,
    *,
    national_report: dict[str, Any],
    style_name: str,
    spec: FigureSpec,
) -> None:
    plt, _, _, preset = _mpl(style_name)
    by_county = pd.DataFrame(
        national_report.get("diagnostics_summary", {}).get("by_county", [])
    ).copy()
    if by_county.empty:
        raise ValueError("national report missing diagnostics_summary.by_county")
    by_county["n_draws"] = pd.to_numeric(by_county["n_draws"], errors="coerce")
    by_county["ensemble_n_unique_draws"] = pd.to_numeric(
        by_county["ensemble_n_unique_draws"], errors="coerce"
    )
    by_county["weight"] = pd.to_numeric(by_county["weight"], errors="coerce")
    by_county["dup_frac"] = (
        (by_county["n_draws"] - by_county["ensemble_n_unique_draws"]) / by_county["n_draws"]
    ).fillna(0.0)

    fig, ax = plt.subplots(figsize=preset.figsize_single)
    ax.scatter(
        by_county["weight"] * 100.0,
        by_county["dup_frac"] * 100.0,
        color=preset.colors["accent"],
        s=55,
        alpha=0.9,
    )
    ax.set_xlabel("County district weight (%)")
    ax.set_ylabel("Duplicate draw fraction (%)")
    top_dup = by_county.sort_values("dup_frac", ascending=False).head(3)["maz"].astype(str)
    labels = set(top_dup.tolist()) | {"01", "14"}
    for _, row in by_county.iterrows():
        m = str(row["maz"])
        if m in labels:
            ax.text(
                float(row["weight"]) * 100.0 + 0.12,
                float(row["dup_frac"]) * 100.0 + 0.12,
                m,
                fontsize=8,
            )
    _add_caption(
        fig,
        title=spec.title,
        subtitle="Identifies high-impact counties where draw diversity is weakest.",
        source=spec.source,
        takeaway=spec.takeaway,
    )
    fig.subplots_adjust(top=0.82, bottom=0.16)
    _save(fig, out)
    plt.close(fig)


def _build_specs(run_id: str) -> list[FigureSpec]:
    src = f"run_id={run_id}; national_report.json + county reports"
    return [
        FigureSpec(
            filename="01_national_weighted_focal_vs_ensemble.png",
            title="National Weighted Focal vs Ensemble",
            section="Executive Findings",
            source=src,
            takeaway="Enacted values diverge from neutral baseline on headline metrics.",
        ),
        FigureSpec(
            filename="02_county_percentile_heatmap.png",
            title="County Percentile Heatmap",
            section="Core Quantitative Results",
            source=src,
            takeaway="Some counties are consistently extreme under enacted boundaries.",
        ),
        FigureSpec(
            filename="03_seat_share_delta_lollipop_by_county.png",
            title="Seat Share Delta by County",
            section="Core Quantitative Results",
            source=src,
            takeaway="County-level seat effects show where enacted lines shift outcomes.",
        ),
        FigureSpec(
            filename="04_efficiency_gap_focal_vs_interval.png",
            title="Efficiency Gap: Enacted vs Ensemble Interval",
            section="Core Quantitative Results",
            source=src,
            takeaway="Out-of-band counties indicate structural vote-translation distortion.",
        ),
        FigureSpec(
            filename="05_selected_counties_seat_share_draw_histograms.png",
            title="Selected Counties: Seat Share Draw Distributions",
            section="Map-Based Evidence",
            source=src,
            takeaway="Enacted seat outcomes can sit at tails of plausible neutral draws.",
        ),
        FigureSpec(
            filename="06_selected_counties_effgap_draw_histograms.png",
            title="Selected Counties: Efficiency Gap Draw Distributions",
            section="Map-Based Evidence",
            source=src,
            takeaway="Efficiency-gap tails reinforce county-level structural skew.",
        ),
        FigureSpec(
            filename="07_pop_deviation_draw_histograms.png",
            title="Selected Counties: Population Deviation Diagnostics",
            section="Robustness",
            source=src,
            takeaway="Diagnostics show whether sampled plans remain within expected balance.",
        ),
        FigureSpec(
            filename="08_unique_draw_fraction_by_county.png",
            title="Unique Draw Fraction by County",
            section="Robustness",
            source=src,
            takeaway="Lower uniqueness flags counties needing higher draw budgets.",
        ),
        FigureSpec(
            filename="09_duplicate_draws_vs_weight_scatter.png",
            title="Duplicate Draws vs County Weight",
            section="Robustness",
            source=src,
            takeaway="High-weight + high-duplicate counties deserve elevated QA attention.",
        ),
    ]


def policy_figure_specs(run_id: str) -> list[FigureSpec]:
    """Public accessor for fixed figure spec manifest."""
    return list(_build_specs(run_id))


def write_figures_manifest(
    out_path: Path,
    *,
    run_id: str,
    style_name: str,
    specs: list[FigureSpec],
    focus_counties: list[str],
    n_draws_by_focus: dict[str, int],
) -> None:
    payload = {
        "schema_version": "hungary_ge.policy_figures/v1",
        "run_id": run_id,
        "style": style_name,
        "focus_counties": focus_counties,
        "n_draws_by_focus_county": n_draws_by_focus,
        "figures": [
            {
                "filename": s.filename,
                "title": s.title,
                "memo_section": s.section,
                "source": s.source,
                "takeaway": s.takeaway,
            }
            for s in specs
        ],
    }
    out_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=True) + "\n",
        encoding="utf-8",
    )


def generate_policy_figures(
    *,
    paths: ProcessedPaths,
    run_id: str,
    out_dir: Path,
    votes_parquet: Path,
    focal_parquet: Path,
    party_coding_path: Path | None,
    style_name: str = DEFAULT_STYLE,
    skip_draw_level: bool = False,
    no_progress: bool = False,
    log_prefix: str = "",
) -> list[Path]:
    if style_name not in STYLE_CHOICES:
        raise ValueError(f"style must be one of {STYLE_CHOICES!r}")
    if not votes_parquet.is_file():
        raise FileNotFoundError(f"missing votes parquet: {votes_parquet}")
    if not focal_parquet.is_file():
        raise FileNotFoundError(f"missing focal parquet: {focal_parquet}")
    if party_coding_path is not None and not party_coding_path.is_file():
        raise FileNotFoundError(f"missing party coding JSON: {party_coding_path}")

    out_dir.mkdir(parents=True, exist_ok=True)
    disable_outer = county_progress_disabled(no_progress=no_progress)
    if disable_outer:
        _phase_log(
            log_prefix,
            "policy_figures: tqdm disabled (--no-progress, TQDM_DISABLE=1, or stderr not a TTY); "
            "status lines on stdout.",
        )

    _phase_log(log_prefix, "policy_figures: reading national_report.json …")
    national = load_rollup(paths, run_id)
    maz_list = _county_code_list(national)
    _phase_log(
        log_prefix,
        f"policy_figures: loading {len(maz_list)} county report JSON pairs …",
    )
    county_part, county_diag = load_county_reports(
        paths, run_id, maz_list, no_progress=no_progress
    )
    _phase_log(log_prefix, "policy_figures: county reports loaded.")
    focus = select_focus_counties(national)
    specs = _build_specs(run_id)
    spec_by_file = {s.filename: s for s in specs}

    outputs: list[Path] = []
    static_steps = [
        (
            "01 national bars",
            lambda: render_plot_01(
                out_dir / "01_national_weighted_focal_vs_ensemble.png",
                national_report=national,
                style_name=style_name,
                spec=spec_by_file["01_national_weighted_focal_vs_ensemble.png"],
            ),
        ),
        (
            "02 percentile heatmap",
            lambda: render_plot_02(
                out_dir / "02_county_percentile_heatmap.png",
                national_report=national,
                style_name=style_name,
                spec=spec_by_file["02_county_percentile_heatmap.png"],
            ),
        ),
        (
            "03 seat delta lollipop",
            lambda: render_plot_03(
                out_dir / "03_seat_share_delta_lollipop_by_county.png",
                national_report=national,
                style_name=style_name,
                spec=spec_by_file["03_seat_share_delta_lollipop_by_county.png"],
            ),
        ),
        (
            "04 efficiency gap intervals",
            lambda: render_plot_04(
                out_dir / "04_efficiency_gap_focal_vs_interval.png",
                county_partisan=county_part,
                national_report=national,
                style_name=style_name,
                spec=spec_by_file["04_efficiency_gap_focal_vs_interval.png"],
            ),
        ),
    ]
    _phase_log(
        log_prefix,
        "policy_figures: rendering plots 01–04 (first matplotlib import can take ~30–60s) …",
    )
    for label, fn in tqdm(
        static_steps,
        desc="policy_figures render",
        unit="plot",
        file=sys.stderr,
        disable=disable_outer,
    ):
        _phase_log(log_prefix, f"policy_figures:   {label} …")
        fn()

    p1 = out_dir / "01_national_weighted_focal_vs_ensemble.png"
    outputs.append(p1)
    p2 = out_dir / "02_county_percentile_heatmap.png"
    outputs.append(p2)
    p3 = out_dir / "03_seat_share_delta_lollipop_by_county.png"
    outputs.append(p3)
    p4 = out_dir / "04_efficiency_gap_focal_vs_interval.png"
    outputs.append(p4)
    _phase_log(log_prefix, "policy_figures: plots 01–04 done.")

    n_draws_focus: dict[str, int] = {}
    if not skip_draw_level:
        seat_series: dict[str, CountyDrawSeries] = {}
        eg_series: dict[str, CountyDrawSeries] = {}
        draw_metrics: tuple[str, str] = ("seat_share_a", "efficiency_gap")
        _phase_log(
            log_prefix,
            f"policy_figures: draw-level metrics ({len(focus)} counties × one ensemble load + "
            f"one draw scan for {draw_metrics}; focus={focus}) …",
        )
        for maz in tqdm(
            focus,
            desc="policy_figures draw metrics",
            unit="county",
            file=sys.stderr,
            disable=disable_outer,
        ):
            ensemble, va, vb, focal_lbl = _load_ensemble_and_vote_arrays(
                paths=paths,
                run_id=run_id,
                maz=maz,
                votes_parquet=votes_parquet,
                focal_parquet=focal_parquet,
                party_coding_path=party_coding_path,
                log_prefix=log_prefix,
            )
            by_m = _draw_series_for_metrics(
                ensemble=ensemble,
                va=va,
                vb=vb,
                focal_lbl=focal_lbl,
                metric_names=draw_metrics,
                maz=maz,
                no_progress=no_progress,
                draw_pbar_desc=f"{maz} seat_share+effgap",
            )
            seat_series[maz] = by_m["seat_share_a"]
            eg_series[maz] = by_m["efficiency_gap"]
            n_draws_focus[maz] = by_m["seat_share_a"].n_draws
            _phase_log(
                log_prefix,
                f"policy_figures:   finished maz={maz} seat_share_a + efficiency_gap "
                f"({n_draws_focus[maz]} draws, one pass).",
            )

        p5 = out_dir / "05_selected_counties_seat_share_draw_histograms.png"
        render_plot_05(p5, seat_draws=seat_series, style_name=style_name, spec=spec_by_file[p5.name])
        outputs.append(p5)

        p6 = out_dir / "06_selected_counties_effgap_draw_histograms.png"
        render_plot_06(p6, eg_draws=eg_series, style_name=style_name, spec=spec_by_file[p6.name])
        outputs.append(p6)

    tail_steps = [
        (
            "07 pop deviation panels",
            lambda: render_plot_07(
                out_dir / "07_pop_deviation_draw_histograms.png",
                county_diagnostics=county_diag,
                focus_counties=focus,
                style_name=style_name,
                spec=spec_by_file["07_pop_deviation_draw_histograms.png"],
            ),
        ),
        (
            "08 unique draw fraction",
            lambda: render_plot_08(
                out_dir / "08_unique_draw_fraction_by_county.png",
                national_report=national,
                style_name=style_name,
                spec=spec_by_file["08_unique_draw_fraction_by_county.png"],
            ),
        ),
        (
            "09 duplicates vs weight",
            lambda: render_plot_09(
                out_dir / "09_duplicate_draws_vs_weight_scatter.png",
                national_report=national,
                style_name=style_name,
                spec=spec_by_file["09_duplicate_draws_vs_weight_scatter.png"],
            ),
        ),
    ]
    _phase_log(log_prefix, "policy_figures: rendering plots 07–09 …")
    for label, fn in tqdm(
        tail_steps,
        desc="policy_figures render",
        unit="plot",
        file=sys.stderr,
        disable=disable_outer,
    ):
        _phase_log(log_prefix, f"policy_figures:   {label} …")
        fn()

    p7 = out_dir / "07_pop_deviation_draw_histograms.png"
    outputs.append(p7)
    p8 = out_dir / "08_unique_draw_fraction_by_county.png"
    outputs.append(p8)
    p9 = out_dir / "09_duplicate_draws_vs_weight_scatter.png"
    outputs.append(p9)

    _phase_log(log_prefix, "policy_figures: writing figures_manifest.json …")
    manifest = out_dir / "figures_manifest.json"
    write_figures_manifest(
        manifest,
        run_id=run_id,
        style_name=style_name,
        specs=specs,
        focus_counties=focus,
        n_draws_by_focus=n_draws_focus,
    )
    outputs.append(manifest)
    return outputs

