#!/usr/bin/env python3
"""Folium map: enacted OEVK (focal) and simulated ensemble draw(s) on precinct polygons.

Requires ``uv sync --extra viz``.

Example::

    uv sync --extra viz
    uv run python scripts/map_ensemble_draw.py \\
        --repo-root . --run-id MYRUN --maz 01 --draw 1 \\
        --out data/processed/runs/MYRUN/counties/01/ensemble/preview.html

Use ``--ensemble-parquet`` instead of ``--run-id`` when the assignments file
lives elsewhere; then pass ``--maz`` and ``--ndists`` unless they appear in the
ensemble ``.meta.json`` (``county_maz``, ``county_ndists``).
"""

from __future__ import annotations

import argparse
import hashlib
import html
import json
import sys
from pathlib import Path

try:
    import folium
except ImportError:
    print(  # noqa: T201
        "folium is required: uv sync --extra viz",
        file=sys.stderr,
    )
    raise SystemExit(1) from None

import geopandas as gpd
import numpy as np
import pandas as pd
from branca.element import Element
from folium.map import CustomPane

from hungary_ge.config import (
    COUNTY_PARTISAN_REPORT_JSON,
    ENSEMBLE_ASSIGNMENTS_PARQUET,
    ProcessedPaths,
)
from hungary_ge.ensemble.persistence import load_plan_ensemble_draw_column
from hungary_ge.io import (
    load_focal_assignments,
    load_processed_geoparquet,
    load_votes_table,
)
from hungary_ge.metrics.balance import apply_two_bloc_vote_balance
from hungary_ge.metrics.compare import metrics_for_assignment
from hungary_ge.metrics.party_coding import (
    default_partisan_party_coding_path,
    load_partisan_party_coding,
)
from hungary_ge.metrics.policy import DEFAULT_METRIC_COMPUTATION_POLICY
from hungary_ge.pipeline.county_allocation import normalize_maz
from hungary_ge.pipeline.county_sample import county_ndists_by_maz
from hungary_ge.problem import (
    DEFAULT_PRECINCT_ID_COLUMN,
    OevkProblem,
    prepare_precinct_layer,
)
from hungary_ge.viz.plan_assignments import (
    merge_enacted_districts,
    merge_simulated_districts,
)

ID_COL = DEFAULT_PRECINCT_ID_COLUMN


def _default_parquet_path(repo_root: Path) -> Path:
    void_hex = repo_root / "data/processed/precincts_void_hex.parquet"
    plain = repo_root / "data/processed/precincts.parquet"
    if void_hex.is_file():
        return void_hex
    return plain


def _default_county_borders_path(repo_root: Path) -> Path:
    return repo_root / "data/raw/admin/hu_megye_shell_maz.geojson"


def _county_border_gdf_for_map(
    path: Path,
    maz: str | None,
) -> gpd.GeoDataFrame | None:
    if not path.is_file():
        return None
    g = gpd.read_file(path)
    if maz is not None:
        if "maz" not in g.columns:
            print(  # noqa: T201
                f"County borders file has no 'maz' column; skipping: {path}",
                file=sys.stderr,
            )
            return None
        g = g[g["maz"].astype(str) == str(maz)].copy()
    if g.empty:
        return None
    if g.crs is not None:
        g = g.to_crs(4326)
    return g


def _load_manifest(ep_path: Path) -> dict | None:
    meta_path = ep_path.with_suffix(".meta.json")
    if not meta_path.is_file():
        return None
    return json.loads(meta_path.read_text(encoding="utf-8"))


def _manifest_unit_ids(meta: dict | None) -> tuple[str, ...]:
    if not meta:
        return ()
    top = tuple(str(x) for x in meta.get("unit_ids", []))
    if top:
        return top
    inner = dict(meta.get("metadata", {}))
    return tuple(str(x) for x in inner.get("unit_ids", []))


def _stable_fill_color(prefix: bytes, label: str) -> str:
    h = hashlib.md5(prefix + label.encode(), usedforsecurity=False).hexdigest()
    return f"#{h[:6]}"


def _style_void_neutral() -> dict:
    return {
        "fillColor": "#dddddd",
        "color": "#999999",
        "weight": 0.6,
        "fillOpacity": 0.2,
        "dashArray": "4 3",
    }


def _parse_draw_ids(draw: int | None, draws_csv: str | None) -> list[int]:
    out: list[int] = []
    if draw is not None:
        out.append(int(draw))
    if draws_csv:
        for part in draws_csv.split(","):
            p = part.strip()
            if p:
                out.append(int(p))
    if not out:
        msg = "pass --draw and/or --draws with at least one draw label"
        raise ValueError(msg)
    # preserve order, unique
    seen: set[int] = set()
    ordered: list[int] = []
    for d in out:
        if d not in seen:
            seen.add(d)
            ordered.append(d)
    return ordered


def _align_two_party_votes(
    *,
    votes: pd.DataFrame,
    unit_ids: tuple[str, ...],
    party_a_columns: tuple[str, ...],
    party_b_columns: tuple[str, ...],
) -> tuple[np.ndarray, np.ndarray]:
    dedup = votes.drop_duplicates(subset=[ID_COL]).copy()
    dedup["_pid"] = dedup[ID_COL].astype(str)
    by_pid = dedup.set_index("_pid", drop=False)
    va = np.zeros(len(unit_ids), dtype=np.float64)
    vb = np.zeros(len(unit_ids), dtype=np.float64)
    for i, pid in enumerate(unit_ids):
        sp = str(pid)
        if sp not in by_pid.index:
            continue
        row = by_pid.loc[sp]
        if isinstance(row, pd.DataFrame):
            row = row.iloc[0]
        a_sum = 0.0
        for col in party_a_columns:
            val = row.get(col)
            if val is not None and pd.notna(val):
                a_sum += float(val)
        b_sum = 0.0
        for col in party_b_columns:
            val = row.get(col)
            if val is not None and pd.notna(val):
                b_sum += float(val)
        va[i] = a_sum
        vb[i] = b_sum
    return va, vb


def _focal_labels_by_unit_id(
    *,
    focal_df: pd.DataFrame,
    unit_ids: tuple[str, ...],
) -> list[str | None]:
    if "oevk_id_full" not in focal_df.columns:
        return [None for _ in unit_ids]
    focal_u = focal_df.drop_duplicates(subset=[ID_COL]).copy()
    focal_u["_pid"] = focal_u[ID_COL].astype(str)
    s = focal_u.set_index("_pid", drop=False)["oevk_id_full"]
    labels: list[str | None] = []
    for pid in unit_ids:
        sp = str(pid)
        if sp not in s.index:
            labels.append(None)
            continue
        v = s.loc[sp]
        if pd.isna(v):
            labels.append(None)
        else:
            labels.append(str(v))
    return labels


def _load_county_partisan_report(
    *,
    paths: ProcessedPaths,
    run_id: str | None,
    maz: str,
) -> dict | None:
    if run_id is None:
        return None
    p = paths.county_reports_dir(run_id, maz) / COUNTY_PARTISAN_REPORT_JSON
    if not p.is_file():
        return None
    return json.loads(p.read_text(encoding="utf-8"))


def _fmt_share(v: float) -> str:
    return f"{100.0 * float(v):.1f}%"


def _fmt_pp(v: float) -> str:
    return f"{100.0 * float(v):+.1f} pp"


def _votes_by_precinct_in_gdf(
    *,
    gdf: gpd.GeoDataFrame,
    votes: pd.DataFrame,
    party_a_columns: tuple[str, ...],
    party_b_columns: tuple[str, ...],
) -> dict[str, tuple[float, float]]:
    """Two-party votes per precinct id for non-void rows in ``gdf``."""
    mask = gdf["unit_kind"].astype(str) != "void"
    pids = tuple(str(x) for x in gdf.loc[mask, ID_COL].astype(str).unique())
    if not pids:
        return {}
    va, vb = _align_two_party_votes(
        votes=votes,
        unit_ids=pids,
        party_a_columns=party_a_columns,
        party_b_columns=party_b_columns,
    )
    return {pids[i]: (float(va[i]), float(vb[i])) for i in range(len(pids))}


def _district_vote_totals(
    gdf_layer: gpd.GeoDataFrame,
    *,
    district_col: str,
    votes_by_pid: dict[str, tuple[float, float]],
) -> dict[str, dict[str, float | int | str]]:
    """Sum bloc votes and szvk count per district (string key)."""
    totals: dict[str, dict[str, float | int]] = {}
    for _, row in gdf_layer.iterrows():
        if str(row.get("unit_kind", "")) == "void":
            continue
        pid = str(row[ID_COL])
        dk = _district_key_for_row(row, district_col=district_col)
        va, vb = votes_by_pid.get(pid, (0.0, 0.0))
        if dk not in totals:
            totals[dk] = {"votes_a": 0.0, "votes_b": 0.0, "n_szvk": 0}
        totals[dk]["votes_a"] += float(va)
        totals[dk]["votes_b"] += float(vb)
        totals[dk]["n_szvk"] += 1

    out: dict[str, dict[str, float | int | str]] = {}
    for dk, t in totals.items():
        if district_col == "sim_district" and dk != "__missing__":
            dlab = dk
        elif dk == "__missing__":
            dlab = "missing"
        else:
            dlab = dk
        out[dk] = {
            "votes_a": float(t["votes_a"]),
            "votes_b": float(t["votes_b"]),
            "n_szvk": int(t["n_szvk"]),
            "district_label": dlab,
        }
    return out


def _district_key_for_row(row: pd.Series, *, district_col: str) -> str:
    raw_d = row.get(district_col)
    if raw_d is None or (isinstance(raw_d, float) and np.isnan(raw_d)):
        return "__missing__"
    if district_col == "sim_district":
        return str(int(float(raw_d)))
    return str(raw_d)


def _build_hge_popup_series(
    gdf_base: gpd.GeoDataFrame,
    layers: list[tuple[str, gpd.GeoDataFrame, str]],
    votes_by_pid: dict[str, tuple[float, float]],
) -> pd.Series:
    """JSON blob per row: per LayerControl name -> district totals + precinct votes."""
    layer_totals = [
        (
            layer_name,
            _district_vote_totals(gdf_l, district_col=col, votes_by_pid=votes_by_pid),
        )
        for layer_name, gdf_l, col in layers
    ]
    idx_maps: list[dict[str, str]] = []
    for _layer_name, gdf_l, col in layers:
        m: dict[str, str] = {}
        for _, row in gdf_l.iterrows():
            if str(row.get("unit_kind", "")) == "void":
                continue
            pid = str(row[ID_COL])
            m[pid] = _district_key_for_row(row, district_col=col)
        idx_maps.append(m)

    out_list: list[str | None] = []
    for _, row in gdf_base.iterrows():
        uk = str(row.get("unit_kind", ""))
        if uk == "void":
            out_list.append(None)
            continue
        pid = str(row[ID_COL])
        pa, pb = votes_by_pid.get(pid, (0.0, 0.0))
        blob: dict[str, dict[str, float | int | str]] = {}
        for i, (layer_name, totals) in enumerate(layer_totals):
            dk = idx_maps[i].get(pid, "__missing__")
            agg = totals.get(dk, totals.get("__missing__", {}))
            if not agg:
                blob[layer_name] = {
                    "district_label": "unknown",
                    "district_votes_a": 0,
                    "district_votes_b": 0,
                    "district_n_szvk": 0,
                    "precinct_votes_a": int(round(pa)),
                    "precinct_votes_b": int(round(pb)),
                }
            else:
                blob[layer_name] = {
                    "district_label": str(agg["district_label"]),
                    "district_votes_a": int(round(float(agg["votes_a"]))),
                    "district_votes_b": int(round(float(agg["votes_b"]))),
                    "district_n_szvk": int(agg["n_szvk"]),
                    "precinct_votes_a": int(round(pa)),
                    "precinct_votes_b": int(round(pb)),
                }
        out_list.append(json.dumps(blob, ensure_ascii=True))
    return pd.Series(out_list, index=gdf_base.index, dtype="object")


def _popup_control_javascript(
    *,
    map_name: str,
    district_layer_names: list[str],
    label_a: str,
    label_b: str,
) -> str:
    names_js = json.dumps(district_layer_names, ensure_ascii=True)
    return f"""
window.__hgeDistrictLayerNames = {names_js};
window.__hgeLabelA = {json.dumps(label_a, ensure_ascii=True)};
window.__hgeLabelB = {json.dumps(label_b, ensure_ascii=True)};
window.__hgeActiveLayerName = (window.__hgeDistrictLayerNames[0] || "");

window.addEventListener("load", function () {{
  var map = window[{json.dumps(map_name, ensure_ascii=True)}];
  if (!map || !window.__hgeDistrictLayerNames.length) return;

  function featureGroupHasPopupData(fg) {{
    var found = false;
    function walk(x) {{
      if (found) return;
      if (x.feature && x.feature.properties && x.feature.properties.hge_popup) {{
        found = true;
        return;
      }}
      if (x.eachLayer) x.eachLayer(walk);
    }}
    walk(fg);
    return found;
  }}

  function districtFeatureGroups() {{
    var out = [];
    map.eachLayer(function (ly) {{
      if (ly instanceof L.FeatureGroup && featureGroupHasPopupData(ly)) out.push(ly);
    }});
    return out;
  }}

  function pickActiveLayer() {{
    var fgs = districtFeatureGroups();
    for (var i = 0; i < fgs.length && i < window.__hgeDistrictLayerNames.length; i++) {{
      if (map.hasLayer(fgs[i])) {{
        window.__hgeActiveLayerName = window.__hgeDistrictLayerNames[i];
        return;
      }}
    }}
  }}

  map.on("overlayadd", function (e) {{
    if (window.__hgeDistrictLayerNames.indexOf(e.name) >= 0) {{
      window.__hgeActiveLayerName = e.name;
    }}
  }});
  map.on("overlayremove", function (e) {{
    if (e.name === window.__hgeActiveLayerName) pickActiveLayer();
  }});

  function formatPopup(props) {{
    var uk = props.unit_kind;
    if (uk === "void") return "<div style=\\"min-width:200px;\\">Void / gap</div>";
    var raw = props.hge_popup;
    if (!raw) return "<div>No vote data (missing votes file?)</div>";
    var data = typeof raw === "string" ? JSON.parse(raw) : raw;
    var key = window.__hgeActiveLayerName;
    var block = data[key];
    if (!block) {{
      return "<div style=\\"min-width:220px;\\">Choose a district layer in the layer control to see OEVK totals.</div>";
    }}
    var esc = function (s) {{
      return String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
    }};
    return (
      '<div style="min-width:260px;font-size:12px;line-height:1.35;">' +
      '<div style="font-weight:700;">' + esc(props.precinct_id) + "</div>" +
      '<div style="margin-top:6px;color:#444;">Layer: <b>' + esc(key) + "</b><br/>" +
      "District: <b>" + esc(block.district_label) + "</b> &middot; " +
      esc(block.district_n_szvk) + " szvk</div>" +
      '<hr style="margin:8px 0;border:none;border-top:1px solid #ccc;"/>' +
      '<div style="font-weight:600;margin-bottom:4px;">This precinct</div>' +
      "<div>" + esc(window.__hgeLabelA) + ": <b>" + block.precinct_votes_a.toLocaleString() + "</b></div>" +
      "<div>" + esc(window.__hgeLabelB) + ": <b>" + block.precinct_votes_b.toLocaleString() + "</b></div>" +
      '<div style="font-weight:600;margin:8px 0 4px 0;">District total (same plan)</div>' +
      "<div>" + esc(window.__hgeLabelA) + ": <b>" + block.district_votes_a.toLocaleString() + "</b></div>" +
      "<div>" + esc(window.__hgeLabelB) + ": <b>" + block.district_votes_b.toLocaleString() + "</b></div>" +
      "</div>"
    );
  }}

  function bindPopupsDeep(ly) {{
    if (ly instanceof L.FeatureGroup || ly instanceof L.GeoJSON) {{
      ly.eachLayer(function (child) {{ bindPopupsDeep(child); }});
    }} else if (ly.feature && ly.feature.properties) {{
      var p = ly.feature.properties;
      var isVoid = p.unit_kind === "void";
      var hasPid = p.precinct_id !== undefined && p.precinct_id !== null && p.precinct_id !== "";
      if (!isVoid && !hasPid) return;
      ly.bindPopup(function () {{ return formatPopup(ly.feature.properties); }}, {{ maxWidth: 340 }});
    }}
  }}

  map.eachLayer(function (ly) {{
    if (ly instanceof L.FeatureGroup && featureGroupHasPopupData(ly)) bindPopupsDeep(ly);
  }});

  pickActiveLayer();
}});
"""


def _build_metrics_legend_html(
    *,
    run_id: str | None,
    maz: str,
    label_a: str,
    label_b: str,
    layer_metrics: list[tuple[str, dict[str, float]]],
    county_partisan: dict | None,
) -> str:
    run_label = run_id if run_id is not None else "(custom ensemble)"
    rows: list[str] = []
    for layer_name, metrics in layer_metrics:
        rows.append(
            "<tr>"
            f"<td>{html.escape(layer_name)}</td>"
            f"<td>{_fmt_share(metrics['vote_share_a'])}</td>"
            f"<td>{_fmt_share(metrics['seat_share_a'])}</td>"
            f"<td>{_fmt_pp(metrics['efficiency_gap'])}</td>"
            f"<td>{_fmt_pp(metrics['mean_median_a_share_diff'])}</td>"
            "</tr>"
        )

    intervals_html = ""
    if county_partisan is not None:
        m = county_partisan.get("metrics", {})
        parts: list[str] = []
        for key, label, formatter in (
            ("vote_share_a", "Vote share A", _fmt_share),
            ("seat_share_a", "Seat share A", _fmt_share),
            ("efficiency_gap", "Efficiency gap", _fmt_pp),
            ("mean_median_a_share_diff", "Mean-median A", _fmt_pp),
        ):
            block = m.get(key, {})
            p05 = block.get("ensemble_p05")
            p95 = block.get("ensemble_p95")
            if p05 is None or p95 is None:
                continue
            parts.append(
                f"<li>{label}: {formatter(float(p05))} to {formatter(float(p95))}</li>"
            )
        if parts:
            intervals_html = (
                "<div style='margin-top:8px;'>"
                "<div style='font-weight:600;'>County ensemble p05-p95</div>"
                "<ul style='margin:6px 0 0 16px; padding:0;'>"
                + "".join(parts)
                + "</ul></div>"
            )

    return (
        '<div style="position: fixed; bottom: 12px; right: 12px; z-index: 9999; '
        "background: rgba(255,255,255,0.95); border: 1px solid #777; "
        "border-radius: 6px; padding: 10px; max-width: 460px; "
        'font-family: Arial, sans-serif; font-size: 12px; line-height: 1.35;">'
        f"<div style='font-size:13px; font-weight:700;'>Run {html.escape(run_label)} | maz {html.escape(maz)} metrics</div>"
        f"<div style='margin-top:4px; color:#333;'>A: {html.escape(label_a)}<br>B: {html.escape(label_b)}</div>"
        "<table style='margin-top:8px; border-collapse: collapse; width: 100%;'>"
        "<thead><tr>"
        "<th style='text-align:left; border-bottom:1px solid #bbb; padding:2px 4px;'>Layer</th>"
        "<th style='text-align:right; border-bottom:1px solid #bbb; padding:2px 4px;'>Vote A</th>"
        "<th style='text-align:right; border-bottom:1px solid #bbb; padding:2px 4px;'>Seat A</th>"
        "<th style='text-align:right; border-bottom:1px solid #bbb; padding:2px 4px;'>EG</th>"
        "<th style='text-align:right; border-bottom:1px solid #bbb; padding:2px 4px;'>Mean-Med</th>"
        "</tr></thead><tbody>"
        + "".join(rows)
        + "</tbody></table>"
        + intervals_html
        + "</div>"
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Folium choropleth: focal enacted OEVK + simulated draw(s).",
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=Path.cwd(),
        help="Repository root for default paths",
    )
    parser.add_argument(
        "--parquet",
        type=Path,
        default=None,
        help="Precinct GeoParquet (default: void_hex if present else precincts.parquet)",
    )
    parser.add_argument(
        "--run-id",
        type=str,
        default=None,
        help="Run id under data/processed/runs/<id>/ (with --maz for ensemble path)",
    )
    parser.add_argument(
        "--maz",
        type=str,
        default=None,
        help="Two-digit megye code; required unless inferable from ensemble manifest",
    )
    parser.add_argument(
        "--ensemble-parquet",
        type=Path,
        default=None,
        help="Path to ensemble_assignments.parquet (overrides --run-id county path)",
    )
    parser.add_argument(
        "--ndists",
        type=int,
        default=None,
        help="District count for OevkProblem when not using --run-id counts table",
    )
    parser.add_argument(
        "--draw",
        type=int,
        default=None,
        help="Single draw label (Parquet ``draw`` column)",
    )
    parser.add_argument(
        "--draws",
        type=str,
        default=None,
        help="Comma-separated draw labels (e.g. 1,2,5)",
    )
    parser.add_argument(
        "--pop-column",
        type=str,
        default="voters",
        help="Population column on precinct layer (match pipeline --sample-pop-column)",
    )
    parser.add_argument(
        "--focal-parquet",
        type=Path,
        default=None,
        help="focal_oevk_assignments.parquet (default: data/processed/...)",
    )
    parser.add_argument(
        "--no-enacted-layer",
        action="store_true",
        help="Do not add enacted focal choropleth (no focal file required)",
    )
    parser.add_argument(
        "--focal-allow-missing",
        action="store_true",
        help=(
            "Allow szvk rows with no focal row (e.g. missing oevk in raw JSON); "
            "style them like void gaps on the enacted layer"
        ),
    )
    parser.add_argument(
        "--no-county-borders",
        action="store_true",
        help="Do not draw megye outline",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=None,
        help="Output HTML (default: next to ensemble parquet as ensemble_map.html)",
    )
    parser.add_argument(
        "--metrics-votes-parquet",
        type=Path,
        default=Path("data/processed/precinct_votes.parquet"),
        help="Votes parquet for map metrics legend (default: data/processed/precinct_votes.parquet)",
    )
    parser.add_argument(
        "--metrics-party-coding",
        type=Path,
        default=None,
        help="Party coding JSON for map metrics legend (default: packaged config)",
    )
    parser.add_argument(
        "--no-metrics-legend",
        action="store_true",
        help="Disable metrics legend panel.",
    )
    parser.add_argument(
        "--no-popups",
        action="store_true",
        help="Disable szvk click popups (district vote totals per layer).",
    )
    args = parser.parse_args()

    repo_root = args.repo_root.resolve()
    paths = ProcessedPaths(repo_root)

    if args.parquet is None:
        pq = _default_parquet_path(repo_root)
    else:
        pq = args.parquet
        if not pq.is_absolute():
            pq = (repo_root / pq).resolve()
    if not pq.is_file():
        print(f"Missing precinct layer: {pq}", file=sys.stderr)  # noqa: T201
        return 1

    ensemble_parquet: Path
    if args.ensemble_parquet is not None:
        ensemble_parquet = args.ensemble_parquet.resolve()
        if not ensemble_parquet.is_absolute():
            ensemble_parquet = (repo_root / ensemble_parquet).resolve()
    else:
        if not args.run_id or args.maz is None:
            print(  # noqa: T201
                "Provide --ensemble-parquet or both --run-id and --maz.",
                file=sys.stderr,
            )
            return 1
        ensemble_parquet = (
            paths.county_ensemble_dir(args.run_id, args.maz)
            / ENSEMBLE_ASSIGNMENTS_PARQUET
        )
    if not ensemble_parquet.is_file():
        print(f"Missing ensemble parquet: {ensemble_parquet}", file=sys.stderr)  # noqa: T201
        return 1

    meta = _load_manifest(ensemble_parquet)
    meta_inner = dict(meta.get("metadata", {})) if meta else {}

    maz_n: str
    if args.maz is not None:
        maz_n = normalize_maz(args.maz)
    elif meta_inner.get("county_maz") is not None:
        maz_n = normalize_maz(str(meta_inner["county_maz"]))
    else:
        print(  # noqa: T201
            "Could not determine county: pass --maz or use ensemble manifest with county_maz.",
            file=sys.stderr,
        )
        return 1

    ndists: int | None = args.ndists
    if ndists is None and args.run_id:
        counts_path = paths.county_oevk_counts_parquet(args.run_id)
        if counts_path.is_file():
            nmap = county_ndists_by_maz(counts_path)
            ndists = nmap.get(maz_n)
    if ndists is None and meta_inner.get("county_ndists") is not None:
        ndists = int(meta_inner["county_ndists"])
    if ndists is None:
        print(  # noqa: T201
            "Could not determine ndists: pass --ndists or use --run-id with "
            "county_oevk_counts.parquet / manifest county_ndists.",
            file=sys.stderr,
        )
        return 1

    try:
        draw_labels = _parse_draw_ids(args.draw, args.draws)
    except ValueError as e:
        print(str(e), file=sys.stderr)  # noqa: T201
        return 1

    gdf = load_processed_geoparquet(pq)
    if "maz" not in gdf.columns:
        print("Precinct layer has no 'maz' column.", file=sys.stderr)  # noqa: T201
        return 1
    mzn = gdf["maz"].map(normalize_maz)
    county_gdf = gdf[mzn == maz_n].copy()
    if county_gdf.empty:
        print(f"No precinct rows for maz={maz_n!r}", file=sys.stderr)  # noqa: T201
        return 1

    if args.pop_column not in county_gdf.columns:
        print(  # noqa: T201
            f"Precinct layer has no population column {args.pop_column!r}.",
            file=sys.stderr,
        )
        return 1

    prob = OevkProblem(
        ndists=int(ndists),
        precinct_id_column=ID_COL,
        county_column=None,
        pop_column=args.pop_column,
        crs="EPSG:4326",
    )
    gdf2, _pmap = prepare_precinct_layer(county_gdf, prob)

    focal_path: Path | None = None
    if not args.no_enacted_layer:
        if args.focal_parquet is not None:
            focal_path = args.focal_parquet
            if not focal_path.is_absolute():
                focal_path = (repo_root / focal_path).resolve()
        else:
            focal_path = paths.focal_oevk_assignments_parquet.resolve()
        if not focal_path.is_file():
            print(  # noqa: T201
                f"Missing focal assignments (--focal-parquet or default): {focal_path}\n"
                "Use --no-enacted-layer to map simulated draws only.",
                file=sys.stderr,
            )
            return 1

    vp_meta = args.metrics_votes_parquet
    if not vp_meta.is_absolute():
        vp_meta = (repo_root / vp_meta).resolve()
    pcp_meta = args.metrics_party_coding
    if pcp_meta is not None and not pcp_meta.is_absolute():
        pcp_meta = (repo_root / pcp_meta).resolve()
    if pcp_meta is None:
        pcp_meta = default_partisan_party_coding_path()

    shared_coding = None
    shared_votes_tbl = None
    if vp_meta.is_file() and pcp_meta.is_file():
        shared_coding = load_partisan_party_coding(pcp_meta)
        shared_votes_tbl = load_votes_table(vp_meta)

    votes_by_pid: dict[str, tuple[float, float]] = {}
    popup_label_a = "Bloc A"
    popup_label_b = "Bloc B"
    if (
        not args.no_popups
        and shared_coding is not None
        and shared_votes_tbl is not None
    ):
        popup_label_a = shared_coding.label_a
        popup_label_b = shared_coding.label_b
        votes_by_pid = _votes_by_precinct_in_gdf(
            gdf=gdf2,
            votes=shared_votes_tbl,
            party_a_columns=shared_coding.party_a_columns,
            party_b_columns=shared_coding.party_b_columns,
        )

    out_path = args.out
    if out_path is None:
        out_path = ensemble_parquet.parent / "ensemble_map.html"
    elif not out_path.is_absolute():
        out_path = (repo_root / out_path).resolve()

    # Map center / bounds from prepared geometries
    b = gdf2.total_bounds
    center_lat = float((b[1] + b[3]) / 2)
    center_lon = float((b[0] + b[2]) / 2)

    m = folium.Map(
        location=(center_lat, center_lon), zoom_start=10, tiles="CartoDB positron"
    )
    CustomPane("countyMegyeBorders", z_index=650).add_to(m)

    if not args.no_county_borders:
        cpath = _default_county_borders_path(repo_root)
        c_gdf = _county_border_gdf_for_map(cpath, maz_n)
        if c_gdf is not None:
            folium.GeoJson(
                c_gdf.to_json(),
                style_function=lambda _f: {
                    "fillOpacity": 0,
                    "fillColor": "#000000",
                    "color": "#000000",
                    "weight": 5,
                    "opacity": 1.0,
                },
                pane="countyMegyeBorders",
            ).add_to(m)

    manifest_path = ensemble_parquet.with_suffix(".meta.json")
    layer_names: list[str] = []

    layer_payloads: list[tuple[str, gpd.GeoDataFrame, str, bool, object]] = []

    if focal_path is not None:
        focal_all = load_focal_assignments(focal_path)
        pids = set(gdf2[ID_COL].astype(str))
        focal_sub = focal_all[focal_all[ID_COL].astype(str).isin(pids)].copy()
        gdf_focal = merge_enacted_districts(
            gdf2,
            focal_sub,
            precinct_id_column=ID_COL,
            require_all_szvk=not args.focal_allow_missing,
        )

        def _style_enacted(feature: dict) -> dict:
            props = feature.get("properties") or {}
            uk = props.get("unit_kind")
            if uk is not None and str(uk) == "void":
                return _style_void_neutral()
            v = props.get("enacted_oevk_full")
            if v is None or (isinstance(v, float) and np.isnan(v)):
                return _style_void_neutral()
            fill = _stable_fill_color(b"enact:", str(v))
            return {
                "fillColor": fill,
                "color": "#222222",
                "weight": 0.35,
                "fillOpacity": 0.52,
            }

        layer_payloads.append(
            ("Enacted OEVK", gdf_focal, "enacted_oevk_full", True, _style_enacted),
        )
        layer_names.append("Enacted OEVK")

    metrics_rows: list[tuple[str, dict[str, float]]] = []
    county_partisan: dict | None = None
    coding_label_a = popup_label_a
    coding_label_b = popup_label_b
    va: np.ndarray | None = None
    vb: np.ndarray | None = None
    if not args.no_metrics_legend:
        if shared_coding is not None and shared_votes_tbl is not None:
            coding_label_a = shared_coding.label_a
            coding_label_b = shared_coding.label_b
            unit_ids_t_metrics = _manifest_unit_ids(meta)
            if not unit_ids_t_metrics:
                print(
                    "metrics legend: missing unit_ids in manifest; skipping.",
                    file=sys.stderr,
                )  # noqa: T201
            else:
                votes_sub = shared_votes_tbl[
                    shared_votes_tbl[ID_COL].astype(str).isin(set(unit_ids_t_metrics))
                ].copy()
                va_t, vb_t = _align_two_party_votes(
                    votes=votes_sub,
                    unit_ids=unit_ids_t_metrics,
                    party_a_columns=shared_coding.party_a_columns,
                    party_b_columns=shared_coding.party_b_columns,
                )
                va, vb, _vb_meta = apply_two_bloc_vote_balance(
                    va_t,
                    vb_t,
                    DEFAULT_METRIC_COMPUTATION_POLICY,
                )
                county_partisan = _load_county_partisan_report(
                    paths=paths,
                    run_id=args.run_id,
                    maz=maz_n,
                )
                if focal_path is not None:
                    focal_all_for_m = load_focal_assignments(focal_path)
                    focal_sub_for_m = focal_all_for_m[
                        focal_all_for_m[ID_COL]
                        .astype(str)
                        .isin(set(unit_ids_t_metrics))
                    ].copy()
                    focal_labels = _focal_labels_by_unit_id(
                        focal_df=focal_sub_for_m,
                        unit_ids=unit_ids_t_metrics,
                    )
                    idx = [i for i, lbl in enumerate(focal_labels) if lbl is not None]
                    if idx:
                        focal_dist = [focal_labels[i] for i in idx]
                        focal_m = metrics_for_assignment(
                            focal_dist,
                            va[np.array(idx, dtype=int)],
                            vb[np.array(idx, dtype=int)],
                            metric_policy=DEFAULT_METRIC_COMPUTATION_POLICY,
                            balance_already_applied=True,
                        )
                        metrics_rows.append(("Enacted OEVK", focal_m))
        else:
            print(  # noqa: T201
                f"metrics legend skipped: missing votes or coding file ({vp_meta}, {pcp_meta})",
                file=sys.stderr,
            )

    unit_ids_t = _manifest_unit_ids(meta)
    if not unit_ids_t:
        print("Ensemble manifest missing unit_ids.", file=sys.stderr)  # noqa: T201
        return 1

    for d_label in draw_labels:
        try:
            dist_col = load_plan_ensemble_draw_column(
                ensemble_parquet,
                int(d_label),
                manifest_path=manifest_path if manifest_path.is_file() else None,
            )
        except ValueError as e:
            print(f"draw {d_label}: {e}", file=sys.stderr)  # noqa: T201
            return 1
        gdf_d = merge_simulated_districts(
            gdf2,
            precinct_id_column=ID_COL,
            unit_ids=unit_ids_t,
            districts=dist_col,
        )

        def _make_style_sim() -> object:
            key = "sim_district"

            def _style_sim(feature: dict) -> dict:
                props = feature.get("properties") or {}
                uk = props.get("unit_kind")
                if uk is not None and str(uk) == "void":
                    return _style_void_neutral()
                raw = props.get(key)
                if raw is None:
                    return _style_void_neutral()
                label = str(int(float(raw)))
                fill = _stable_fill_color(b"sim:", label)
                return {
                    "fillColor": fill,
                    "color": "#333333",
                    "weight": 0.4,
                    "fillOpacity": 0.48,
                }

            return _style_sim

        sim_name = f"Simulated draw {d_label}"
        layer_payloads.append(
            (sim_name, gdf_d, "sim_district", False, _make_style_sim()),
        )
        layer_names.append(f"Draw {d_label}")
        if not args.no_metrics_legend and va is not None and vb is not None:
            d_metrics = metrics_for_assignment(
                dist_col.tolist(),
                va,
                vb,
                metric_policy=DEFAULT_METRIC_COMPUTATION_POLICY,
                balance_already_applied=True,
            )
            metrics_rows.append((sim_name, d_metrics))

    layers_spec = [(n, g, c) for n, g, c, _show, _sty in layer_payloads]
    district_js_names = [n for n, _g, _c, _show, _sty in layer_payloads]
    if votes_by_pid and layers_spec and not args.no_popups:
        popup_series = _build_hge_popup_series(gdf2, layers_spec, votes_by_pid)
        for _n, gdf_l, _c, _show, _sty in layer_payloads:
            gdf_l["hge_popup"] = popup_series.reindex(gdf_l.index)

    for name, gdf_l, _col, show_layer, style_fn in layer_payloads:
        fg = folium.FeatureGroup(name=name, show=show_layer)
        folium.GeoJson(
            gdf_l.to_json(),
            style_function=style_fn,
        ).add_to(fg)
        fg.add_to(m)

    if len(layer_names) > 1:
        folium.LayerControl(collapsed=False).add_to(m)

    if not args.no_popups and votes_by_pid and district_js_names:
        m.get_root().script.add_child(
            Element(
                _popup_control_javascript(
                    map_name=m.get_name(),
                    district_layer_names=district_js_names,
                    label_a=popup_label_a,
                    label_b=popup_label_b,
                )
            )
        )

    if not args.no_metrics_legend and metrics_rows:
        legend_html = _build_metrics_legend_html(
            run_id=args.run_id,
            maz=maz_n,
            label_a=coding_label_a,
            label_b=coding_label_b,
            layer_metrics=metrics_rows,
            county_partisan=county_partisan,
        )
        m.get_root().html.add_child(folium.Element(legend_html))

    m.fit_bounds([[b[1], b[0]], [b[3], b[2]]])
    out_path.parent.mkdir(parents=True, exist_ok=True)
    m.save(str(out_path))
    print(f"Wrote {out_path}")  # noqa: T201
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
