# Data model

## Raw: `szavkor_topo` (precinct JSON)

The repository includes **`data/raw/szavkor_topo/`**: on the order of **3,177** **settlement-level JSON** files (one település file each), grouped by county, **not** standard GeoJSON.

### Layout on disk

- Path pattern: `data/raw/szavkor_topo/{maz}/{maz}-{taz}.json`
- **`maz`:** two-digit **county** (megye) code, `01` … `20` (top-level folders).
- **`taz`:** three-digit **settlement** code within the county (suffix in the filename after `{maz}-`).
- Each file’s **`list`** holds one row per **precinct** (szavazókör) in that settlement.

### JSON schema (per file)

| Field (root) | Meaning |
|--------------|--------|
| `header.generated` | ISO timestamp for the extract (observed: `2022-04-02T23:30:00`) |
| `header.vl_id`, `header.nvv_id` | Numeric IDs packaged with the extract (treat as provenance until mapped to official NVI metadata) |
| `list[]` | Array of precinct records |

| Field (each element of `list`) | Meaning |
|----------------------------------|--------|
| `maz` | County code (string, matches parent folder) |
| `taz` | Settlement code within county (string, zero-padded) |
| `szk` | Precinct index within the settlement (szavazókör; string, zero-padded) |
| `centrum` | Single point as `"<lat> <lon>"` (space-separated decimals) |
| `poligon` | Closed-ring outline as comma-separated `"<lat> <lon>"` vertices (same order as published; parse as WGS84) |
| `voters` | Optional: votes cast (or electorate count—project treats as **votes cast** when present); integer |
| `listVotes` | Optional: object mapping **string list registry ids** (e.g. `"952"`) → vote counts; **list votes only** for partisan metrics |
| `oevk_id` | Optional: enacted OEVK id **unique within** county `maz` (string or int in JSON) |
| `oevk_id_full` | Optional: enacted OEVK id **unique nationally** (string or int); canonical focal district key |
| `name`, `allowsReg`, … | Other fields may appear; electoral ETL reads the columns above when building Slice 4 artifacts |

**List → column mapping:** Party/list semantics are **not** embedded in geometry extracts. The repo ships an editable JSON map ([`src/hungary_ge/io/data/election_2022_list_map.json`](../src/hungary_ge/io/data/election_2022_list_map.json)): each `listVotes` key maps to a stable Parquet column (`votes_*`). Extend this file as official list metadata is confirmed; unmapped keys produce **warnings** (or errors in strict ETL mode).

**Precinct identifier:** Use a stable composite key, e.g. **`{maz}-{taz}-{szk}`**, for joins and graph nodes. Coordinates read as **EPSG:4326** (decimal degrees; latitude first in the source strings).

### Pipeline note

Downstream tools (GeoPandas, `shapely`, `redist`-style graphs) expect standard geometries. The implemented ETL parses `poligon` into rings and writes a national layer under `data/processed/` (see below).

### ETL and geometry repair (implemented)

The library builds a single precinct table from all settlement files via `hungary_ge.io.build_precinct_gdf`, or from the repo root:

```bash
uv run python scripts/build_precinct_layer.py
```

**Provenance:** `header.generated` and related header fields are NVI-style extract metadata; treat as provenance until mapped to official catalogues.

**Invalid / messy rings:** Vertex order follows the published `poligon` strings (comma-separated `"lat lon"` pairs, WGS84). Repair order is: `shapely.make_valid`, then `buffer(0)` if still invalid; rows that cannot be reduced to a non-empty `Polygon` or `MultiPolygon` are **dropped** and logged in the optional build manifest. Duplicate `precinct_id` values (should not occur) keep the first row.

**QA:** Compare `raw_precinct_list_total(szavkor_root)` (sum of `list` lengths) to the output row count plus dropped rows recorded in the manifest.

---

## Canonical analysis form: precinct layer (processed)

After conversion (or if you ingest third-party GeoJSON), the canonical artifact is a **GeoParquet** or **GeoJSON** layer whose features are **precinct polygons** (or multipolygons) with at least county, settlement, precinct, and **`precinct_id`** (`maz-taz-szk`).

**Empirical stack (checklist):** To run ensemble comparisons end-to-end you still need **votable results** at (or aggregable to) the same geographic level as these precincts, plus a **reference enacted OEVK map** (106 districts) to compare against simulated plans.

### Feature properties (conceptual)

| Concept        | Role |
|----------------|------|
| Precinct ID    | Smallest geographic unit for assignment to an OEVK; use composite `maz-taz-szk` when sourced from `szavkor_topo` |
| Settlement ID  | `maz` + `taz` (settlement within county) |
| County ID      | `maz` |

### Geometry

- Polygon or MultiPolygon per precinct
- CRS: document explicitly (WGS **EPSG:4326** if converted without reprojection from `szavkor_topo`); reprojection for area-balanced constraints should be explicit (e.g. metric CRS for Hungarian extent)

### Problem binding (column contract)

Before adjacency or simulation, tie the layer to an [`OevkProblem`](../src/hungary_ge/problem/oevk_problem.py) using [`prepare_precinct_layer`](../src/hungary_ge/problem/precinct_index_map.py) (or [`PrecinctIndexMap.from_frame`](../src/hungary_ge/problem/precinct_index_map.py) plus [`validate_problem_frame`](../src/hungary_ge/problem/precinct_index_map.py)).

| Requirement | Notes |
|-------------|--------|
| `precinct_id` (or `problem.precinct_id_column`) | Required; **unique**, non-null strings. **Canonical row order** is **lexicographic** by this column (stable sort). |
| Active geometry column | Required; not all-empty. |
| `maz` (or `problem.county_column`) | Required if `county_column` is set on the problem (default: `maz`). |
| Settlement column | Required only if `problem.settlement_column` is non-`None`. |
| `population` (or `problem.pop_column`) | Required only if `pop_column` is non-`None` (default name `population`; must be numeric). |
| CRS | If `problem.crs` is set (default `EPSG:4326`), the frame’s CRS must match; if `problem.crs` is `None`, CRS is not checked. |

[`PrecinctIndexMap`](../src/hungary_ge/problem/precinct_index_map.py) maps row index `i` ↔ `precinct_id` so [`PlanEnsemble`](../src/hungary_ge/ensemble/plan_ensemble.py) row order matches the sorted frame. Optional `OevkProblem.with_artifact` records paths/checksums for processed layers without loading files in the constructor.

### Adjacency (contiguity graph)

Build weights with [`build_adjacency`](../src/hungary_ge/graph/adjacency.py) after [`prepare_precinct_layer`](../src/hungary_ge/problem/precinct_index_map.py). Integer node indices **`0 … n-1`** match the sorted `GeoDataFrame` and [`PrecinctIndexMap`](../src/hungary_ge/problem/precinct_index_map.py).

| Mode | Rule |
|------|------|
| **Queen** | Two precincts are neighbors if their polygons share at least a **boundary segment or a vertex** (libpysal queen contiguity). |
| **Rook** | Neighbors share a **boundary segment** only (shared vertex alone does not count). |
| **Fuzzy** | Optional [`AdjacencyBuildOptions`](../src/hungary_ge/graph/adjacency_graph.py) flag ``fuzzy=True``: libpysal **fuzzy_contiguity** (predicate-based, default ``intersects``), orthogonal to queen/rook. **Without buffering**, weights use the layer’s CRS (often WGS84); small geometric inconsistencies there are less reliable. **With buffering** (`fuzzy_buffering=True`), a **copy** is reprojected to a **metric CRS** (default EPSG:32633 for Hungary) so buffer distances are in meters; tune `fuzzy_tolerance` or set `fuzzy_buffer_m` explicitly. Large buffers risk **spurious edges** (narrow alleys, slivers). Use queen/rook as a strict baseline for sensitivity checks; fuzzy + buffering helps **near-miss** gaps from digitizing or topology. |

**Persistence:** undirected edges `i < j` in `data/processed/graph/adjacency_edges.parquet` with metadata in `adjacency_edges.meta.json` (same basename). When saving after a fuzzy build, pass `build_options` to [`save_adjacency`](../src/hungary_ge/graph/adjacency_io.py) so the JSON records fuzzy parameters (`fuzzy_buffering`, `fuzzy_tolerance`, optional `fuzzy_buffer_m`, `fuzzy_metric_crs` when buffering). **Patches** (optional) use JSON: `{"add": [[i,j], ...], "remove": [[i,j], ...]}` in index space, applied via [`apply_adjacency_patch`](../src/hungary_ge/graph/adjacency_io.py).

**Visualization:** optional Folium HTML (`uv sync --extra viz`, then `scripts/map_adjacency.py`) draws centroid–centroid lines; use a **county filter** (`--maz`) or edge caps for national data. If the layer includes **`unit_kind=void`** (gap polygons), the map uses separate **FeatureGroups**—szvk in grey, void in orange with dashed outline—and **`LayerControl`** to toggle; pass **`--no-gaps`** to plot szvk polygons only.

**Default approach for fuzzy + hex maps (recommended):**
- Use the hex-void precinct layer (`precincts_void_hex.parquet`) with fuzzy buffering in meters:
  `--fuzzy --fuzzy-buffering --fuzzy-buffer-m 3`.
- For **national** maps, set `--max-features` above total row count (or disable truncation) so adjacency is built from the full layer. A low cap changes graph topology, not only rendering size.
- Avoid relying on tolerance-only buffering for national runs (`--fuzzy-buffering` without `--fuzzy-buffer-m`), because tolerance scales with overall extent and can inflate near-miss edges relative to county-only builds.

### Void (`gap`) units (shell minus szvk union)

NVI polygons do not tile all land (e.g. uninhabited belts around cities). Optional ETL adds **gap** rows so the contiguity graph can connect szvks that face each other across empty space.

- **Geometry:** `county_shell \ union(szvk in county)` in a **metric CRS** (default EPSG:32633), then polygon parts above **`min_area_m2`** (specks dropped).
- **Hex subdivision (optional):** Large void polygons can be **tessellated** into flat-top **hex cells** in metric CRS so void nodes are closer to szvk scale. Set **`GapBuildOptions.hex_void`** to a [`HexVoidOptions`](../src/hungary_ge/io/gaps_hex.py) with **`enabled=True`**, or use [`scripts/build_precinct_layer.py`](../scripts/build_precinct_layer.py) **`--void-hex`** (with **`--with-gaps`**). **Automatic cell area** defaults to **1.5 × mean szvk polygon area** in that county (via **`hex_area_factor`** default `1.5`, clamped by **`hex_min_cell_area_m2`** / **`hex_max_cell_area_m2`**). Override with **`hex_cell_area_m2`**. Only voids with area ≥ **`subdivide_min_void_m2`** (or ≥ mean szvk area × **`subdivide_min_void_factor`**) are split; smaller gaps stay a single void feature. **`max_cells_per_gap`** caps output per raw gap (manifest records **`n_hex_cells_truncated`** if hit). Hex cells are **not** official units; queen adjacency can show hairline gaps between cells—**fuzzy buffering** is often appropriate for the combined szvk+void layer.
- **Identifiers:** `precinct_id` pattern ``{void_id_prefix}-{maz}-{seq:04d}`` (default prefix `gap`). Columns **`maz`**, **`taz`**, **`szk`** use placeholders `000` on void rows so the schema stays join-friendly with szvk rows.
- **`unit_kind`:** `szvk` on normal rows (set at merge time); `void` on gap rows.
- **Population / votes:** No official population on voids; if a population column exists later, use **0** for void rows. **Do not** assign synthetic votes; exclude voids from partisan totals when aggregating (see Slice 4 in [master-plan.md](master-plan.md)).
- **Provenance:** Record the **shell** file path, SHA-256, and `GapBuildOptions` in the precinct ETL manifest when using [`scripts/build_precinct_layer.py`](../scripts/build_precinct_layer.py) with **`--with-gaps`** and **`--shell`**. Use an **official or openly licensed** megye / NUTS boundary source appropriate for your study; document the exact URL and license in your run README or appendix—the repo does not ship a national shell by default.

Implementation types: [`hungary_ge.io.gaps`](../src/hungary_ge/io/gaps.py) (**`GapShellSource`**, **`GapBuildOptions`** including **`hex_void`**, **`GapBuildStats`**, **`read_shell_gdf`**, **`build_gap_features_*`**, **`merge_szvk_and_gaps`**) and [`hungary_ge.io.gaps_hex`](../src/hungary_ge/io/gaps_hex.py) (**`HexVoidOptions`**, tessellation helpers).

**QA / diagnostics:** To quantify county shell vs precinct union overlap and explain large void coverage, run [`scripts/verify_void_gap_metrics.py`](../scripts/verify_void_gap_metrics.py) and see [void-gap-verification.md](void-gap-verification.md). If an admin export uses **wrong stems for NVI `maz` 11–16**, run [`scripts/fix_admin_shell_nvi_mapping.py`](../scripts/fix_admin_shell_nvi_mapping.py) once (see void-gap doc) before gap ETL.

### Electoral tables (Slice 4): `precinct_votes.parquet` and `focal_oevk_assignments.parquet`

Built from the same `szavkor_topo` walk as geometry ETL via [`hungary_ge.io.electoral_etl`](../src/hungary_ge/io/electoral_etl.py) and [`scripts/build_precinct_votes.py`](../scripts/build_precinct_votes.py).

**Scope:** **List votes only** (no invalid/blank ballot columns required). **`voters`** is documented as votes cast when present. **Census population** is out of scope for this artifact; use `voters` or another column as an optional weight when wiring [`OevkProblem`](../src/hungary_ge/problem/oevk_problem.py). **Void (`gap`) rows** are not in raw JSON—after merging gap polygons into the canonical layer, use [`join_electoral_to_gdf`](../src/hungary_ge/io/electoral_etl.py) so void rows do not receive imputed party votes (vote columns stay null).

#### `precinct_votes.parquet`

| Column | Type | Notes |
|--------|------|--------|
| `precinct_id` | string | **`maz-taz-szk`**; primary key; one row per szvk precinct |
| `maz`, `taz`, `szk` | string | Copy from JSON (QA / joins) |
| `voters` | int64 (nullable) | Optional; votes cast when present |
| `votes_*` | int64 | One column per **mapped** `listVotes` key (names from list map JSON) |
| `election_year` | int32 (nullable) | From list map file |
| `header_vl_id`, `header_nvv_id` | int64 (nullable) | Copied from each record’s settlement file `header` (provenance) |

All vote columns are **valid list totals** only.

#### `focal_oevk_assignments.parquet`

| Column | Type | Notes |
|--------|------|--------|
| `precinct_id` | string | Primary key; unique |
| `oevk_id_full` | string | **Canonical** enacted district id (national uniqueness); stored as string for stable joins |
| `oevk_id` | string (nullable) | County-scoped id when present |
| `maz` | string (nullable) | County code |

Derived from **`oevk_id_full`** (and related fields) on each precinct record in raw JSON—no OEVK boundary overlay in v1.

**Loaders:** [`load_votes_table`](../src/hungary_ge/io/electoral_etl.py), [`load_focal_assignments`](../src/hungary_ge/io/electoral_etl.py); validate focal uniqueness with [`assert_focal_assignments_valid`](../src/hungary_ge/io/electoral_etl.py).

## Derived representations (future)

- **Assignment vector:** For each simulated plan, a mapping `precinct_id → oevk_id` (integer labels 1 … 106 or official OEVK codes)
- **Population or votes:** Tabular join on `precinct_id` (or finer units aggregated to precinct) for balance and outcome metrics

### Analysis concepts (for proposal-style clarity)

- **Dependent variable(s) (illustrative):** Under each map, outcomes such as party seat share in OEVKs, efficiency gap, or other ensemble-ranked statistics — exact DVs depend on party coding and research design.
- **Independent / design inputs:** Precinct attributes (votes, population), adjacency, and legal constraint parameters that define the simulation; the **focal** districting plan is then compared to the **ensemble distribution** of plans sharing those constraints.

## Outputs (future)

- Summary tables: metric distributions across the ensemble and ranks/placements of focal plans

### Ensemble assignments (Slice 7)

Simulated draws are stored as **Parquet** plus a **JSON sidecar** (same pattern as graph `adjacency_edges.parquet` + `.meta.json`). [`PlanEnsemble`](../src/hungary_ge/ensemble/plan_ensemble.py) row order matches **lexicographic `precinct_id`** ([`PrecinctIndexMap`](../src/hungary_ge/problem/precinct_index_map.py)).

**Void / gap units:** Files include every graph node (szvk and `gap-…` voids). Partisan metrics and vote joins should **exclude voids** unless explicitly required (Electoral tables above; master-plan Slice 9).

#### Long layout (default)

One row per **(precinct, draw)**; recommended for large `n_draws` (~10k).

| Column | Type | Meaning |
|--------|------|---------|
| `precinct_id` | string | Unit id (`maz-taz-szk` or `gap-…`) |
| `draw` | int | Draw label (matches `PlanEnsemble.draw_ids` when set, else `1 … n_draws`) |
| `district` | int | District label for that unit in that draw |
| `chain` | int, optional | SMC chain / run id (matches `chain_or_run` when present) |

Row count = `n_units × n_draws`. Prefer **zstd** compression.

#### Wide layout (optional, small pilots)

Rows = units (`precinct_id` + one column per draw). Draw columns are named `d000001`, `d000002`, … in column order. Supported only for `n_draws` ≤ **1024** (library default); use long layout at national scale.

#### Sidecar manifest (`ensemble_assignments.meta.json`)

| Field | Meaning |
|-------|---------|
| `schema_version` | `hungary_ge.ensemble/v1` |
| `layout` | `long` or `wide` |
| `assignments_file` | Basename of the parquet file (relative to the sidecar directory) |
| `precinct_id_column` | Usually `precinct_id` |
| `n_units`, `n_draws` | Shape |
| `unit_ids` | Row order for `PlanEnsemble` (must match canonical sort) |
| `draw_ids` | Optional list of draw labels (length `n_draws`) |
| `chain_per_draw` | Optional list parallel to draws |
| `column_map` | Long layout: parquet column names for `draw` / `district` / `chain` |
| `wide_draw_columns` | Wide layout: list of `d000001`, … |
| `metadata` | Subset of `PlanEnsemble.metadata` (sampler seed, paths, …) |
| `sha256` | Optional checksum of the parquet file |

**Loaders:** [`save_plan_ensemble`](../src/hungary_ge/ensemble/persistence.py), [`load_plan_ensemble`](../src/hungary_ge/ensemble/persistence.py); lazy per-draw reads via [`load_plan_ensemble_draw_column`](../src/hungary_ge/ensemble/persistence.py).

**Map QA (optional):** [`scripts/map_ensemble_draw.py`](../scripts/map_ensemble_draw.py) with `uv sync --extra viz` builds a Folium HTML map of **focal** `oevk_id_full` and selected **simulated draws** on the county’s prepared precinct geometry (see [REPRODUCIBILITY.md](../REPRODUCIBILITY.md)).

#### Diagnostics JSON (Slice 8)

Optional **UTF-8 JSON** written next to the assignments Parquet (default basename `{parquet_stem}_diagnostics.json`, e.g. `ensemble_assignments_diagnostics.json` when the Parquet is `ensemble_assignments.parquet`). Produced by [`write_diagnostics_json`](../src/hungary_ge/diagnostics/report.py) or by passing ``diagnostics_report=`` into [`save_plan_ensemble`](../src/hungary_ge/ensemble/persistence.py). The ensemble sidecar manifest may list ``diagnostics_file`` with that basename.

| Field | Meaning |
|-------|---------|
| `schema_version` | `hungary_ge.diagnostics/v1` |
| `n_units`, `n_draws`, `ndists` | Shape and district count used for population parity |
| `population` | Ideal population per district, total, per-draw **max absolute relative deviation** vs equal split, optional ``draws_exceeding_pop_tol`` |
| `county_splits` | If ``county_ids`` were supplied: per-draw count of counties touching more than one district |
| `chains` | If multiple SMC ``chain`` ids: univariate **R-hat** (clamped at 1) on per-draw scalars — max population deviation and optionally split counts |
| `ensemble` | Counts of **unique** assignment columns vs duplicates |
| `smc_log` | Best-effort scan of ``redist_stderr_path`` / ``redist_stdout_path`` from ``PlanEnsemble.metadata`` (e.g. ESS keyword hits) |

**Void / gap units:** Pass a **full-length** ``populations`` vector (zeros on void units) into [`summarize_ensemble`](../src/hungary_ge/diagnostics/__init__.py) so district totals match the sampling graph; do not drop void columns before diagnostics if the ensemble includes them.

#### Partisan metrics (Slice 9)

[`focal_vs_ensemble_metrics`](../src/hungary_ge/metrics/compare.py) / [`partisan_metrics`](../src/hungary_ge/metrics/__init__.py) consume **`precinct_votes.parquet`**, **`focal_oevk_assignments.parquet`**, and a **`PlanEnsemble`**. There is **no** required new binary artifact for v1; results live in a [`PartisanComparisonReport`](../src/hungary_ge/metrics/report.py) (optional JSON via [`write_json`](../src/hungary_ge/metrics/report.py)).

**Party coding:** Editable JSON listing which `votes_*` columns sum into bloc **A** vs **B** (schema `hungary_ge.metrics.party_coding/v1`). Packaged example: [`src/hungary_ge/metrics/data/partisan_party_coding.json`](../src/hungary_ge/metrics/data/partisan_party_coding.json) defaults to Fidesz–KDNP (952) vs united opposition list (950). See [partisan-metrics.md](partisan-metrics.md).

**County `partisan_report.json` `extra`:** Besides focal alignment fields, reports include `partisan_report_metadata_schema`, `efficiency_gap_definition_id`, `metric_computation_policy` (balance/safety knobs), `party_coding_columns`, and `vote_balance` (applied statewide scaling metadata) so reruns and paper appendices can line up definitions across runs.

## Processed artifacts (canonical names)

Build outputs and derived layers use **fixed basenames** under **`data/processed/`** (large files are usually gitignored). The same names are exposed in code as `hungary_ge.config` / `hungary_ge.ProcessedPaths`.

| Basename | Role |
|----------|------|
| `precincts.parquet` | **Preferred** canonical precinct layer (GeoParquet); columns include `maz`, `taz`, `szk`, `precinct_id`, `geometry`. |
| `precincts.geojson` | Optional interchange / inspection copy of the same layer. |
| `precinct_votes.parquet` | Vote / population table keyed by `precinct_id`. |
| `ensemble_assignments.parquet` | Simulated district assignments (Slice 7). **Default layout: long** — see [Ensemble assignments (Slice 7)](#ensemble-assignments-slice-7). Sidecar `ensemble_assignments.meta.json`. Optional `ensemble_assignments_diagnostics.json` (Slice 8). |
| `focal_oevk_assignments.parquet` | Enacted plan: `precinct_id`, `oevk_id_full` (national id), optional `oevk_id`, `maz`. |
| `graph/adjacency_edges.parquet` (+ `.meta.json`) | Undirected contiguity edges (`i`,`j`) in `PrecinctIndexMap` index space; from [`save_adjacency`](../src/hungary_ge/graph/adjacency_io.py). |
| `ensemble_assignments.meta.json` | Sidecar to `ensemble_assignments.parquet` (manifest: layout, `unit_ids`, draw metadata). |

**Optional reproducibility:** the ETL script writes `data/processed/manifests/precincts_etl.json` by default (row counts, SHA-256 of the parquet output, CRS). Other manifests may use `data/processed/manifests/<build_id>.json` (stdlib JSON).

### Policy memo figure artifacts (pipeline `policy_figures` stage)

For county-mode runs with reports + rollup, the optional `policy_figures` stage writes publication-ready chart artifacts under:

- `data/processed/runs/<RUN_ID>/policy_figures/`

Outputs are fixed-name PNGs plus `figures_manifest.json` (schema `hungary_ge.policy_figures/v1`) that maps each figure to source files/metrics and suggested memo section. These figures are intended for enacted-vs-ensemble comparisons and draw diagnostics in policy-facing writeups.

## Code layout and `data/processed/` artifacts

The [`src/hungary_ge/`](../src/hungary_ge/) package aligns pipeline code with this data model:

| Artifact | Path | Consumed by (module) |
|----------|------|----------------------|
| Canonical precinct layer | `data/processed/precincts.parquet` (preferred) or `precincts.geojson` | `hungary_ge.io.load_processed_geoparquet` (GeoJSON via `geopandas.read_file` if needed) → `problem` + `graph` |
| Adjacency edges | `data/processed/graph/adjacency_edges.parquet` | `hungary_ge.graph.save_adjacency` / `load_adjacency` |
| Votes / population table | `data/processed/precinct_votes.parquet` | joined on `precinct_id` (`maz-taz-szk`) for `metrics` |
| Ensemble assignments | `data/processed/ensemble_assignments.parquet` (+ `.meta.json`, optional `…_diagnostics.json`) | [`save_plan_ensemble`](../src/hungary_ge/ensemble/persistence.py), [`load_plan_ensemble`](../src/hungary_ge/ensemble/persistence.py) → `PlanEnsemble`; [`summarize_ensemble`](../src/hungary_ge/diagnostics/__init__.py) |
| Focal enacted plan | `data/processed/focal_oevk_assignments.parquet` | [`focal_vs_ensemble_metrics`](../src/hungary_ge/metrics/compare.py) vs ensemble |

See [methodology.md](methodology.md) **Code layout** and [`AGENTS.md`](../AGENTS.md) for the full ALARM-stage → submodule map.
