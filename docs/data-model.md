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

## Derived representations (future)

- **Adjacency graph:** Nodes = precincts; edges = pairs of precincts sharing a boundary (possibly with queen vs rook contiguity choice)
- **Assignment vector:** For each simulated plan, a mapping `precinct_id → oevk_id` (integer labels 1 … 106 or official OEVK codes)
- **Population or votes:** Tabular join on `precinct_id` (or finer units aggregated to precinct) for balance and outcome metrics

### Analysis concepts (for proposal-style clarity)

- **Dependent variable(s) (illustrative):** Under each map, outcomes such as party seat share in OEVKs, efficiency gap, or other ensemble-ranked statistics — exact DVs depend on party coding and research design.
- **Independent / design inputs:** Precinct attributes (votes, population), adjacency, and legal constraint parameters that define the simulation; the **focal** districting plan is then compared to the **ensemble distribution** of plans sharing those constraints.

## Outputs (future)

- Ensemble catalog: identifiers or hashes for each simulated plan, storage format TBD (e.g. parquet of assignments, compact binary, or references to GeoPackages)
- Summary tables: metric distributions across the ensemble and ranks/placements of focal plans

## Processed artifacts (canonical names)

Build outputs and derived layers use **fixed basenames** under **`data/processed/`** (large files are usually gitignored). The same names are exposed in code as `hungary_ge.config` / `hungary_ge.ProcessedPaths`.

| Basename | Role |
|----------|------|
| `precincts.parquet` | **Preferred** canonical precinct layer (GeoParquet); columns include `maz`, `taz`, `szk`, `precinct_id`, `geometry`. |
| `precincts.geojson` | Optional interchange / inspection copy of the same layer. |
| `precinct_votes.parquet` | Vote / population table keyed by `precinct_id`. |
| `ensemble_assignments.parquet` | Simulated plans: rows = precincts (same order as the canonical layer), columns = draws; column semantics TBD. |
| `focal_oevk.parquet` | Enacted or focal plan: `precinct_id → oevk_id` (Parquet narrow table or equivalent). JSON is acceptable for small mapping files if you document the schema. |

**Optional reproducibility:** the ETL script writes `data/processed/manifests/precincts_etl.json` by default (row counts, SHA-256 of the parquet output, CRS). Other manifests may use `data/processed/manifests/<build_id>.json` (stdlib JSON).

## Code layout and `data/processed/` artifacts

The [`src/hungary_ge/`](../src/hungary_ge/) package aligns pipeline code with this data model:

| Artifact | Path | Consumed by (module) |
|----------|------|----------------------|
| Canonical precinct layer | `data/processed/precincts.parquet` (preferred) or `precincts.geojson` | `hungary_ge.io.load_processed_geoparquet` / `load_processed_geojson` → `problem` + `graph` |
| Votes / population table | `data/processed/precinct_votes.parquet` | joined on `precinct_id` (`maz-taz-szk`) for `metrics` |
| Ensemble assignments | `data/processed/ensemble_assignments.parquet` | loaded into `hungary_ge.ensemble.PlanEnsemble` |
| Focal enacted plan | `data/processed/focal_oevk.parquet` | compared via `hungary_ge.metrics` |

See [methodology.md](methodology.md) **Code layout** and [`AGENTS.md`](../AGENTS.md) for the full ALARM-stage → submodule map. Sampling and metrics remain stubs until later slices.
