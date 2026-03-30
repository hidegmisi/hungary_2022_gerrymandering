# Data model

## Inputs: precinct GeoJSON

Expected artifact: a **GeoJSON** `FeatureCollection` whose features are precinct polygons (or multipolygons).

**Empirical stack (checklist):** To run ensemble comparisons end-to-end you need precinct geometry **and** votable results at (or aggregable to) the same geographic level, plus a **reference enacted OEVK map** (106 districts) to compare against simulated plans. Missing precinct-level results or non-joinable IDs is a project risk; document sources and joins here as you lock them in.

### Feature properties (conceptual)

Each feature should carry identifiers usable for aggregation and legal constraints:

| Concept        | Role |
|----------------|------|
| Precinct ID    | Smallest geographic unit for assignment to an OEVK |
| Settlement ID  | Municipal or locality grouping (for rules tying plans to settlements) |
| County ID      | Megye-level grouping (for county-related constraints) |

**Exact attribute names** in your file (for example `precinct_id` vs Hungarian labels) will be recorded here once a sample file is available.

### Geometry

- Polygon or MultiPolygon per precinct
- Consistent CRS in the file or documented in sidecar metadata; reprojection for analysis should be explicit

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
