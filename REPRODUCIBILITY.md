# Reproducibility (Slice 10)

This note lists what you need on disk and which commands rebuild the **pilot** data products under `data/processed/`. Full national ensembles (R `redist`, ~10k draws) are optional and not required for CI.

## Environment

| Tool | Notes |
|------|------|
| Python | 3.12+ (see `.python-version`). Install deps with [`uv`](https://docs.astral.sh/uv/): `uv sync`. |
| Optional Folium maps | `uv sync --extra viz` for `scripts/map_adjacency.py` and the pipeline `viz` stage. |
| Optional R | `Rscript` on `PATH` for `sample_plans(..., backend="redist")`. On Windows, use **User** `Path` (e.g. `C:\Program Files\R\R-*\bin\x64`) and/or Git Bash `~/.bashrc`; see project `.vscode/settings.json` if you use Cursor. Pin R packages when the sampling slice is finalized (`renv` / `DESCRIPTION` TBD). |

## Inputs checklist

1. **Raw precinct JSON:** `data/raw/szavkor_topo/{maz}/{maz}-{taz}.json` (settlement layout as documented in [`docs/data-model.md`](docs/data-model.md)).
2. **Optional void / hex layer:** county **shell** GeoJSON (or similar) if you use `--etl-with-gaps` / `--etl-void-hex` on the ETL script or pipeline.
3. **No secrets** in config; paths are local.

## Command sequence (happy path)

From the **repository root**, after `uv sync` and with raw data present:

```bash
uv run python scripts/run_pilot_pipeline.py
```

Stages (default): **etl** → **votes** → **graph**.

- **etl:** `scripts/build_precinct_layer.py` → `data/processed/precincts.parquet` (+ default manifest under `data/processed/manifests/`).
- **votes:** `scripts/build_precinct_votes.py` → `precinct_votes.parquet`, `focal_oevk_assignments.parquet`.
- **graph:** writes `data/processed/graph/adjacency_edges.parquet` and `adjacency_edges.meta.json` (queen contiguity by default).

Equivalent module invocation:

```bash
uv run python -m hungary_ge.pipeline
```

Console entry (after `uv sync`):

```bash
uv run hungary-ge-pipeline --help
```

### Optional Folium map (one county)

Requires `uv sync --extra viz`. Append the **viz** stage and pass county code:

```bash
uv run python scripts/run_pilot_pipeline.py --only etl votes graph viz --viz-maz 01
```

Fuzzy adjacency for graph + viz:

```bash
uv run python scripts/run_pilot_pipeline.py --graph-fuzzy --graph-fuzzy-buffering --only graph viz --viz-maz 01 --parquet data/processed/precincts_void_hex.parquet
```

(Adjust `--parquet` if your void/hex layer lives elsewhere.)

## County-first ensemble (optional)

Use this path when you want **per-county** graphs, `redist` ensembles, diagnostics, and a **national rollup** under one run id. Prerequisite: national **`etl`** and **`votes`** (and `focal_oevk_assignments.parquet`) must already exist; the county driver reads `data/processed/precinct_votes.parquet` for reports and the precinct layer for sampling.

Pick a **`RUN_ID`** (folder name under `data/processed/runs/`).

1. **Allocation** — derive `county_oevk_counts.parquet` from focal assignments for every megye in the runs folder:

   ```bash
   uv run python -m hungary_ge.pipeline --only allocation --run-id "$RUN_ID"
   ```

2. **Graph** (all counties, or add `--maz 01` for one megye):

   ```bash
   uv run python -m hungary_ge.pipeline --mode county --run-id "$RUN_ID" --only graph
   ```

   Folium maps after each county require `uv sync --extra viz`. To skip them: `--no-county-maps`.

3. **`sample`** — R + `redist` per county (tune `--sample-n-draws`, `--sample-seed`, etc.):

   ```bash
   uv run python -m hungary_ge.pipeline --mode county --run-id "$RUN_ID" --only sample
   ```

4. **`reports`** — per-county `diagnostics.json` and `partisan_report.json`.

5. **`rollup`** — `data/processed/runs/<RUN_ID>/national_report.json` (use `--rollup-allow-partial` if some counties lack reports).

Example **one-shot** chain after allocation (national mode is unchanged for `etl`/`votes`):

```bash
RUN_ID=pilot-2022-04
uv run python -m hungary_ge.pipeline --mode county --run-id "$RUN_ID" \
  --only graph sample reports rollup
```

County artifacts live under `data/processed/runs/<RUN_ID>/counties/<maz>/` (`graph/`, `ensemble/`, `reports/`). **Caveat:** ensembles are drawn **within each county** with fixed district counts; they are not a single national coupled sampler over 106 districts.

### Default national fuzzy+hex map settings

For national maps on the hex-void layer, use a **fixed meter buffer** and avoid feature truncation:

```bash
uv run python scripts/map_adjacency.py \
  --parquet data/processed/precincts_void_hex.parquet \
  --fuzzy --fuzzy-buffering --fuzzy-buffer-m 3 \
  --max-features 30000 --max-edges 300000 \
  --out data/processed/graph/adjacency_map_national_fuzzy_hex_fixed3m_full.html
```

Rationale: this keeps fuzzy behavior stable between county and national runs. Small `--max-features` values alter adjacency (not just rendering) because the graph is built after row truncation.

### Graph-only (skip ETL)

If `precincts.parquet` already exists:

```bash
uv run python scripts/run_pilot_pipeline.py --only graph
```

### Hex void ETL then national adjacency

Example (you must supply `--etl-shell`):

```bash
uv run python scripts/run_pilot_pipeline.py \
  --etl-with-gaps --etl-shell path/to/megye.geojson --etl-void-hex \
  --etl-out-parquet data/processed/precincts_void_hex.parquet \
  --parquet data/processed/precincts_void_hex.parquet
```

## Tests

Default **CI / local** run skips tests marked `requires_r` (R + redist integration).

```bash
uv run pytest
```

Run **all** tests, including R integration (needs `Rscript` and a working `redist`):

```bash
uv run pytest --override-ini addopts=
```

Or select only R tests:

```bash
uv run pytest -m requires_r --override-ini addopts=
```

## Outputs (reference)

Artifact names are defined in [`docs/data-model.md`](docs/data-model.md) and [`src/hungary_ge/config.py`](src/hungary_ge/config.py). The pilot pipeline writes at least:

- `data/processed/precincts.parquet` (ETL)
- `data/processed/precinct_votes.parquet`, `data/processed/focal_oevk_assignments.parquet` (votes)
- `data/processed/graph/adjacency_edges.parquet` + sidecar metadata (graph)

County runs add per-county **`ensemble/ensemble_assignments.parquet`**, **`reports/`** JSON, and **`national_report.json`** at the run root. See `docs/master-plan.md` (Slice 10).
