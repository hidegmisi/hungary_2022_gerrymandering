# Reproducibility (Slice 10)

This note lists what you need on disk and which commands rebuild the **pilot** data products under `data/processed/`. Full national ensembles (R `redist`, ~10k draws) are optional and not required for CI.

## Environment

| Tool | Notes |
|------|------|
| Python | 3.12+ (see `.python-version`). Install deps with [`uv`](https://docs.astral.sh/uv/): `uv sync`. |
| Optional Folium maps | `uv sync --extra viz` for `scripts/map_adjacency.py`, `scripts/map_ensemble_draw.py`, and the pipeline `viz` stage. |
| Optional R | `Rscript` on `PATH` for `sample_plans(..., backend="redist")`. On Windows, use **User** `Path` (e.g. `C:\Program Files\R\R-*\bin\x64`) and/or Git Bash `~/.bashrc`; see project `.vscode/settings.json` if you use Cursor. Pin R packages when the sampling slice is finalized (`renv` / `DESCRIPTION` TBD). |

## Inputs checklist

1. **Raw precinct JSON:** `data/raw/szavkor_topo/{maz}/{maz}-{taz}.json` (settlement layout as documented in [`docs/data-model.md`](docs/data-model.md)).
2. **Optional void / hex layer:** county **shell** GeoJSON (or similar) if you use `--etl-with-gaps` / `--etl-void-hex` on the ETL script or pipeline.
3. **No secrets** in config; paths are local.

## Command sequence (happy path)

From the **repository root**, after `uv sync` and with raw data present:

```bash
uv run hungary-ge-pipeline
```

(or `uv run python -m hungary_ge.pipeline` — same entrypoint). Stage logic lives under `src/hungary_ge/pipeline/stages/` (`NAME`, `add_arguments`, `run(ctx)` per stage; see `stages/base.py`).

Stages (default): **etl** → **votes** → **graph**.

- **etl:** in-process `hungary_ge.pipeline.precinct_etl` (same flags as `scripts/build_precinct_layer.py`) → `data/processed/precincts.parquet` (+ default manifest `data/processed/manifests/<parquet-stem>_etl.json`, e.g. `precincts_etl.json`).
- **votes:** in-process `hungary_ge.pipeline.votes_etl` → `precinct_votes.parquet`, `focal_oevk_assignments.parquet` (+ default `manifests/<votes-stem>_etl.json`).
- **graph:** writes `data/processed/graph/adjacency_edges.parquet` and `adjacency_edges.meta.json` (queen contiguity by default). National scope uses **county-merge** adjacency and requires a **`maz`** column on the precinct layer.

After a successful run, the CLI also writes a **run provenance** file: `data/processed/manifests/run_<UTC-timestamp>.json` (argv, optional git HEAD, fingerprints for the precinct layer and adjacency Parquet when present).

```bash
uv run hungary-ge-pipeline --help
```

### Pipeline profiles (bundled flags)

- **`--pipeline-profile plain`** — `precincts.parquet`, queen (no fuzzy graph flags).
- **`--pipeline-profile void_hex_fuzzy_latest`** — void-hex ETL defaults (`--etl-with-gaps`, `--etl-shell data/raw/admin`, `--etl-void-hex`, `--etl-out-parquet data/processed/precincts_void_hex.parquet`), matching `--parquet`, plus `--graph-fuzzy --graph-fuzzy-buffering`.

### Optional Folium map (one county)

Requires `uv sync --extra viz`. Append the **viz** stage and pass county code:

```bash
uv run hungary-ge-pipeline --only etl votes graph viz --viz-maz 01
```

Fuzzy adjacency for graph + viz (or use `--pipeline-profile void_hex_fuzzy_latest` for ETL+graph defaults):

```bash
uv run hungary-ge-pipeline --graph-fuzzy --graph-fuzzy-buffering --only graph viz --viz-maz 01 --parquet data/processed/precincts_void_hex.parquet
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

   Folium maps after each county require `uv sync --extra viz`. To skip them: `--no-county-maps`. Per-county tqdm progress bars go to stderr; use `--no-progress` or set `TQDM_DISABLE=1` for CI or plain log capture. Unless `--no-progress`, `redist_smc` prints SMC split progress to stderr (mirrored live from R).

3. **`sample`** — R + `redist` per county (tune `--sample-n-draws`, `--sample-seed`, etc.):

   ```bash
   uv run python -m hungary_ge.pipeline --mode county --run-id "$RUN_ID" --only sample
   ```

4. **`reports`** — per-county `diagnostics.json` and `partisan_report.json`.
   - Default party coding is Fidesz–KDNP (`votes_list_952`) vs united opposition joint list (`votes_list_950`); override with `--reports-party-coding`.
   - Symmetric statewide vote balance is **on** by default for partisan metrics; disable with `--no-partisan-balance` or tune `--partisan-balance-eps-bloc`, `--partisan-balance-eps-total`, `--partisan-small-values` (same flags apply to `policy_figures`).

5. **`rollup`** — `data/processed/runs/<RUN_ID>/national_report.json` (use `--rollup-allow-partial` if some counties lack reports).

6. **`policy_figures`** — memo-ready PNG figures + `figures_manifest.json` under `data/processed/runs/<RUN_ID>/policy_figures/`.
   - Includes enacted-vs-ensemble comparisons and draw diagnostics for policy writeups.
   - Style presets: `--policy-figures-style memo-light` (screen) or `memo-print` (print contrast).
   - **Progress:** tqdm on stderr when it is a TTY (and not `--no-progress` / `TQDM_DISABLE`). If bars are off, the stage still prints **flushed stdout** status lines (national rollup, county JSON load, each plot, each draw-metric task). The longest silent stretch is usually **`load_plan_ensemble` reading a large county Parquet** before per-draw tqdm starts; the log line before that load states the file size.

Example **one-shot** chain after allocation (national mode is unchanged for `etl`/`votes`):

```bash
RUN_ID=pilot-2022-04
uv run python -m hungary_ge.pipeline --mode county --run-id "$RUN_ID" \
  --only graph sample reports rollup policy_figures
```

County artifacts live under `data/processed/runs/<RUN_ID>/counties/<maz>/` (`graph/`, `ensemble/`, `reports/`). **Caveat:** ensembles are drawn **within each county** with fixed district counts; they are not a single national coupled sampler over 106 districts.

Policy memo figure artifacts live under `data/processed/runs/<RUN_ID>/policy_figures/` and include:

- `01_national_weighted_focal_vs_ensemble.png`
- `02_county_percentile_heatmap.png`
- `03_seat_share_delta_lollipop_by_county.png`
- `04_efficiency_gap_focal_vs_interval.png`
- `05_selected_counties_seat_share_draw_histograms.png`
- `06_selected_counties_effgap_draw_histograms.png`
- `07_pop_deviation_draw_histograms.png`
- `08_unique_draw_fraction_by_county.png`
- `09_duplicate_draws_vs_weight_scatter.png`
- `figures_manifest.json`

### Ensemble plan preview (Folium)

Requires `uv sync --extra viz`, [`focal_oevk_assignments.parquet`](docs/data-model.md) under `data/processed/`, and county [`ensemble_assignments.parquet`](docs/data-model.md) (**long** layout). The script builds the same `OevkProblem` / `prepare_precinct_layer` path as county sampling, then draws **Enacted OEVK** (focal `oevk_id_full`) and one or more **simulated** layers (Parquet `draw` labels) as togglable choropleths.

```bash
uv run python scripts/map_ensemble_draw.py --repo-root . --run-id "$RUN_ID" --maz 01 --draw 1 \
  --out data/processed/runs/"$RUN_ID"/counties/01/ensemble/ensemble_map.html
```

Defaults: precinct GeoParquet (`precincts_void_hex.parquet` if present, else `precincts.parquet`), focal table `data/processed/focal_oevk_assignments.parquet`, `--pop-column voters` (match `--sample-pop-column`). Use `--draws 1,2,3` for multiple sim layers; `--no-enacted-layer` if focal is unavailable; `--ensemble-parquet` and `--ndists` / `--maz` when not resolving paths from `--run-id`.

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
uv run hungary-ge-pipeline --only graph
```

### Hex void ETL then national adjacency

Example (you must supply `--etl-shell`):

```bash
uv run hungary-ge-pipeline \
  --etl-with-gaps --etl-shell path/to/megye.geojson --etl-void-hex \
  --etl-out-parquet data/processed/precincts_void_hex.parquet \
  --parquet data/processed/precincts_void_hex.parquet
```

Or one switch after raw data is present:

```bash
uv run hungary-ge-pipeline --pipeline-profile void_hex_fuzzy_latest
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
