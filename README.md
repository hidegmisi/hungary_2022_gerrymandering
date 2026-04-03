# Hungary 2022 gerrymandering — ensemble analysis

## Reproduce the reference `main` analysis

**Minimum path** (from the **repository root**, Bash — Git Bash or WSL on Windows):

1. **Python 3.12+** and **[uv](https://docs.astral.sh/uv/)**: `uv sync`
2. **R** with **`redist`** on `PATH` (`Rscript`). Pin versions with [`r/redist/renv.lock`](r/redist/renv.lock) — see [`r/redist/README.md`](r/redist/README.md) (`renv::restore(lockfile = "renv.lock")` from `r/redist/`). On Windows, add R’s `bin\x64` to your user `PATH` if needed.
3. **Inputs**: this repo tracks **`data/raw/szavkor_topo`** and **`data/raw/admin`** (see [`data/raw/README.md`](data/raw/README.md)); no extra raw downloads for the void-hex stack.
4. Run:

```bash
bash scripts/run_main_analysis.sh
```

That script runs void-hex **ETL + votes**, builds **`precincts_void_hex_voters.parquet`** (joins votes so SMC has a non-missing **`voters`** column), then county mode **`run_id=main`**: allocation → fuzzy graphs → **1000** `redist` SMC draws per county → reports → rollup → **policy figures**. Outputs: **`data/processed/runs/main/national_report.json`**, **`data/processed/runs/main/policy_figures/`**. The full run is **slow** (many counties × 1000 draws).

Manual steps, `RUN_ID`, optional `--sample-seed`, and publishing frozen outputs: **[`docs/runs/main.md`](docs/runs/main.md)**.

---

This project quantifies gerrymandering in Hungary by generating a large ensemble of legally plausible **OEVK** (Országgyűlési egyéni választókerület — single-member constituency) plans and comparing outcomes to baselines, following ideas from the Harvard **ALARM** (Algorithm-Assisted Redistricting Methodology) project: simulate many alternative plans under explicit rules and geography, then assess how extreme an enacted or proposed plan is relative to that distribution.

## Setup

- Install [uv](https://docs.astral.sh/uv/).
- Clone this repository and install dependencies:

```bash
uv sync
```

Optional **Folium** map of adjacency (for exploration): `uv sync --extra viz`.

- Smoke test (GeoPandas + editable package import):

```bash
uv run python -c "import geopandas; import hungary_ge; print('ok')"
```

### Lint and format

```bash
uv run ruff check src tests scripts
uv run ruff format src tests scripts
```

## Data

- **Raw precinct geometry:** `data/raw/szavkor_topo/` — settlement JSON files with **szavazókör** polygons and IDs (`maz` county, `taz` settlement, `szk` precinct). Not GeoJSON; see [`docs/data-model.md`](docs/data-model.md). The repo tracks the full raw tree and `data/raw/admin` shells; details in [`data/raw/README.md`](data/raw/README.md).
- **Processed:** canonical national precinct layer as **GeoParquet** (`data/processed/precincts.parquet`) plus optional GeoJSON; large outputs (`*.parquet`, `data/processed/runs/`, …) are usually gitignored.

### ETL: build the precinct layer

From the repository root (after `uv sync`), with raw data present:

```bash
uv run python scripts/build_precinct_layer.py
```

This writes `data/processed/precincts.parquet` and, by default, `data/processed/manifests/<output-stem>_etl.json` (e.g. `precincts_etl.json`: counts, dropped rows, SHA-256 of the parquet). Use `--out-geojson path` for a GeoJSON copy. Geometry repair and provenance are documented in [`docs/data-model.md`](docs/data-model.md) (ETL subsection).

Other processed artifacts (graphs, votes tables, ensemble outputs) also go under `data/processed/` per the data model.

### Pilot pipeline (ETL → votes → graph)

Default Slice 10 command (optional Folium **viz** stage needs `uv sync --extra viz`):

```bash
uv run hungary-ge-pipeline
```

Same as `uv run python -m hungary_ge.pipeline`. Default stages: **etl** → **votes** → **graph** (writes `precincts.parquet`, vote/focal parquets, national `graph/adjacency_edges.parquet`). County allocation uses **`--only allocation --run-id …`**. Profiles: **`--pipeline-profile plain`** or **`--pipeline-profile void_hex_fuzzy_latest`** (void-hex ETL + fuzzy graph defaults). See **`--help`** and stage modules under `src/hungary_ge/pipeline/stages/`.

After a successful run, the CLI may write **`data/processed/manifests/run_<UTC>.json`** (argv, optional git HEAD, fingerprints when present).

### Adjacency map (optional)

After `precincts.parquet` exists, with `uv sync --extra viz`:

```bash
uv run python scripts/map_adjacency.py --maz 01 --out data/processed/graph/adjacency_map.html
```

Subsets by county (`maz`) and caps edges/features so the HTML stays usable. See [`docs/data-model.md`](docs/data-model.md) (adjacency subsection).

### County-first ensemble (general `RUN_ID`)

Per-county graphs, **`redist`** ensembles, diagnostics, **rollup** (`national_report.json`), and **`policy_figures`** live under `data/processed/runs/<RUN_ID>/`. Stages: **allocation** → **graph** → **sample** → **reports** → **rollup** → **policy_figures** (requires national **etl** + **votes** first). Example after allocation:

```bash
RUN_ID=pilot-2022-04
uv run python -m hungary_ge.pipeline --mode county --run-id "$RUN_ID" \
  --only graph sample reports rollup policy_figures
```

**Caveat:** ensembles are drawn **within each county** with fixed district counts, not one national 106-district coupled sampler. For the void-hex **`main`** recipe (voters join, fuzzy 3 m, 1000 draws), use **[`docs/runs/main.md`](docs/runs/main.md)** or **`scripts/run_main_analysis.sh`**.

### Ensemble map preview (Folium)

Requires `uv sync --extra viz`, focal + ensemble Parquet under the run. Example:

```bash
uv run python scripts/map_ensemble_draw.py --repo-root . --run-id "$RUN_ID" --maz 01 --draw 1 \
  --out data/processed/runs/"$RUN_ID"/counties/01/ensemble/ensemble_map.html
```

See [`docs/data-model.md`](docs/data-model.md).

### Python package layout

The installable package [`src/hungary_ge/`](src/hungary_ge/) mirrors the ALARM simulation pipeline (problem spec → adjacency → sampling → plan ensemble → diagnostics and metrics). Submodule names and their match to `redist`-style stages are summarized in [`AGENTS.md`](AGENTS.md). Conceptual background: [`docs/alarm-methodology.md`](docs/alarm-methodology.md); artifact conventions: [`docs/data-model.md`](docs/data-model.md).

## Tests

Default **`uv run pytest`** skips tests marked **`requires_r`** (R + redist), **`requires_data`**, and **`heavy`** (see `pyproject.toml`).

```bash
uv run pytest
```

All markers (needs `Rscript` + working **redist** for R tests):

```bash
uv run pytest --override-ini addopts=
```

R-only:

```bash
uv run pytest -m requires_r --override-ini addopts=
```

## Documentation

- [Reference run `main` (`docs/runs/main.md`)](docs/runs/main.md) — void-hex county ensemble + memo figures (manual commands)
- [Contributor / agent guide (`AGENTS.md`)](AGENTS.md) — layout, tooling, **Conventional Commits**
- [Methodology (`docs/methodology.md`)](docs/methodology.md) — ensemble framing and ALARM alignment
- [Data model (`docs/data-model.md`)](docs/data-model.md) — expected inputs and representations
- [References (`docs/references.md`)](docs/references.md) — ALARM, tools, and citation stubs

## Roadmap

- Document Hungarian OEVK redistricting rules and encode them in the sampler
- Confirm **data feasibility:** precinct geometries (see `szavkor_topo`), **precinct-level election results** joinable via `maz` / `taz` / `szk` (or your composite key), and reference geometry or labels for the **106** enacted OEVKs
- Simulate on the order of **10,000** alternative OEVK designs consistent with those rules
- Compare enacted (or focal) plans to the ensemble using **partisan outcome metrics** (e.g. seats–votes, **efficiency gap**, symmetry-style measures); treat **compactness** mainly as a constraint or secondary descriptor, not the primary fairness evidence

## References (quick links)

- [ALARM Project](https://alarm-redist.org/index.html)
- [50-State Redistricting Simulations — about / FAQ](https://alarm-redist.github.io/fifty-states/about/)

Public ALARM work often uses ensembles of thousands of plans per context (for example, 5,000 simulated plans in their U.S. state releases). R packages **redist** and **geomander** are common tooling choices for simulation-oriented workflows. This repo may stay Python-first for data I/O and analysis while calling **redist** / R for plan generation.
