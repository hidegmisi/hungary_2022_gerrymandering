# Hungary 2022 gerrymandering — ensemble analysis

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

- **Raw precinct geometry:** `data/raw/szavkor_topo/` — settlement JSON files with **szavazókör** polygons and IDs (`maz` county, `taz` settlement, `szk` precinct). Not GeoJSON; see [`docs/data-model.md`](docs/data-model.md).
- **Processed:** canonical national precinct layer as **GeoParquet** (`data/processed/precincts.parquet`) plus optional GeoJSON; large outputs may be gitignored locally.

### ETL: build the precinct layer

From the repository root (after `uv sync`), with raw data present:

```bash
uv run python scripts/build_precinct_layer.py
```

This writes `data/processed/precincts.parquet` and, by default, `data/processed/manifests/precincts_etl.json` (counts, dropped rows, SHA-256 of the parquet). Use `--out-geojson path` for a GeoJSON copy. Geometry repair and provenance are documented in [`docs/data-model.md`](docs/data-model.md) (ETL subsection).

Other processed artifacts (graphs, votes tables, ensemble outputs) also go under `data/processed/` per the data model.

### Pilot pipeline (ETL → votes → graph)

Slice 10 bundles the default **processed-data** steps in one command (optional Folium **viz** stage needs `uv sync --extra viz`):

```bash
uv run python scripts/run_pilot_pipeline.py
```

Same runner as `uv run python -m hungary_ge.pipeline` or `uv run hungary-ge-pipeline`. Commands, inputs, graph-only runs, and **pytest** marker behavior are documented in [`REPRODUCIBILITY.md`](REPRODUCIBILITY.md).

### Adjacency map (optional)

After `precincts.parquet` exists, with `uv sync --extra viz`:

```bash
uv run python scripts/map_adjacency.py --maz 01 --out data/processed/graph/adjacency_map.html
```

Subsets by county (`maz`) and caps edges/features so the HTML stays usable. See [`docs/data-model.md`](docs/data-model.md) (adjacency subsection).

### Python package layout

The installable package [`src/hungary_ge/`](src/hungary_ge/) mirrors the ALARM simulation pipeline (problem spec → adjacency → sampling → plan ensemble → diagnostics and metrics). Submodule names and their match to `redist`-style stages are summarized in [`AGENTS.md`](AGENTS.md). Conceptual background: [`docs/alarm-methodology.md`](docs/alarm-methodology.md); artifact conventions: [`docs/data-model.md`](docs/data-model.md).

## Documentation

- [Reproducibility (`REPRODUCIBILITY.md`)](REPRODUCIBILITY.md) — pilot pipeline commands, inputs, optional R/tests
- [Contributor / agent guide (`AGENTS.md`)](AGENTS.md) — layout, tooling, **Conventional Commits**
- [Methodology (`docs/methodology.md`)](docs/methodology.md) — ensemble framing and ALARM alignment
- [Data model (`docs/data-model.md`)](docs/data-model.md) — expected inputs and future representations
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
