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

Future: ensemble parquet, diagnostics JSON, partisan report JSON — see `docs/master-plan.md` (Slices 7–9).
