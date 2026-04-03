# Reference run: `main`

This document is the **versioned recipe** for the county-first ensemble stored under `data/processed/runs/main/` (`national_report.json`, per-county ensembles and reports, `policy_figures/*.png`). That directory is **gitignored**; clone the repo, build inputs, then run these commands (or use [`scripts/run_main_analysis.sh`](../../scripts/run_main_analysis.sh)).

## Prerequisites

- **Python 3.12+**, [`uv`](https://docs.astral.sh/uv/), repo root as working directory.
- **Raw inputs** in git: `data/raw/szavkor_topo` (3177 JSON) and `data/raw/admin` (20 shells). See [`data/raw/README.md`](../../data/raw/README.md).
- **R + redist** for the `sample` stage: `Rscript` on `PATH`. Pin versions via [`r/redist/renv.lock`](../../r/redist/renv.lock) (see [R setup](#r-setup-redist) below).
- **Shell:** Bash (Git Bash or WSL on Windows). Use forward slashes in paths as below.

## National inputs (no national `graph` required)

Allocation only needs [`focal_oevk_assignments.parquet`](../data-model.md); county `graph` uses the void-hex GeoParquet you pass to `--parquet`. Run **ETL + votes** with the void-hex profile:

```bash
uv sync
uv run python -m hungary_ge.pipeline \
  --pipeline-profile void_hex_fuzzy_latest \
  --only etl votes
```

Produces (under `data/processed/`): `precincts_void_hex.parquet`, `precinct_votes.parquet`, `focal_oevk_assignments.parquet`, plus ETL manifests.

### Join votes onto the void-hex layer (required for `sample`)

The void-hex GeoParquet from ETL has **no** `voters` column. The SMC stage uses `--sample-pop-column voters` by default, so **`redist` needs a population column on the same file you pass to `--parquet`**. Build a joined layer:

```bash
uv run python scripts/join_votes_to_precinct_layer.py \
  --precinct-parquet data/processed/precincts_void_hex.parquet \
  --votes-parquet data/processed/precinct_votes.parquet \
  --out-parquet data/processed/precincts_void_hex_voters.parquet \
  --require-voters
```

Use `data/processed/precincts_void_hex_voters.parquet` for **graph**, **sample**, and (optionally) explicit `--parquet` on later stages so paths stay consistent. [`scripts/run_main_analysis.sh`](../../scripts/run_main_analysis.sh) runs this automatically after ETL + votes.

### ETL manifest caveat

The committed fingerprint [`data/processed/manifests/precincts_void_hex_etl.json`](../../data/processed/manifests/precincts_void_hex_etl.json) may list a historical `shell_path` (e.g. a single merged county shell file). **Canonical reproduction** uses **`--pipeline-profile void_hex_fuzzy_latest`**, which sets `--etl-shell data/raw/admin` (per-county `*.geojson`). Ignore the stale `shell_path` in that JSON when following this recipe.

## County ensemble + figures (`run_id=main`)

Party coding file (repo-relative):

```bash
PARTY_JSON=src/hungary_ge/metrics/data/partisan_party_coding.json
RUN_ID=main
PQ_V=data/processed/precincts_void_hex_voters.parquet
```

### 1. Allocation

```bash
uv run python -m hungary_ge.pipeline --mode county --run-id "$RUN_ID" --only allocation
```

### 2. Graph (per county, fuzzy 3 m, no Folium maps)

```bash
uv run python -m hungary_ge.pipeline --mode county --run-id "$RUN_ID" --only graph \
  --parquet "$PQ_V" \
  --graph-fuzzy --graph-fuzzy-buffering --graph-fuzzy-buffer-m 3 \
  --no-county-maps
```

### 3. Sample (R `redist` SMC, 1000 draws per county)

The reference `main` run did **not** pass `--sample-seed`; reruns follow the **same procedure** but will **not** reproduce identical `ensemble_assignments.parquet` bytes. For future pinned runs, add e.g. `--sample-seed 20220403` (and record the value).

```bash
uv run python -m hungary_ge.pipeline --mode county --run-id "$RUN_ID" --only sample \
  --parquet "$PQ_V" \
  --graph-fuzzy --graph-fuzzy-buffering --graph-fuzzy-buffer-m 3 \
  --sample-n-draws 1000 \
  --no-county-maps
```

Optional: append `--sample-seed <int>` for reproducible draws (see [`r/redist/README.md`](../../r/redist/README.md) on seed / cores).

### 4. Reports, rollup, policy figures

Partisan tables use `--reports-votes` / policy votes paths by default; pass the same **`--parquet`** as for graph/sample so any stage that resolves geometry stays aligned:

```bash
uv run python -m hungary_ge.pipeline \
  --mode county --run-id "$RUN_ID" \
  --only reports rollup policy_figures \
  --parquet "$PQ_V" \
  --reports-party-coding "$PARTY_JSON" \
  --policy-figures-party-coding "$PARTY_JSON"
```

Outputs include `data/processed/runs/main/national_report.json` and `data/processed/runs/main/policy_figures/` (see the repo [README](../../README.md) for pilot pipeline and tests).

## R setup (`redist`)

From [`r/redist/`](../../r/redist/) (see [`r/redist/README.md`](../../r/redist/README.md)):

```r
install.packages("renv", repos = "https://cloud.r-project.org")
renv::restore(lockfile = "renv.lock")
```

Run in R from the `r/redist` directory so the lockfile path resolves. The lockfile targets **R 4.4.2** with `redist` **4.3.2**, `sf` **1.0-19**, `jsonlite` **1.9.1** (adjust if your R version differs; use `renv` to reconcile).

## Related scripts

- [`scripts/run_main_analysis.sh`](../../scripts/run_main_analysis.sh) — automates this recipe.
- [`scripts/run_county_ensemble_hex_fuzzy.sh`](../../scripts/run_county_ensemble_hex_fuzzy.sh) — **different defaults** (e.g. 250 draws, expects a voters-enriched parquet by default, `--allow-disconnected-county-graph`, combined graph+sample+reports without rollup/`policy_figures`). Do not assume it matches `main` without editing.

## Frozen outputs (bit-identical figures)

CI does not upload ensemble artifacts. To share exact `main` outputs without resampling, publish a tarball (e.g. GitHub Release or Zenodo) of `data/processed/runs/main/` and document checksums (SHA-256, git tag, Python/R versions).
