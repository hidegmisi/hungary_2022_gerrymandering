# R `redist` SMC driver

Runs `redist::redist_smc` on a bundle produced by `hungary_ge.sampling.redist_export.export_redist_bundle` (`run.json`, `precincts.gpkg`, `edges.csv`), writes **long-format** `assignments.csv` (`unit_index`, `draw`, `chain`, `district`).

## Setup (renv)

From this directory (`r/redist/`):

```r
install.packages("renv", repos = "https://cloud.r-project.org")
renv::init(bare = TRUE)
renv::install("jsonlite")
renv::install("sf")
renv::install("redist")
renv::snapshot()
```

If `renv.lock` is present and matches your platform, prefer:

```r
install.packages("renv", repos = "https://cloud.r-project.org")
renv::restore()
```

Quick one-off (no renv): install `jsonlite`, `sf`, and `redist` from CRAN, then run the script (the `renv/activate.R` block in `run_smc.R` is skipped if `renv` was never initialized).

## Run

```sh
Rscript run_smc.R /path/to/run_directory
```

`run_directory` must contain `run.json` and the input files listed there. Logs are not written by R; the Python adapter captures `Rscript` stdout/stderr.

## Reproducibility

- Set `seed` in `run.json` (Python export does this when `SamplerConfig.seed` is set).
- For bitwise-stable SMC output, keep `ncores = 1` (default). Parallel `redist_smc` is not fully seed-stable across platforms.

## Population tolerance

See `docs/software-decisions.md`: `pop_tol` in `run.json` matches `redist_map`’s allowed deviation from average district population and is wired from `ElectorBalanceConstraint.max_relative_deviation` (default 0.15) on the Python side.
