# Software decisions (ensemble sampling)

This note records why the first **plan-generation** backend is **R `redist`**, how it relates to the ALARM framing in [`alarm-methodology.md`](alarm-methodology.md), and practical caveats. It corresponds to **master-plan Slice 6** ([`master-plan.md`](master-plan.md)).

## Why `redist` first

- **ALARM alignment:** Public ALARM-style workflows are built around **`redist`**: SMC (`redist::redist_smc`) and supporting types (`redist_map`, `redist_plans`). Mirroring that stack keeps methodology and literature references aligned.
- **SMC maturity:** Sequential Monte Carlo for redistricting is well documented (McCartan & Imai 2023); reusing **`redist`** avoids re-implementing contiguity-aware splitting and population constraints in Python for v1.

## Interop (no `rpy2`)

The Python package calls **`Rscript`** as a subprocess and exchanges **files** (GeoPackage, edge list, JSON manifest, CSV assignments). That keeps dependencies small and avoids embedding an R interpreter in the same process.

## Risks

- **R on Windows / CI:** Installations differ; ensemble code paths should **fail loudly** when `Rscript` or `redist` is missing rather than silently falling back.
- **Reproducibility:** Use a **pinned R environment** under `r/redist/` (`renv`) and record **seeds** in run metadata. Note: `redist_smc(..., ncores > 1)` is not fully reproducible under `set.seed()`; prefer `ncores = 1` for strict replay.
- **National scale:** Full Hungary (many units) may be slow or memory-heavy; start with **pilots** (subset counties or tiny fixtures) before large production runs (Slice 7).

## Population tolerance vs project ±15%

- **`ElectorBalanceConstraint.max_relative_deviation`** defaults to **0.15** (±15% vs ideal electors per district); see [`constraint_spec.py`](../src/hungary_ge/constraints/constraint_spec.py).
- **`redist::redist_map(..., pop_tol = ...)`** uses the package meaning: allowed **fractional deviation from average district population** (symmetric band around parity). Mapping **`pop_tol = max_relative_deviation`** is the intended v1 wiring.
- **Spot-check:** After sampling, validate draws with **`check_plan`** (contiguity, elector balance, optional county rules). If future `redist` versions change semantics, adjust mapping or bounds here and in [`redist_export.py`](../src/hungary_ge/sampling/redist_export.py) / `run_smc.R`.

## Escape hatch

A future **Python-native** sampler can plug in behind the same **`SamplerConfig` / `SamplerResult`** façade and file-oriented contracts; **`backend="redist"`** remains explicit in [`sample_plans`](../src/hungary_ge/sampling/sample.py).
