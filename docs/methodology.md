# Methodology

## Ensemble gerrymandering analysis

**Goal:** Compare an enacted or candidate OEVK plan to a **distribution** of alternative plans that satisfy the same legal and procedural constraints. If observed partisan outcomes (or other metrics) under the focal plan lie in the tail of the distribution produced by neutral simulations, that supports a claim that the plan is unusually favorable to one side, conditional on the rules and geography encoded in the ensemble.

This is the same broad logic used in academic redistricting simulation work: the ensemble defines what is “typical” under stated constraints; individual plans are evaluated against that benchmark.

## Alignment with ALARM

The [ALARM Project](https://alarm-redist.org/index.html) (Algorithm-Assisted Redistricting Methodology) develops and applies simulation-based methods for legislative redistricting. A longer, implementation-focused summary of ALARM’s simulation workflow (the `redist` pipeline, SMC vs MCMC, constraints, and diagnostics) lives in [alarm-methodology.md](alarm-methodology.md). Their public releases emphasize:

- Large **ensembles** of plans sampled under **state-specific** rules and geographic data
- Comparison of enacted or proposed plans to the ensemble on **outcome metrics** (partisan balance, competitiveness, representation, etc.), not to abstract norms from other jurisdictions

This repository adopts that **ensemble comparison** principle for Hungarian OEVK districts and 2022-relevant inputs. Exact summary statistics will be specified once vote or population inputs and the target election are fixed (see **Metrics** below).

### Metrics: compactness versus partisan outcomes

**Compactness** (how “reasonable” district shapes look) is a common constraint in simulations and a common headline measure, but it is a weak proxy for democratic fairness on its own: compact districts can still be built to **crack** or **pack** opposition voters, and some less-compact shapes may be required to respect municipalities or other legitimate boundaries. For evaluating partisan distortion, the literature often emphasizes **partisan symmetry**-style summaries and **wasted-vote** measures such as the **efficiency gap**, which connect more directly to seat–vote translation than geometry alone.

Practical plan for this project:

- Use **compactness** mainly as a **sampling constraint** or secondary descriptive statistic, not as the main evidentiary metric.
- Prioritize ensemble comparisons on **seat shares, votes-to-seats relationships, and efficiency gap / symmetry-style metrics** under simulated versus enacted assignments.

## Hungary and 2022 context

- **OEVK:** National single-member districts for the Hungarian National Assembly. The number of OEVKs has been **106** under the post-2011 framework; confirm against your boundary and aggregation data.
- **Data:** Precinct geometries live in **`data/raw/szavkor_topo/`** (settlement JSON with `maz` / `taz` / `szk` and `poligon` strings). Convert to standard vector layers for building a **dual graph** (adjacency) and assigning units to OEVKs; see [`data-model.md`](data-model.md).
- **CRS:** Source coordinates are implicit **WGS84** (`lat lon` strings). Document any reprojection (e.g. to a metric CRS for population balance or area).

Placeholders until data and law text are attached:

- Official provenance link for the `szavkor_topo` extract (NVI or secondary compiler) and match to **2022** parliamentary election definitions
- Whether analysis targets the **2022** parliamentary election specifically or a broader comparability window

## Simulation target

The intended scale is on the order of **10,000** independent or quasi-independent OEVK designs, subject to rules you will specify. Typical constraint families (final list TBD):

- **Population balance** across OEVKs within legal tolerances
- **Contiguity** (and any stricter geographic requirements)
- **Administrative boundary** respect (counties, settlements), if required by law or by modeling choice
- Additional Hungarian statutory or jurisprudential rules

Until those rules are written down in this repo, the sampler is intentionally unimplemented.

## Software note

ALARM’s reference implementations often use the **redist** R package for MCMC / SMC sampling. This project may call R tooling, reimplement constraints in Python, or hybridize; that choice is deferred to implementation time.

## Code layout (`hungary_ge`)

The Python package under [`src/hungary_ge/`](../src/hungary_ge/) follows the same stages as [alarm-methodology.md](alarm-methodology.md): **I/O and problem spec** (`io`, `problem`) → **adjacency** (`graph`) → **constraints and sampling** (`constraints`, `sampling`) → **ensemble storage** (`ensemble`) → **diagnostics and partisan metrics** (`diagnostics`, `metrics`). Public types include `OevkProblem` (problem metadata), `PrecinctIndexMap` (stable precinct row order aligned with `PlanEnsemble`’s `unit_ids`), and `PlanEnsemble` (assignments matrix). Use `prepare_precinct_layer` to validate a precinct `GeoDataFrame` and fix lexicographic row order before graph or sampler code. Partisan summaries use [`focal_vs_ensemble_metrics`](../src/hungary_ge/metrics/compare.py) with configurable two-bloc vote columns ([partisan-metrics.md](partisan-metrics.md)). See [`AGENTS.md`](../AGENTS.md) for the submodule table.

Processed geometries and ensemble outputs should live under **`data/processed/`** (GeoJSON or GeoPackage for precincts; parquet or similar for assignment tables) as described in [`data-model.md`](data-model.md).

## Implementation roadmap

Phased slices, dependencies, suggested types (`PrecinctIndexMap`, `AdjacencyGraph`, `ConstraintSpec`), and a sub-plan template live in [`master-plan.md`](master-plan.md).
