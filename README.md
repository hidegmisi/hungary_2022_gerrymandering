# Hungary 2022 gerrymandering — ensemble analysis

This project quantifies gerrymandering in Hungary by generating a large ensemble of legally plausible **OEVK** (Országgyűlési egyéni választókerület — single-member constituency) plans and comparing outcomes to baselines, following ideas from the Harvard **ALARM** (Algorithm-Assisted Redistricting Methodology) project: simulate many alternative plans under explicit rules and geography, then assess how extreme an enacted or proposed plan is relative to that distribution.

## Setup

- Install [uv](https://docs.astral.sh/uv/).
- Clone this repository and install dependencies:

```bash
uv sync
```

- Smoke test (editable package import):

```bash
uv run python -c "import hungary_ge; print('ok')"
```

### Lint and format

```bash
uv run ruff check src
uv run ruff format src
```

## Data

Precinct boundaries and identifiers are expected as **GeoJSON** (polygon features) with attributes for precinct, settlement, and county IDs. Place source files under `data/raw/`. Large GeoJSON and archives are ignored by Git by default (see root `.gitignore`); use Git LFS or external storage if you need them in version control.

Processed artifacts (graphs, cleaned tables) will go under `data/processed/`.

## Documentation

- [Contributor / agent guide (`AGENTS.md`)](AGENTS.md) — layout, tooling, **Conventional Commits**
- [Methodology (`docs/methodology.md`)](docs/methodology.md) — ensemble framing and ALARM alignment
- [Data model (`docs/data-model.md`)](docs/data-model.md) — expected inputs and future representations
- [References (`docs/references.md`)](docs/references.md) — ALARM, tools, and citation stubs

## Roadmap

- Document Hungarian OEVK redistricting rules and encode them in the sampler
- Confirm **data feasibility:** precinct GeoJSON, **precinct-level election results** joinable to those units, and reference geometry or labels for the **106** enacted OEVKs
- Simulate on the order of **10,000** alternative OEVK designs consistent with those rules
- Compare enacted (or focal) plans to the ensemble using **partisan outcome metrics** (e.g. seats–votes, **efficiency gap**, symmetry-style measures); treat **compactness** mainly as a constraint or secondary descriptor, not the primary fairness evidence

## References (quick links)

- [ALARM Project](https://alarm-redist.org/index.html)
- [50-State Redistricting Simulations — about / FAQ](https://alarm-redist.github.io/fifty-states/about/)

Public ALARM work often uses ensembles of thousands of plans per context (for example, 5,000 simulated plans in their U.S. state releases). R packages **redist** and **geomander** are common tooling choices for simulation-oriented workflows. This repo may stay Python-first for data I/O and analysis while calling **redist** / R for plan generation.
