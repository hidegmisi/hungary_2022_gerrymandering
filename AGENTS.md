# Agent and contributor guide

This file helps humans and coding agents work consistently on the Hungary OEVK ensemble / gerrymandering project.

## Project intent

- **Goal:** Quantify districting patterns using **ensemble analysis** in the spirit of the Harvard **ALARM** project: simulate many legally plausible **OEVK** maps (target ~10,000 plans), then compare enacted or focal plans to that distribution.
- **Metrics:** Prefer **partisan outcome** measures (e.g. seats–votes behavior, **efficiency gap**, symmetry-style statistics). Do **not** treat **compactness** alone as primary evidence of fairness; it may be a **constraint** or secondary descriptive statistic. See [`docs/methodology.md`](docs/methodology.md).
- **Data:** Precinct **GeoJSON** plus joinable **precinct-level votes** and a **106**-district reference map are required for end-to-end analysis. Large raw geodata is usually **not** committed (see `.gitignore`). See [`docs/data-model.md`](docs/data-model.md).

## Repository layout

| Path | Use |
|------|-----|
| `src/hungary_ge/` | Installable Python package (primary code) |
| `data/raw/` | Includes `szavkor_topo/` (precinct JSON by settlement); other GeoJSON or downloads (large flat files often gitignored) |
| `data/processed/` | Derived tables, graphs, pipeline outputs |
| `docs/` | Methodology, data model, references |

### `hungary_ge` submodules (ALARM pipeline map)

Stages follow [`docs/alarm-methodology.md`](docs/alarm-methodology.md) (`redist_map` → adjacency → sampling → `redist_plans` → diagnostics / metrics). R name is for analogy only.

| Submodule | ALARM / `redist` role |
|-----------|------------------------|
| `hungary_ge.io` | Load raw settlement JSON and processed precinct GeoJSON (`data/processed/`) |
| `hungary_ge.problem` | Problem specification (`OevkProblem`; analogue of `redist_map` metadata) |
| `hungary_ge.graph` | Adjacency / contiguity from geometries |
| `hungary_ge.constraints` | Hungarian hard/soft constraints (placeholder) |
| `hungary_ge.sampling` | Ensemble draws (`sample_plans`; R `redist` or Python later) |
| `hungary_ge.ensemble` | Stored draws (`PlanEnsemble`; analogue of `redist_plans`) |
| `hungary_ge.diagnostics` | Ensemble diagnostics (ESS, chain summaries, etc.) |
| `hungary_ge.metrics` | Partisan outcome metrics vs focal plan |

Prefer **relative paths** in scripts and notebooks so runs are reproducible across machines.

## Tooling

- **Python:** 3.12+ (see `.python-version`).
- **Dependencies:** [uv](https://docs.astral.sh/uv/) — `uv sync`, `uv run …`. Optional **visualization** extras: `uv sync --extra viz` (Folium adjacency map script).
- **Verification (Slice 0):** after `uv sync`, confirm the geospatial stack and package import:
  `uv run python -c "import geopandas; import hungary_ge"`
- **Lint / format:** Ruff — `uv run ruff check src`, `uv run ruff format src`.
- **Git hooks:** [pre-commit](https://pre-commit.com/) — `uv run pre-commit install`, then `uv run pre-commit run --all-files` (config: `.pre-commit-config.yaml`).
- **R (optional later):** Ensemble sampling may use **redist** / **geomander**; document any R environment or scripts when added.

## Commits: Conventional Commits

Use **[Conventional Commits](https://www.conventionalcommits.org/)** for all commit messages:

- Format: `<type>[optional scope]: <description>`
- Common types: `feat`, `fix`, `docs`, `style`, `refactor`, `perf`, `test`, `chore`, `ci`
- Use the imperative mood in the subject line (“add graph loader”, not “added”).
- Breaking changes: add `!` after the type or a `BREAKING CHANGE:` footer when relevant.

Examples:

- `feat: add precinct adjacency builder from GeoJSON`
- `fix: correct county ID join in clean script`
- `docs: expand OEVK constraint notes`
- `chore: bump ruff in dev dependencies`

## Change discipline

- Keep diffs **focused** on the requested task; avoid unrelated refactors.
- Do not delete user-authored documentation without explicit direction.
- Do not commit secrets or huge binaries; use Git LFS or external storage if large assets must be tracked.

## Where to look first

- [`README.md`](README.md) — setup and roadmap
- [`docs/methodology.md`](docs/methodology.md) — ensemble framing and metric priorities
- [`docs/references.md`](docs/references.md) — ALARM, redist, literature stubs
