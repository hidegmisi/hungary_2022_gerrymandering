# Assessing Partisan Bias in Hungary's 2022 Single Member District System

Mihaly Hideg  
Policy with Data Final Project  
March 2026

## Executive findings

This project tests whether the enacted district structure behaves like a typical map under explicit redistricting constraints. The current completed run is county level for Baranya (`maz` `02`) and should be read as a pilot result, not as a national conclusion.

Key empirical findings from the completed run:

- The enacted map returns bloc A seat share `0.50` in Baranya, while the ensemble mean is `0.546`.
- The enacted map's efficiency gap is `0.0145`, while the ensemble mean is `-0.0283`.
- Mean minus median bloc A district vote share is `0.0051` in the enacted map, compared to an ensemble mean of `-0.0043`.
- Districting changes seat outcomes while county level vote share is fixed by construction. Vote share for bloc A is `0.4967` in both enacted and simulated plans.
- Diagnostic quality is acceptable for a pilot but incomplete for final inference. The run has `250` draws, `195` unique plans, and a unique draw fraction of `0.78`.

The immediate policy implication is practical. Hungary needs a routine, transparent district audit protocol that evaluates enacted or proposed plans against neutral, rule constrained simulation benchmarks. The current pilot already shows that this approach is operational in this repository and yields policy relevant output.

## Policy question and decision context

The policy question is direct. Do current district boundaries translate votes into seats in ways that are atypical under legal style constraints and real electoral geography.

This question matters for:

- The National Election Office and legislators who control districting procedures.
- Opposition and governing parties that contest map fairness claims.
- Civil society and election monitoring organizations that need evidence standards stronger than visual map critique.

A usable standard must be transparent and reproducible. This brief uses an ensemble benchmark, not shape based rhetoric, to assess whether observed outcomes are ordinary or outlying under declared constraints.

## Core policy recommendations

1. **Adopt an official ensemble audit requirement for district reform**
   - Every enacted or proposed map should be reported against a benchmark distribution generated under published constraints.
   - Required outputs should include seat share distributions, efficiency gap distributions, and focal percentile ranks.

2. **Publish machine readable constraint specifications**
   - Constraint logic should be versioned and public, including contiguity, county containment, and population tolerance settings.
   - The same constraint file must be used for simulation and for external replication.

3. **Set minimum simulation and diagnostic standards before legal use**
   - A pilot run with 250 draws is useful for workflow validation.
   - Official review should require substantially larger ensembles and stronger diagnostics before making legal claims.

4. **Make reproducibility a legal and administrative requirement**
   - Publish code, run commands, and artifact checks in one public repository.
   - Require a traceable run manifest so independent teams can rebuild results without private tooling.

5. **Report limits as part of every fairness claim**
   - Claims should separate county pilot evidence from national evidence.
   - Any partial rollup must explicitly list missing counties and weighting rules.

## Empirical results

### Data status of the current run

The current completed report is a partial rollup under `data/processed/runs/baranya-2022`. The rollup file reports:

- `counties_with_pair_of_reports: 1`
- `expected_counties: 20`
- `partial: true`
- Missing counties: `01`, `03` to `20`

This means the numerical values below are county pilot evidence. They do not support a national bias estimate yet.

### Metrics reported

The pipeline reports focal versus ensemble values for four metrics:

- `seat_share_a`
- `efficiency_gap`
- `mean_median_a_share_diff`
- `vote_share_a`

Bloc coding follows the repository default in the current run:

- Bloc A: `FIDESZ-KDNP (list 952)`
- Bloc B: sum of mapped lists `937` to `951` and `954` to `955`

### Baranya pilot comparison

From `counties/02/reports/partisan_report.json`:

- **Seat share A**
  - Focal: `0.50`
  - Ensemble mean: `0.546`
  - Ensemble p05 to p95: `0.50` to `0.75`
  - Percentile rank: `78.8`

- **Efficiency gap**
  - Focal: `0.0145`
  - Ensemble mean: `-0.0283`
  - Ensemble p05 to p95: `-0.2403` to `0.0901`
  - Percentile rank: `57.6`

- **Mean minus median A share**
  - Focal: `0.0051`
  - Ensemble mean: `-0.0043`
  - Ensemble p05 to p95: `-0.0205` to `0.0119`
  - Percentile rank: `78.8`

- **Vote share A**
  - Focal: `0.4967`
  - Ensemble mean: `0.4967`
  - Percentile rank: `100.0`

Interpretation is straightforward. The map changes seat translation but not county vote totals. That is exactly what districting does. At the same time, none of these pilot percentiles alone establish extreme outlier status. This is evidence for method validity and for non trivial seat translation effects, not final proof of national gerrymandering.

### Diagnostic quality

From `counties/02/reports/diagnostics.json`:

- Draws: `250`
- Unique assignment columns: `195`
- Duplicate assignment columns: `55`
- Unique fraction: `0.78`
- Number of units: `1455`
- Number of districts in county run: `4`
- Mean of maximum absolute relative population deviation: `0.1790`
- Maximum observed value: `0.2495`
- p95 value: `0.2420`

Coverage fields from the partisan report are important:

- `n_units_missing_vote_row: 880`
- `n_units_with_positive_two_party_votes: 555`
- `n_units_in_focal_aggregate: 555`

This confirms the county metrics are computed on the unit subset with valid vote data and focal district alignment. The coverage logic is explicit in the output and should remain explicit in policy communication.

## Conceptual framework

This brief follows the ensemble logic used in modern redistricting research and practice. The key idea is comparative, not absolute. A plan should be evaluated against a distribution of alternatives that respect the same legal and geographic rules.

This framework avoids two common errors:

- Treating compactness as sufficient proof of fairness.
- Treating one election cycle outcome as direct proof of manipulation.

In this repository, compactness is treated as secondary and constrained by the broader objective of evaluating partisan outcome distortion. Outcome metrics are the primary evidence class because they link district boundaries to seat allocation.

## Data and measurement design

### Unit of analysis

The pipeline builds precinct level geometry and vote tables from `data/raw/szavkor_topo/{maz}/{maz}-{taz}.json`. The canonical identifier is `precinct_id` using the `maz-taz-szk` structure.

### Inputs

- Geometry layer: `data/processed/precincts.parquet`
- Vote table: `data/processed/precinct_votes.parquet`
- Focal assignment table: `data/processed/focal_oevk_assignments.parquet`
- Adjacency graph: `data/processed/graph/adjacency_edges.parquet`

### Constraints

The current framework encodes and documents:

- Contiguity checks
- County containment option
- Elector balance constraints used for simulation runs
- Explicit discussion of statutory context and deferred legal details

Important legal implementation detail for policy readers: the repository distinguishes between enforced constraints and documentation only statutory notes. That distinction should be preserved in any public release.

### Two bloc coding

The analysis uses a two bloc reduction from multiparty list votes. This is a measurement choice needed for metrics like efficiency gap and seat share, not a legal claim about coalition structure. The mapping is versioned in `src/hungary_ge/metrics/data/partisan_party_coding.json`.

## Literature and analytical grounding

The analytical design aligns with the ALARM style of simulation based district evaluation:

- [ALARM Project](https://alarm-redist.org/index.html)
- [50 State Redistricting Simulations](https://alarm-redist.github.io/fifty-states/)
- [redist package](https://alarm-redist.org/redist/index.html)

For metric interpretation, this brief follows standard efficiency gap and symmetry style usage and keeps the emphasis on comparative distributions rather than single metric thresholds in isolation.

## Limits and what remains before national inference

This brief is confident about the completed pilot and explicit about its limits.

What can be claimed now:

- The workflow is operational and reproducible from raw JSON to county level fairness metrics.
- District assignment changes seat outcomes in measurable ways under fixed vote totals.
- The reporting format is policy ready and transparent about missing data and coverage.

What cannot be claimed now:

- A national estimate of partisan bias for all `106` OEVKs.
- Stable tail probability statements based on a large ensemble target.
- Cross county generalization from one completed county report pair.

What is required next:

- Complete county reports for all counties in a single run id.
- Expand draw count toward the project target scale.
- Retain and publish diagnostics for every county before rollup interpretation.

## Reproducibility protocol

### Environment

From repository root:

```bash
uv sync
```

Optional map visualization support:

```bash
uv sync --extra viz
```

Smoke test:

```bash
uv run python -c "import geopandas; import hungary_ge; print('ok')"
```

### Default pilot pipeline

```bash
uv run hungary-ge-pipeline
```

Equivalent:

```bash
uv run python -m hungary_ge.pipeline
```

Default stage sequence is `etl`, `votes`, `graph`.

### County run workflow for reports and rollup

Pick `RUN_ID`, then run:

```bash
uv run python -m hungary_ge.pipeline --only allocation --run-id "$RUN_ID"
uv run python -m hungary_ge.pipeline --mode county --run-id "$RUN_ID" --only graph
uv run python -m hungary_ge.pipeline --mode county --run-id "$RUN_ID" --only sample
uv run python -m hungary_ge.pipeline --mode county --run-id "$RUN_ID" --only reports
uv run python -m hungary_ge.pipeline --mode county --run-id "$RUN_ID" --only rollup --rollup-allow-partial
```

### Artifacts to verify

At minimum verify:

- `data/processed/runs/<RUN_ID>/counties/<maz>/reports/partisan_report.json`
- `data/processed/runs/<RUN_ID>/counties/<maz>/reports/diagnostics.json`
- `data/processed/runs/<RUN_ID>/national_report.json`

### Preconditions

- Raw `szavkor_topo` files must exist locally.
- County sampling stages that use `redist` require a working `Rscript` setup.
- Large processed artifacts are often gitignored, so full replication depends on local rebuild.

## Repository documentation and code traceability

This project already includes the core documentation expected for reproducible policy work:

- `README.md` for setup and workflow overview
- `REPRODUCIBILITY.md` for run commands and expected artifacts
- `docs/data-model.md` for data contracts and artifact definitions
- `docs/methodology.md` and `docs/partisan-metrics.md` for conceptual and metric design
- `docs/oevk-constraints.md` for legal rule translation and enforcement status

For final submission packaging, the brief should be submitted with:

- This policy memo
- The repository link or archive
- A run id specific note listing exact commands, runtime environment, and output checksums where available

## Conclusion

The project already delivers a serious policy prototype. It replaces map shape argument with reproducible evidence based on explicit constraints and comparative distributions. The current Baranya run supports a clear administrative recommendation. Hungary should institutionalize ensemble based district audits and require public reproducibility for districting review.

The evidence base is not yet national. That is a scope limit, not a design failure. The next step is to complete all county report pairs under one run and then rerun the same reporting structure at full coverage.
