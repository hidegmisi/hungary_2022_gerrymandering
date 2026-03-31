# Partisan metrics (Slice 9)

## Two-bloc reduction

Hungarian parliamentary list votes are **multiparty**. For ensemble comparisons in v1, the code sums mapped `votes_*` columns into two blocs **A** and **B** using a versioned JSON config:

- Packaged example: [`src/hungary_ge/metrics/data/partisan_party_coding.json`](../src/hungary_ge/metrics/data/partisan_party_coding.json)
- Schema: `hungary_ge.metrics.party_coding/v1` (`party_a_columns`, `party_b_columns`, optional labels)

This is a **measurement choice** for metrics (efficiency gap, seat share), not a claim that those lists form a legal electoral alliance. Researchers should edit the JSON to match their design and document the mapping in papers or appendices.

[`list_map_vote_columns`](../src/hungary_ge/metrics/party_coding.py) lists `votes_*` names from `election_2022_list_map.json` but **does not** assign blocs automatically.

## Void (`gap`) units

[`focal_vs_ensemble_metrics`](../src/hungary_ge/metrics/compare.py) aligns votes to [`PlanEnsemble`](../src/hungary_ge/ensemble/plan_ensemble.py) `unit_ids`. Precincts without vote rows contribute **0** to both blocs but **still carry** a district label in simulated plans. The **focal** enacted table usually has no row for `gap-…` ids; those units are **omitted from the focal district aggregation** when they have no votes (see report `extra.n_units_in_focal_aggregate`). Any unit with **positive** two-party votes and no focal label raises if `strict_focal_for_voting_units=True` (default).

## Implemented metrics

| Metric | Notes |
|--------|--------|
| `vote_share_a` | National two-party: \(\sum A / \sum(A+B)\) over districts with data |
| `seat_share_a` | SMD: A wins district if \(A>B\); **ties** credit **0.5** seats each; denominator = districts with positive turnout |
| `efficiency_gap` | Stephanopoulos–McGhee-style wasting on the same district totals |
| `mean_median_a_share_diff` | Mean minus median of district-level **A share** (symmetry-style descriptive) |

**Percentile rank:** For each metric, the report includes the focal value and `percentile_rank` = \(100 \times \mathbb{E}[\text{draw} \le \text{focal}]\) over ensemble draws (inclusive).

## API

- [`focal_vs_ensemble_metrics(focal, ensemble, votes, party_coding=…)`](../src/hungary_ge/metrics/compare.py)
- [`partisan_metrics(ensemble, votes, focal=…)`](../src/hungary_ge/metrics/__init__.py) — same computation with argument order optimized for pipelines

Outputs are typically **in-memory** [`PartisanComparisonReport`](../src/hungary_ge/metrics/report.py); optional [`write_json`](../src/hungary_ge/metrics/report.py) for snapshots.
