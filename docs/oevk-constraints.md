# Hungarian OEVK constraints (simulation encoding)

This note translates **statutory and project rules** into what [`hungary_ge.constraints`](../src/hungary_ge/constraints/) validates today, what is **documentation-only**, and what is **deferred**. It is not legal advice. Replace **Statutory source (TBD)** with official Hungarian citations (Act on parliamentary elections; Annexes 1–2) when finalizing a publication.

For ALARM / `redist` framing, see [alarm-methodology.md](alarm-methodology.md). For void (`gap`) units and zero elector weights, see [data-model.md](data-model.md) and [master-plan.md](master-plan.md) Slice 3–5 notes.

---

## Hard rules

| Rule | Statutory / project summary | v1 in code (`check_plan`) | Deferred / doc only |
|------|-----------------------------|---------------------------|---------------------|
| **106 OEVKs** | Single-member districts; Annex 1 fixes counts **per county and Budapest**. | `ElectorBalanceConstraint.ndists` / label range `1 … ndists` (default 106). | Enforcing **Annex 1 seat totals per megye/Budapest** (needs machine-readable allocation). |
| **County / Budapest walls** | No district may cross a **county** boundary or **Budapest vs non-Budapest** boundary. | Optional [`CountyContainmentConstraint`](../src/hungary_ge/constraints/constraint_spec.py): with `county_ids` per node, each district may use only **one** county code (model Budapest as its own code, e.g. canonical `maz`). | — |
| **Contiguity** | Every OEVK must be **contiguous** (territorially connected). | [`ContiguityConstraint`](../src/hungary_ge/constraints/constraint_spec.py): induced subgraph per district must be connected on [`AdjacencyGraph`](../src/hungary_ge/graph/adjacency_graph.py). | — |
| **Elector equality** | Law uses **eligible voters** (választópolgárok), not census population. | Weights vector aligned to graph nodes; voids = **0**. Use the same column the sampler uses (e.g. joined **`voters`** as proxy until a dedicated eligible-voter field exists). | Official NVI eligible-voter extract if it differs from `voters`. |
| **±15% (simulation)** | Statute allows **>15%** only under a fuller legal test (county/contiguity/local factors). **This project:** every **simulated** plan must keep each district within **±15%** of ideal electors per district. | `max_relative_deviation = 0.15` (hard fail). | — |
| **20% (statute)** | Above **20%** requires Annex 2 / statutory correction. | **Not** an acceptable band for draws—**documented here only**; validator does **not** allow 15–20% for ensemble members. | — |
| **Redistricting freeze** | Jan 1 of year before general election through election day (except dissolution). | Document only. | Calendar checks. |
| **Annex 2 binding** | Map in Annex 2 defines territories; cosmetics do not move boundaries. | Document only (focal vs ensemble comparison uses your focal table). | — |
| **Split municipalities** | Annex 2 splits several non-Budapest cities (Debrecen, Győr, Kecskemét, Miskolc, Pécs, Szeged, Székesfehérvár). Splitting other municipalities follows electorate-size rules. | **Deferred:** v1 uses **whole-settlement** graph units (e.g. szvk) unless you add sub-municipality nodes later. | Eligibility test for splittable settlements internal boundaries (roads, centerlines, …). |
| **County-rights city** | Old (1997-era) rule is **not** current law for modern splits. | **Guideline:** simulation uses **electorate threshold**, not megyei jogú város status. | — |

---

## Guidelines (sampler / narrative)

- Prefer **whole municipalities** when building priors or proposal symmetry.
- **Optimization ordering** (for Slice 6+): elector balance first, then county containment + contiguity, then local-factor coherence.
- **±15% in law** cannot be “filled” with compactness or partisan convenience—those are not statutory substitutes for the listed factors.

---

## Void (`gap`) rows

Graph nodes can include `unit_kind=void` with **zero** electors. They must appear in `assignments` and in the **same** `AdjacencyGraph` as szvk units. Ideal district size uses **total electors = sum(weights)** (voids contribute 0). Do not assign synthetic votes on voids for partisan metrics ([data-model.md](data-model.md)).

---

## Annex 1 / 2 placeholders

- **Statutory source (TBD):** cite the official short title and § / Annex references for 106 districts, Annex 1 seat distribution, and Annex 2 territorial descriptions.
- **Annex 1 enforcement:** add a future check “district count per county/Budapest matches table” when a versioned JSON of that table lives in the repo.

---

## JSON spec artifact

[`ConstraintSpec`](../src/hungary_ge/constraints/constraint_spec.py) serializes to JSON (`schema_version` + `version` + nested constraints) for reproducible runs alongside ensemble manifests.
