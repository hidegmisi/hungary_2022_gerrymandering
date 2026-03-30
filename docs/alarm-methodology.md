# ALARM methodology (Algorithm-Assisted Redistricting Methodology)

This note summarizes how the **ALARM Project** defines and implements simulation-based redistricting analysis, as documented on [alarm-redist.org](https://alarm-redist.org/index.html) and in the peer-reviewed literature. It is **not** specific to Hungary or this repository’s eventual sampler; for project scope, see [methodology.md](methodology.md).

## What ALARM is

The **Algorithm-Assisted Redistricting Methodology (ALARM) Project** is a research program that develops methodology and open-source software to analyze **legislative redistricting** (and related political geography questions). According to the [project home page](https://alarm-redist.org/index.html), it is led by co-PIs Kosuke Imai, Christopher T. Kenny, Cory McCartan, and Tyler Simko.

ALARM is the **research umbrella** (methods, papers, datasets). The **default public implementation** of the sampling and analysis workflow is the R package **redist**, maintained in the same ecosystem. Applied studies (for example on U.S. congressional maps) use **redist**-style ensembles and compare enacted plans to simulated alternatives; see [Kenny et al. (2023)](#references).

## Conceptual core: counterfactuals via simulation

### Why not compare across states or time periods?

To assess whether a map is “unfair,” analysts need a **counterfactual**: what would plausibly have happened under the **same** rules and the **same** geographic distribution of people and votes? Comparing to maps from other states or past cycles confounds differences in **electorate**, **geography**, and **legal rules**. ALARM’s materials (including the [redist 101](https://alarm-redist.org/redist101.html) vignette) stress that **in-jurisdiction simulation** under explicit constraints is the appropriate benchmark, not informal cross-jurisdiction comparison (see also Kenny et al., 2022, cited in redist 101).

### Why simulation?

The number of distinct contiguous partitions of geographic units into districts is **astronomically large** even for modest grids (redist 101 cites Fifield et al., 2020, for scale illustrations). **Enumeration** is infeasible for real precinct-scale problems. **Monte Carlo sampling** draws a **representative ensemble** of feasible plans under stated constraints; the enacted or proposed plan is then compared to that ensemble. If its partisan or representational outcomes sit in the **tail** of the ensemble distribution, that supports an argument that the plan is **unusual** relative to “neutral” sampling under those rules.

Empirical validation of simulation design choices is emphasized in the literature (e.g. Fifield et al., 2020).

## Implementation: the `redist` pipeline

The [redist 101](https://alarm-redist.org/redist101.html) vignette walks through the standard workflow. The following mirrors that order and ties each step to **R** objects and functions.

### 1. `redist_map`: problem specification

A **`redist_map`** object bundles:

- **Geographic units** (e.g. precincts) and their **geometry** (for plotting and spatial operations).
- **Population** (or other weights) and the number of districts **`ndists`**.
- **Population parity:** typically controlled via **`pop_tol`**, the allowable deviation from exact equality (for example ±0.5% in the North Carolina example in redist 101, which yields an explicit min–max population band printed by the sampler).
- **Adjacency** **`adj`:** a list giving, for each unit, which other units share a boundary (formal **contiguity** for district-building).

Projections (**CRS**) should be consistent when combining layers; redist 101 points to CRS and shapefile preprocessing considerations.

### 2. Adjacency and contiguity fixes

Districts are usually required to be **contiguous**. Algorithms consume an **adjacency graph** derived from geography. When coastlines, water, or bridges break naive adjacency, analysts may **edit edges** (for example with the **geomander** package’s helpers, as noted in redist 101).

### 3. Sampling algorithms

The **redist** package implements several ways to draw plans from a **target distribution** over feasible partitions.

| Mechanism | R functions | Role |
|-----------|-------------|------|
| **Sequential Monte Carlo (SMC)** | **`redist_smc()`** | Primary recommended sampler in redist 101 for generating large ensembles under contiguity, population, compactness, and administrative rules. Theoretical reference: McCartan & Imai (2023). |
| **Markov chain Monte Carlo (MCMC)** | **`redist_flip()`**, **`redist_mergesplit()`** | Alternative MCMC moves; documented alongside SMC in redist 101. |

#### `redist_smc()` in practice

The [reference documentation](https://alarm-redist.org/redist/reference/redist_smc.html) states that **`redist_smc`** draws samples from a target measure determined by **`map`**, **`compactness`**, and **`constraints`**.

Notable implementation details from that documentation:

- **`compactness`:** nonnegative; higher values favor more compact districts. **`compactness = 1`** is noted as computationally efficient and producing reasonably compact districts; **values other than 1** can produce **highly variable importance sampling weights**, in which case **truncation** of weights is recommended (with **PSIS** preferred when the **loo** package is available).
- **`counties`:** optional vector of county (or similar) labels; the algorithm restricts to maps that split at most **`ndists - 1`** counties (with further split constraints available via constraint helpers).
- **`constraints`:** a **`redist_constr`** object or list for additional sampling constraints (see below).
- **`resample`:** whether to apply a **final resampling** step so draws are immediately usable; if `FALSE`, importance sampling estimates can be adjusted manually.
- **`runs`:** number of **independent parallel runs**, each with **`nsims`** plans—useful for **simulation standard errors** and **cross-run diagnostics** (output uses a **`chain`** column).
- **`ncores`:** parallelization within a run; multi-core runs may **not** be bitwise reproducible with `set.seed()` alone.
- **`pop_temper`**, **`final_infl`:** help when the sampler **stalls** on late splits or the final population constraint.
- **Diagnostics:** unless silenced, the function reports **effective sample size** at resampling steps; **`verbose`** adds per-step detail. Users are pointed to **`summary.redist_plans()`** for further checks.

The **mathematical construction** of the SMC sampler (sequential splitting, resampling, and the role of compactness in the proposal) is given in full in McCartan & Imai (2023); this document does not reproduce those equations.

### 4. Constraints: hard vs soft

- **Hard constraints** (must hold for every kept plan) include **population tolerance**, **contiguity**, and caps on **administrative splits** (e.g. counties), as summarized in redist 101.
- **Soft constraints** encode preferences or approximate legal norms in the **target distribution** via **`redist_constr()`** and **`add_constr_*`** helpers. The [constraints reference](https://alarm-redist.org/redist/reference/constraints.html) lists options. redist 101 warns that **very large `strength`** values on soft constraints can hurt **accuracy and efficiency**—diagnostics should be checked.

### 5. Output: `redist_plans`

Simulations return a **`redist_plans`** object: a **matrix** of district assignments (units × draws) plus tidy **metadata** (draw identifiers, populations per district, etc.). Multiple **`runs`** support **R-hat**-style comparison of summary statistics across chains, as shown in redist 101’s **`summary()`** output.

### 6. Diagnostics

redist 101 demonstrates **`summary()`** on SMC output, including:

- **R-hat**-style diagnostics for derived summaries (e.g. population deviation, compactness, county splits, partisan metrics).
- Per-**split** tables: **effective sample size (%)**, **acceptance rates**, **log weight SD**, **estimated uniqueness** of partial plans.

These are used to verify that the ensemble **explores** the constrained space rather than collapsing to a few near-duplicate plans.

### 7. Evaluating plans (post-simulation metrics)

After building an ensemble, analysts compare the **focal** plan to simulated plans using metrics such as (as listed in redist 101):

- **Expected seats** (or seat counts under a fixed electoral model).
- **District-level vote margins** (for packing/cracking-style interpretation).
- **Efficiency gap** (wasted-vote imbalance).
- **Partisan bias** (excess seat share relative to vote share).

**Interpretation:** Evidence of gerrymandering is often framed as the focal map being a **clear outlier** on these metrics relative to the ensemble—conditional on the encoded rules and data.

## Ecosystem beyond the core sampler

- **[50-State Redistricting Simulations](https://alarm-redist.org/fifty-states/):** large-scale simulated congressional plans for U.S. states; methodology and data are described in the **Scientific Data** article (see [References](#references)).
- **`alarmdata`:** R package to retrieve **pre-built `redist_map` objects** and related outputs from ALARM releases ([introduction](https://alarm-redist.org/posts/2024-03-10-introducing-alarmdata/)).
- **2020 redistricting inputs:** ALARM publishes tidily joined **Census** and **VEST**-style election data at precinct scale for U.S. work ([data release](https://alarm-redist.org/posts/2021-08-10-census-2020/)).

## Relation to this repository

This project may **reuse the same logical steps** (map → adjacency → constrained ensemble → metrics) while implementing constraints appropriate to **Hungarian OEVK** law in R, Python, or a mix. The present repo’s choices are documented in [methodology.md](methodology.md); they are **not** identical to the U.S.-centric defaults in **alarmdata** unless explicitly aligned.

## References

- ALARM Project: [https://alarm-redist.org/index.html](https://alarm-redist.org/index.html)
- **redist 101** (vignette): [https://alarm-redist.org/redist101.html](https://alarm-redist.org/redist101.html)
- **redist** package: [https://alarm-redist.org/redist/index.html](https://alarm-redist.org/redist/index.html)
- **`redist_smc`**: [https://alarm-redist.org/redist/reference/redist_smc.html](https://alarm-redist.org/redist/reference/redist_smc.html)
- **Constraints (`redist_constr`, `add_constr_*`)**: [https://alarm-redist.org/redist/reference/constraints.html](https://alarm-redist.org/redist/reference/constraints.html)
- McCartan, C., & Imai, K. (2023). Sequential Monte Carlo for sampling balanced and compact redistricting plans. *Annals of Applied Statistics*, 17(4), 3300–3323. [https://doi.org/10.1214/23-AOAS1763](https://doi.org/10.1214/23-AOAS1763)
- Fifield, B., Imai, K., Kawahara, J., & Kenny, C. T. (2020). The essential role of empirical validation in legislative redistricting simulation. *Statistics and Public Policy*, 7(1), 52–68. [https://doi.org/10.1080/2330443X.2020.1791773](https://doi.org/10.1080/2330443X.2020.1791773)
- Kenny, C. T., McCartan, C., Simko, T., Kuriwaki, S., & Imai, K. (2022). Widespread partisan gerrymandering mostly cancels nationally, but reduces electoral competition. arXiv:2208.06968. [https://doi.org/10.48550/arXiv.2208.06968](https://doi.org/10.48550/arXiv.2208.06968) (see also published version below.)
- Kenny, C. T., McCartan, C., Simko, T., Kuriwaki, S., & Imai, K. (2023). Widespread partisan gerrymandering mostly cancels nationally, but reduces electoral competition. *Proceedings of the National Academy of Sciences*, 120(24), e2217322120. [https://doi.org/10.1073/pnas.2217322120](https://doi.org/10.1073/pnas.2217322120)
- McCartan, C., Kenny, C. T., Simko, T., Garcia III, G., Wang, K., Wu, M., Kuriwaki, S., & Imai, K. (2022). Simulated redistricting plans for the analysis and evaluation of redistricting in the United States. *Scientific Data*, 9, 683. [https://doi.org/10.1038/s41597-022-01808-2](https://doi.org/10.1038/s41597-022-01808-2)
