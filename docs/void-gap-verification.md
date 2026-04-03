# Void / hex overlay verification

This document records how **gap (void) polygons** are defined in ETL, how to reproduce **county-level shell vs precinct** metrics, and what the latest run shows about **near–full-county hex coverage**.

## ETL definition (confirmed)

In [`build_gap_features_for_maz`](../src/hungary_ge/io/gaps.py), for each `maz`:

1. Project shell and precincts to metric CRS (default **EPSG:32633**).
2. `shell_u = unary_union(shell geometries)` (with `buffer(0)` on parts).
3. `prec_u = unary_union(precinct geometries)`.
4. **`gap_m = shell_u.difference(prec_u)`** — i.e. county shell minus precinct union.
5. Optional **`--void-hex`**: tessellate gap pieces in [`gaps_hex.py`](../src/hungary_ge/io/gaps_hex.py).

So “hex overlay” density is driven by **how much of the shell remains after subtracting the precinct union**, not by a separate full-county grid.

## Script: per-county areas and overlap

Run from the repo root:

```bash
uv run python scripts/verify_void_gap_metrics.py \
  --precinct-parquet data/processed/precincts_void_hex.parquet \
  --shell data/raw/admin \
  --out-json data/processed/manifests/void_gap_metrics.json
```

- **`--shell`** may be repeated to compare two sources (e.g. admin directory vs a single multi-county GeoJSON).
- Output JSON lists, per shell source and per `maz`:
  - `shell_area_m2`, `precinct_union_area_m2`, `void_area_m2`
  - **`shell_precinct_intersection_area_m2`** — area\(`shell_u ∩ prec_u`\); when this is **~0**, `void_over_shell` is **~1** (entire shell becomes void) even if `precinct_union_area_m2` is large.

## Evidence from ETL manifest (`precincts_void_hex_etl.json`)

The build manifest under `data/processed/manifests/` records:

- Row counts: **`n_rows_szvk`** vs **`n_void_cells_after_hex`** (void rows explode when hex subdivision is on).
- **`total_gap_area_m2`** — sum of void polygon areas after processing.
- **`gap_build.warnings`**: e.g. **`no shell polygon for maz=08`** when the shell layer has no feature for that county (no void rows for that `maz`).
- **`per_maz`** — per-county void area and polygon counts from the same build.

Use the manifest together with `void_gap_metrics.json` for shell vs geometry QA.

## NVI alignment for `data/raw/admin` (megye 11–16)

Gap ETL sets shell **`maz` from the file stem** (`13.geojson` → `maz="13"`). One OSM-derived export had **six** files where the stem did **not** match NVI `maz` (e.g. **Pest** geometry lived under `13.geojson` while NVI **Pest** is `14`). That paired the wrong shell with each county’s precincts and produced **near–full-county void** for those `maz`.

**Fix applied in-repo:** [`scripts/fix_admin_shell_nvi_mapping.py`](../scripts/fix_admin_shell_nvi_mapping.py) rotates `11.geojson`–`16.geojson` so stem = NVI code and updates **`properties.ksh`** to match the stem (required by [`read_shell_gdf`](../src/hungary_ge/io/gaps.py)). The script is **idempotent** (no-op once `13.geojson` is Nógrád, not Pest). **Do not** run it twice on an already-fixed tree with `--force` — that would corrupt files.

After replacing admin shells, **re-run** `build_precinct_layer.py` with `--with-gaps` (and optional `--void-hex`) so void geometry and ETL manifests match the corrected boundaries.

## Findings (`verify_void_gap_metrics.py`)

Use **`scripts/verify_void_gap_metrics.py`** on your shell + szvk layer:

- Expect **non-zero** `shell_precinct_intersection_over_shell` for every `maz` when stems match NVI. **`void_over_shell` near 1** with large precinct union area usually means **wrong shell for that `maz`** (as with the misnumbered 11–16 export), not invalid precincts.

**Dropped precincts** (unrepaired geometry) are listed under `warnings` in the precinct ETL manifest; each id is `maz-taz-szk` — typically a small **count** with limited area impact compared with **zero shell overlap** from misaligned shells.

## Shell coverage checklist

- **Admin directory** `data/raw/admin/` (`01.geojson` … `20.geojson`): expect **20** county shells and **file stem = NVI `maz`** for the county whose boundary is inside (see [`read_shell_gdf`](../src/hungary_ge/io/gaps.py)).
- **Precinct layer**: every `maz` in the szvk table should have a matching shell row for gap construction; otherwise ETL logs **`no shell polygon for maz=…`** and skips voids for that county.
- **Alternative shell** (e.g. single multi-county file): must include all `maz` values present in the precinct layer; otherwise results differ materially (manifests may show **`n_shell_features_read` &lt; 20**).

## References

- Data model: [data-model.md](data-model.md) — void (`gap`) units section.
- Implementation: [`hungary_ge.io.gaps`](../src/hungary_ge/io/gaps.py), [`hungary_ge.io.gaps_hex`](../src/hungary_ge/io/gaps_hex.py).
