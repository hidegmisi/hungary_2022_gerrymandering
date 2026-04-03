# Raw data

## What is tracked in git (clone is sufficient for void-hex + votes)

As of the current repository layout:

- **`szavkor_topo/`** — **3177** settlement JSON files (`{maz}/{maz}-{taz}.json`), one per tracked település, with **szavazókör** geometries and IDs.
- **`admin/`** — **20** county shell GeoJSON files (`01.geojson` … `20.geojson`, `maz` codes) used by `--pipeline-profile void_hex_fuzzy_latest` as `--etl-shell data/raw/admin`.

No additional raw downloads are required for rebuilding **`precincts_void_hex.parquet`**, **`precinct_votes.parquet`**, and **`focal_oevk_assignments.parquet`** from a fresh clone.

For the full county-first ensemble and memo figures (**`run_id=main`**), run the steps in [`docs/runs/main.md`](../../docs/runs/main.md). Large outputs (`data/processed/*.parquet`, `data/processed/runs/`, …) stay out of git by default; see [`REPRODUCIBILITY.md`](../../REPRODUCIBILITY.md).

## `szavkor_topo/`

Hungarian **szavazókör** (precinct) boundaries as **custom JSON**: each file lists precinct polygons and IDs. See [`docs/data-model.md`](../../docs/data-model.md) for fields (`maz`, `taz`, `szk`, `poligon`, `centrum`) and join keys.

## Other files

Place additional GeoJSON, CSV, or archives under `data/raw/` as needed.

Files matching broad patterns in the root [`.gitignore`](../../.gitignore) (for example `*.geojson` or `*.zip` **directly** under `data/raw/`) are not tracked by default. Paths like `data/raw/admin/*.geojson` are **not** covered by that top-level rule and are versioned here.

Forks or future releases may slim the tree; if `szavkor_topo` is omitted, use Git LFS or an external artifact store and document acquisition in this README.
