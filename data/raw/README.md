# Raw data

## `szavkor_topo/`

Hungarian **szavazókör** (precinct) boundaries as **custom JSON**: one file per settlement (`{maz}/{maz}-{taz}.json`), each containing a `list` of precinct polygons and IDs. See [`docs/data-model.md`](../docs/data-model.md) for field definitions (`maz`, `taz`, `szk`, `poligon`, `centrum`) and join keys.

## Other files

Place additional GeoJSON, CSV, or archives here as needed.

Large files matching patterns in the root `.gitignore` (for example `*.geojson` or `*.zip` placed directly under `data/raw/`) are not tracked by default; the `szavkor_topo/**/*.json` tree is intended to be versioned unless you add a broader ignore rule. Use Git LFS or an external artifact store if the repository gets too heavy for plain Git.
