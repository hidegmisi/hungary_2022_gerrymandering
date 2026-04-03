# Processed data

Cleaned tables, adjacency graphs, and other derived artifacts produced by the pipeline live here.

- **ETL / votes manifests** — `manifests/<artifact-stem>_etl.json` (reproducible fingerprints; safe to keep in git if small).
- **Run manifests** — `manifests/run_<UTC>.json` written after each successful `hungary-ge-pipeline` invocation (argv, optional git commit, output hashes). Ignored by default in `.gitignore`; delete old files locally if clutter builds up.
- **Experimental Folium HTML** under `graph/` — safe to remove if not needed; regenerate with the `viz` stage or `scripts/map_adjacency.py`.
