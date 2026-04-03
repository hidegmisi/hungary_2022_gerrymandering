#!/usr/bin/env bash
# Example: full Hungary county-first ensemble using void-hex GeoParquet + fuzzy adjacency (fixed 3 m buffer).
# Produces per-county graphs/ensembles/reports and data/processed/runs/<RUN_ID>/national_report.json.
#
# -----------------------------------------------------------------------------
# NOTE: This is NOT the same recipe as the reference run "main".
#   - Default PARQUET: precincts_void_hex_voters.parquet (main uses the same after join_votes_to_precinct_layer).
#   - Default SAMPLE_DRAWS: 250 (main uses 1000).
#   - Passes --allow-disconnected-county-graph (main graph/sample did not).
#   - Combines graph+sample+reports in one step; omits rollup/policy_figures.
# For the versioned "main" commands, use docs/runs/main.md or scripts/run_main_analysis.sh.
# -----------------------------------------------------------------------------
#
# Prerequisites:
#   - Raw szavkor under data/raw/szavkor_topo (if you need to (re)build processed tables).
#   - National ETL + votes: precincts_void_hex[_voters].parquet, precinct_votes.parquet,
#     focal_oevk_assignments.parquet (see README.md and docs/runs/main.md).
#   - R + redist on PATH for the sample stage.
#
# Usage (from repo root, Git Bash or WSL):
#   chmod +x scripts/run_county_ensemble_hex_fuzzy.sh
#   export RUN_ID=my-national-2022
#   ./scripts/run_county_ensemble_hex_fuzzy.sh
#
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

RUN_ID="${RUN_ID:-national-hex-fuzzy-2022}"
# Population column for SMC must exist on this layer (typically *_voters build).
PARQUET="${PARQUET:-data/processed/precincts_void_hex_voters.parquet}"
SAMPLE_DRAWS="${SAMPLE_DRAWS:-250}"
SAMPLE_SEED="${SAMPLE_SEED:-20220403}"

export PYTHONIOENCODING=utf-8

if [[ ! -f "$PARQUET" ]]; then
  echo "Missing precinct layer: $PARQUET" >&2
  exit 1
fi

echo "[1/3] allocation: county OEVK counts -> runs/${RUN_ID}/"
uv run python -m hungary_ge.pipeline \
  --repo-root "$ROOT" \
  --only allocation \
  --run-id "$RUN_ID"

echo "[2/3] county graph + SMC + reports (fuzzy 3 m; tqdm on stderr in a TTY)"
uv run python -m hungary_ge.pipeline \
  --repo-root "$ROOT" \
  --mode county \
  --run-id "$RUN_ID" \
  --only graph sample reports \
  --parquet "$PARQUET" \
  --graph-fuzzy \
  --graph-fuzzy-buffering \
  --graph-fuzzy-buffer-m 3 \
  --allow-disconnected-county-graph \
  --no-county-maps \
  --sample-n-draws "$SAMPLE_DRAWS" \
  --sample-seed "$SAMPLE_SEED" \
  --sample-skip-existing

echo "[3/3] national rollup (strict: all counties must have report pairs)"
uv run python -m hungary_ge.pipeline \
  --repo-root "$ROOT" \
  --mode county \
  --run-id "$RUN_ID" \
  --only rollup \
  --parquet "$PARQUET"

echo "Done: data/processed/runs/${RUN_ID}/national_report.json"
