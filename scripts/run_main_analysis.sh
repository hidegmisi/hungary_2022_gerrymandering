#!/usr/bin/env bash
# Reproduce the reference county-first ensemble under data/processed/runs/main/.
# See docs/runs/main.md for context, raw data inventory, and R/renv setup.
#
# Usage (from repo root, Git Bash or WSL):
#   chmod +x scripts/run_main_analysis.sh
#   ./scripts/run_main_analysis.sh
#
# Optional:
#   SAMPLE_SEED=20220403 ./scripts/run_main_analysis.sh   # pin SMC seed (main reference had none)
#
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

RUN_ID="${RUN_ID:-main}"
PARTY_JSON="${PARTY_JSON:-src/hungary_ge/metrics/data/partisan_party_coding.json}"
# Void-hex ETL output (no vote columns); sampling needs `voters` on the graph layer.
VOID_HEX_PARQUET="${VOID_HEX_PARQUET:-data/processed/precincts_void_hex.parquet}"
PARQUET="${PARQUET:-data/processed/precincts_void_hex_voters.parquet}"

export PYTHONIOENCODING=utf-8

echo "[0/5] national void-hex ETL + votes -> ${VOID_HEX_PARQUET}, precinct_votes, focal_oevk_assignments"
uv run python -m hungary_ge.pipeline \
  --repo-root "$ROOT" \
  --pipeline-profile void_hex_fuzzy_latest \
  --only etl votes

echo "[0b/5] join precinct_votes onto void-hex layer -> ${PARQUET} (required for --sample-pop-column voters)"
uv run python scripts/join_votes_to_precinct_layer.py \
  --precinct-parquet "$VOID_HEX_PARQUET" \
  --votes-parquet data/processed/precinct_votes.parquet \
  --out-parquet "$PARQUET" \
  --require-voters

echo "[1/5] allocation -> runs/${RUN_ID}/"
uv run python -m hungary_ge.pipeline \
  --repo-root "$ROOT" \
  --mode county \
  --run-id "$RUN_ID" \
  --only allocation

echo "[2/5] county graphs (fuzzy 3 m, no Folium)"
uv run python -m hungary_ge.pipeline \
  --repo-root "$ROOT" \
  --mode county \
  --run-id "$RUN_ID" \
  --only graph \
  --parquet "$PARQUET" \
  --graph-fuzzy \
  --graph-fuzzy-buffering \
  --graph-fuzzy-buffer-m 3 \
  --no-county-maps

SAMPLE_EXTRA=()
if [[ -n "${SAMPLE_SEED:-}" ]]; then
  SAMPLE_EXTRA+=(--sample-seed "$SAMPLE_SEED")
fi

echo "[3/5] SMC sample (1000 draws/county; R + redist required)"
uv run python -m hungary_ge.pipeline \
  --repo-root "$ROOT" \
  --mode county \
  --run-id "$RUN_ID" \
  --only sample \
  --parquet "$PARQUET" \
  --graph-fuzzy \
  --graph-fuzzy-buffering \
  --graph-fuzzy-buffer-m 3 \
  --sample-n-draws 1000 \
  --no-county-maps \
  "${SAMPLE_EXTRA[@]}"

echo "[4/5] reports, rollup, policy_figures"
uv run python -m hungary_ge.pipeline \
  --repo-root "$ROOT" \
  --mode county \
  --run-id "$RUN_ID" \
  --only reports rollup policy_figures \
  --parquet "$PARQUET" \
  --reports-party-coding "$PARTY_JSON" \
  --policy-figures-party-coding "$PARTY_JSON"

echo "Done: data/processed/runs/${RUN_ID}/national_report.json"
