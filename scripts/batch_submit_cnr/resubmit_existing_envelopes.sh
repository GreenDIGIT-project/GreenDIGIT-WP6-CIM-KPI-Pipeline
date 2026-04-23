#!/usr/bin/env bash

set -euo pipefail

cd /home/ubuntu/GreenDIGIT-WP6-CIM-KPI-Pipeline

set -a
source .env
set +a

BATCH_SIZE="${BATCH_SIZE:-5000}"
DRY_RUN="${DRY_RUN:-0}"

# Use only the minimal dump set that covers the failed windows without replaying
# 2026-03-30 twice. The second dump contains both 2026-03-30 and 2026-03-31.
dump_dirs=(
  "/~/data/1774224000_1774310399_dump"
  "/~/data/1774828800_1775001599_dump"
)

for dump_dir in "${dump_dirs[@]}"; do
  processed_dir="$dump_dir/02_dump_processed"

  if [[ ! -d "$processed_dir" ]]; then
    echo "Missing processed dir: $processed_dir" >&2
    exit 1
  fi

  shopt -s nullglob
  files=( "$processed_dir"/envelopes_*.jsonl )
  shopt -u nullglob

  if [[ ${#files[@]} -eq 0 ]]; then
    echo "No envelope files found in: $processed_dir" >&2
    exit 1
  fi

  echo "Submitting from: $processed_dir"
  if [[ "$DRY_RUN" == "1" ]]; then
    python3 scripts/batch_submit_cnr/load_envelopes_direct_cnr.py \
      "${files[@]}" \
      --batch-size "$BATCH_SIZE" \
      --dry-run
  else
    python3 scripts/batch_submit_cnr/load_envelopes_direct_cnr.py \
      "${files[@]}" \
      --batch-size "$BATCH_SIZE"
  fi
done

echo "Replay completed."
