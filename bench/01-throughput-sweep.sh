#!/usr/bin/env bash
# Benchmark 1: steady-state write throughput vs concurrency, single-node
# baseline vs a 3-node cluster. Reports where throughput plateaus. Each
# concurrency is sampled BENCH_REPEATS times (default 5, override via env)
# so the notebook can plot mean +/- stddev instead of one noisy sample.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib/common.sh"

CONCURRENCIES=(1 10 25 50 75 100)
OUT_CSV="$RESULTS_DIR/throughput-sweep.csv"
echo -e "clusterSize\tconcurrency\trun\treqPerSec\tp50Ms\tp95Ms\tp99Ms" > "$OUT_CSV"

for size in 1 3; do
    run_dir="$RUN_ROOT/throughput-n$size"
    echo "== cluster size $size =="
    start_cluster "$size" "$run_dir"
    run_throughput_sweep "$run_dir" "$size" "$OUT_CSV" "${CONCURRENCIES[@]}"
    stop_cluster "$run_dir"
done

echo
echo "Results written to $OUT_CSV"
column -t -s $'\t' "$OUT_CSV"
echo
echo "Plateau: the concurrency at which reqPerSec stops increasing >5% is the interesting number, not the peak."
