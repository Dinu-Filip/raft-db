#!/usr/bin/env bash
# Benchmark 4: same write load at N=3 vs N=5 nodes, to show the
# replication-cost tradeoff (more nodes = more durability, less throughput).
# Sampled BENCH_REPEATS times (default 5, override via env) so the notebook
# can plot mean +/- stddev instead of one noisy sample.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib/common.sh"

CONCURRENCY="${1:-50}"
OUT_CSV="$RESULTS_DIR/cluster-size-throughput.csv"
echo -e "clusterSize\tconcurrency\trun\treqPerSec\tp50Ms\tp95Ms\tp99Ms\terrors" > "$OUT_CSV"

for size in 3 5; do
    run_dir="$RUN_ROOT/clustersize-n$size"
    echo "== cluster size $size =="
    start_cluster "$size" "$run_dir"
    run_throughput_sweep "$run_dir" "$size" "$OUT_CSV" "$CONCURRENCY"
    stop_cluster "$run_dir"
done

echo
echo "Results written to $OUT_CSV"
column -t -s $'\t' "$OUT_CSV"
