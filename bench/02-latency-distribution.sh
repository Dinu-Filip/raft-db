#!/usr/bin/env bash
# Benchmark 2: full write latency distribution at a fixed concurrency (the
# throughput plateau from 01-throughput-sweep.sh, overridable via $1).
#
# Every single request's latency is recorded - not just wrk's aggregate
# percentiles - so the notebook can plot the real distribution as a
# histogram. wrk's Lua scripting exposes a raw per-bucket histogram API
# (latency(i)), but it's effectively undocumented (wrk's own SCRIPTING doc
# gives it one line: "latency(i) -- raw value and count") and empirically
# it did not behave sanely when probed directly (iterating it returned
# wildly inconsistent totals against summary.requests) - not something to
# build a benchmark on without being able to verify it against wrk's
# source. So instead this drives a pool of CONCURRENCY background curl
# workers directly, each timing every request it makes with wall-clock
# date calls around the call - the same directly-verifiable approach
# 03-failover-recovery.sh and 05-read-your-writes.sh already use for
# per-request timing, just running a worker pool instead of one sequential
# loop. This trades wrk's raw throughput for every single latency value
# being real and auditable.
#
# Sampled BENCH_REPEATS times (default 5, override via env) against a fresh
# cluster each time, each run lasting BENCH_DURATION seconds (default 10).
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib/common.sh"

CONCURRENCY="${1:-50}"
CLUSTER_SIZE="${2:-3}"
DURATION="${BENCH_DURATION:-10}"
REPEATS="${BENCH_REPEATS:-5}"

OUT_CSV="$RESULTS_DIR/latency-distribution.csv"
echo -e "run\tclusterSize\tconcurrency\tlatencyMs" > "$OUT_CSV"

run_dir="$RUN_ROOT/latency-n$CLUSTER_SIZE"

# One background worker per unit of concurrency, each firing INSERTs
# back-to-back until end_time, recording its own start/end timestamp pair
# per request into its own file (no shared state between workers, so no
# locking needed).
run_latency_workers() {
    local leader_port="$1"
    local worker_dir="$2"
    local end_time="$3"

    for ((w = 1; w <= CONCURRENCY; w++)); do
        (
            local i=0
            local id_base=$((w * 10000000))
            local out="$worker_dir/worker_$w.tsv"
            : > "$out"
            while [ "$(date +%s)" -lt "$end_time" ]; do
                i=$((i + 1))
                local id=$((id_base + i))
                local t0 t1
                t0=$(date +%s.%N)
                curl -s -m5 -X POST "http://localhost:$leader_port/" \
                    -d "{\"queryType\":\"INSERT\",\"tableName\":\"bench\",\"attributes\":[],\"values\":[$id,$i]}" \
                    > /dev/null
                t1=$(date +%s.%N)
                echo "$t0 $t1" >> "$out"
            done
        ) &
    done
    wait
}

for ((run = 1; run <= REPEATS; run++)); do
    echo "== run $run/$REPEATS =="
    start_cluster "$CLUSTER_SIZE" "$run_dir"

    IFS=$'\t' read -r leader_port _ <<< "$(leader_endpoint "$run_dir")"

    worker_dir=$(mktemp -d)
    end_time=$(($(date +%s) + DURATION))
    run_latency_workers "$leader_port" "$worker_dir" "$end_time"

    # Converts each worker's raw (t0 t1) timestamp pairs to a latencyMs row
    # in one batched awk pass per worker, rather than spawning a process per
    # request to do the arithmetic.
    for f in "$worker_dir"/worker_*.tsv; do
        awk -v run="$run" -v cs="$CLUSTER_SIZE" -v conc="$CONCURRENCY" \
            '{ printf "%d\t%d\t%d\t%.2f\n", run, cs, conc, ($2 - $1) * 1000 }' \
            "$f" >> "$OUT_CSV"
    done
    rm -rf "$worker_dir"

    stop_cluster "$run_dir"
done

echo
echo "Results written to $OUT_CSV"
awk -F'\t' 'NR>1 {n++; sum+=$4} END {if (n>0) printf "Captured %d requests, mean latency %.2f ms\n", n, sum/n}' "$OUT_CSV"
