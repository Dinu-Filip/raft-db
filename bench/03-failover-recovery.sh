#!/usr/bin/env bash
# Benchmark 3: leader failover recovery time. Kills -9 the leader process
# and measures wall-clock time to the first successful write afterwards.
# Repeated BENCH_REPEATS times (default 5, override via env), each trial on
# a freshly started cluster, so the notebook can plot mean +/- stddev
# instead of one noisy sample.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib/common.sh"

CLUSTER_SIZE="${1:-3}"
REPEATS="${BENCH_REPEATS:-5}"
run_dir="$RUN_ROOT/failover"

OUT_CSV="$RESULTS_DIR/failover-recovery.csv"
echo -e "run\tclusterSize\trecoveryMs" > "$OUT_CSV"

for ((run = 1; run <= REPEATS; run++)); do
    echo "== run $run/$REPEATS =="
    start_cluster "$CLUSTER_SIZE" "$run_dir"

    leader_id=$(wait_for_leader "$run_dir")
    IFS=$'\t' read -r leader_port leader_pid <<< "$(leader_endpoint "$run_dir")"

    echo "Leader is node $leader_id (pid $leader_pid, port $leader_port). Sending kill -9..."
    kill_time=$(date +%s.%N)
    kill -9 "$leader_pid"

    mapfile -t remaining_ports < <(awk -F'\t' -v id="$leader_id" '$1!=id {print $3}' "$run_dir/manifest.tsv")

    success=0
    attempt=0
    while [ "$success" -eq 0 ]; do
        attempt=$((attempt + 1))
        for port in "${remaining_ports[@]}"; do
            resp=$(curl -s -m1 -X POST "http://localhost:$port/" \
                -d '{"queryType":"INSERT","tableName":"bench","attributes":[],"values":[777,777]}' 2>/dev/null || true)
            if [[ "$resp" == *'"success"'* ]]; then
                success=1
                break
            fi
        done
        if [ "$attempt" -gt 300 ]; then
            echo "error: no successful write within 30s of killing the leader" >&2
            stop_cluster "$run_dir"
            exit 1
        fi
        [ "$success" -eq 0 ] && sleep 0.1
    done
    recovered_time=$(date +%s.%N)

    recovery_ms=$(echo "($recovered_time - $kill_time) * 1000" | bc)
    printf "%d\t%d\t%.1f\n" "$run" "$CLUSTER_SIZE" "$recovery_ms" >> "$OUT_CSV"

    stop_cluster "$run_dir"
    echo "Recovery time: ${recovery_ms} ms"
done

echo
echo "Results written to $OUT_CSV"
column -t -s $'\t' "$OUT_CSV"
echo
awk -F'\t' 'NR>1 {sum+=$3; n++} END {if (n>0) printf "Mean recovery: %.1f ms over %d runs\n", sum/n, n}' "$OUT_CSV"
