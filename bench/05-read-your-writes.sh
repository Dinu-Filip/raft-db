#!/usr/bin/env bash
# Benchmark 5: read-your-writes spot check. Writes a uniquely identifiable
# row to the leader, then polls a follower's local SELECT until the row
# shows up, recording the staleness window. Repeated BENCH_REPEATS times
# (default 3, override via env), each trial on a freshly started cluster, so
# the notebook can plot mean +/- stddev across independent trials instead of
# treating all samples as if they came from one warm cluster.
#
# Uses a full-table SELECT rather than a WHERE clause: parseTwoArgCondition
# (client-handling/input.c) dereferences an Operand it never allocates,
# corrupting the heap on any conditioned SELECT/UPDATE/DELETE. None of these
# benchmarks need a WHERE clause, so this works around it instead of fixing
# that parsing path.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib/common.sh"

CLUSTER_SIZE="${1:-3}"
ITERATIONS="${2:-20}"
REPEATS="${BENCH_REPEATS:-3}"
run_dir="$RUN_ROOT/read-your-writes"

OUT_CSV="$RESULTS_DIR/read-your-writes.csv"
echo -e "run\titeration\tstalenessMs" > "$OUT_CSV"

for ((run = 1; run <= REPEATS; run++)); do
    echo "== run $run/$REPEATS =="
    start_cluster "$CLUSTER_SIZE" "$run_dir"

    leader_id=$(wait_for_leader "$run_dir")
    leader_port=$(awk -F'\t' -v id="$leader_id" '$1==id {print $3}' "$run_dir/manifest.tsv")
    follower_port=$(follower_http_port "$run_dir")

    for ((i = 1; i <= ITERATIONS; i++)); do
        unique_id=$((run * 100000 + 10000 + i))
        write_time=$(date +%s.%N)
        curl -s -m5 -X POST "http://localhost:$leader_port/" \
            -d "{\"queryType\":\"INSERT\",\"tableName\":\"bench\",\"attributes\":[],\"values\":[$unique_id,$i]}" \
            > /dev/null

        found=0
        attempts=0
        while [ "$found" -eq 0 ]; do
            attempts=$((attempts + 1))
            resp=$(curl -s -m1 -X POST "http://localhost:$follower_port/" \
                -d '{"queryType":"SELECT","tableName":"bench","attributes":[]}')
            if echo "$resp" | grep -Eq "\"id\":[[:space:]]*$unique_id"; then
                found=1
            elif [ "$attempts" -gt 50 ]; then
                echo "warning: id $unique_id never appeared on the follower within 5s" >&2
                break
            else
                sleep 0.1
            fi
        done
        read_time=$(date +%s.%N)
        staleness_ms=$(echo "($read_time - $write_time) * 1000" | bc)
        printf "%d\t%d\t%.1f\n" "$run" "$i" "$staleness_ms" >> "$OUT_CSV"
    done

    stop_cluster "$run_dir"
done

echo
echo "Results written to $OUT_CSV"
column -t -s $'\t' "$OUT_CSV"
echo
awk -F'\t' 'NR>1 {sum+=$3; n++} END {if (n>0) printf "Mean staleness: %.1f ms over %d writes across all runs\n", sum/n, n}' "$OUT_CSV"
