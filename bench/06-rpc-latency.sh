#!/usr/bin/env bash
# Benchmark 6: RPC round-trip latency between Raft nodes, at the networking
# protocol level (networking/rpc.c), not the client-facing HTTP API that
# benchmarks 1/2/4/5 measure.
#
# The leader tags every AppendEntries it sends with its own monotonic clock
# reading (Msg's appendEntries.sentAtNs); the follower echoes that value back
# unchanged in AppendEntriesResponse. The leader then diffs it against its
# own clock on receipt (raft/callbacks.c's handleAppendEntriesResponse),
# so the round-trip time is measured entirely on one node's clock - no
# cross-node clock sync required. Two further diffs split that total into
# where the time actually goes: `dequeuedAtNs` (worker.c's runNodeWorker,
# taken the instant the response is popped off its peer's job queue) versus
# `sentAtNs` gives queue/network time, and versus the moment raftNodeLock is
# actually acquired in handleAppendEntriesResponse gives leader-side lock
# wait. Each measurement is logged as "RPC_LATENCY peer=<id>
# rpc=append_entries numEntries=<n> latencyUs=<v> queueUs=<v>
# lockWaitUs=<v>" to stderr, landing in the leader's node<id>.log.
#
# Each repeat runs two back-to-back phases on the same cluster, so both are
# measured under identical conditions: `idle` (no client traffic - every
# AppendEntries is a numEntries=0 heartbeat) and `writeLoad` (WRITE_CONCURRENCY
# background clients hammering the leader with INSERTs - AppendEntries mostly
# carry real entries). Comparing the two is the point: heartbeat latency
# alone doesn't tell you what a client actually experiences when a write has
# to make its way through this same RPC to reach a majority.
#
# Swept across CLUSTER_SIZES (default "3 5"), each sampled BENCH_REPEATS
# times (default 5, override via env) against a fresh cluster per repeat, for
# BENCH_DURATION seconds (default 10) per phase per repeat.
#
# Two settle delays: SETTLE_S after leader election, before sampling starts,
# lets connections/threads finish basic warm-up; COOLDOWN_S after tearing a
# cluster down, before the next repeat rebinds the same ports, avoids
# TIME_WAIT/rebind noise from starting a fresh cluster on the previous one's
# still-settling sockets.
#
# SETTLE_S is deliberately modest, not a fix for the idle phase's real
# behavior: an idle leader's AppendEntriesResponses occasionally get stuck
# behind other traffic on the same per-peer job queue (networking/worker.c -
# see raft.c's MAIN_THREAD_SLEEP_US comment), and in repeated local
# diagnostics this isn't a one-time startup transient that a longer settle
# avoids - it's bistable. A given repeat either stays clean the whole time
# it runs, or at some point (observed anywhere from a few seconds in to
# well past 15s) locks into a persistent degraded state - once locked, it
# stays there, it doesn't self-recover - with mean latency roughly 15-20x
# higher. No fixed SETTLE_S reliably dodges this since the lock can occur
# at any time, including after sampling has already started. What *is*
# stable regardless of lock state is the per-sample median (the notebook
# leads with p50, not mean, for this reason) - about half of a "locked"
# repeat's samples are still fast; it's specifically the slow half that
# gets far slower, dragging the mean up without moving the median much.
# Read `fracOver1ms` as "how often did this repeat's tail get stuck", not
# as durable per-cluster-size architecture, since it's dominated by how
# many of BENCH_REPEATS happened to lock in that particular run.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib/common.sh"

CLUSTER_SIZES=(${1:-3 5})
WRITE_CONCURRENCY="${2:-10}"
DURATION="${BENCH_DURATION:-10}"
REPEATS="${BENCH_REPEATS:-5}"
SETTLE_S=3
COOLDOWN_S=2

OUT_CSV="$RESULTS_DIR/rpc-latency.csv"
echo -e "run\tclusterSize\tscenario\tpeer\tnumEntries\tlatencyUs\tqueueUs\tlockWaitUs" > "$OUT_CSV"

# Extracts RPC_LATENCY lines in (startLine, endLine] from leader_log into
# OUT_CSV, tagged with the given scenario.
harvest() {
    local leader_log="$1" start_line="$2" end_line="$3" run="$4" size="$5" scenario="$6"
    sed -n "$((start_line + 1)),${end_line}p" "$leader_log" | grep "RPC_LATENCY" | \
        awk -v run="$run" -v cs="$size" -v scenario="$scenario" '
            { split($2, p, "="); split($4, ne, "="); split($5, l, "=");
              split($6, q, "="); split($7, lw, "=");
              printf "%d\t%d\t%s\t%d\t%d\t%s\t%s\t%s\n",
                     run, cs, scenario, p[2], ne[2], l[2], q[2], lw[2] }
        ' >> "$OUT_CSV"
}

# Runs WRITE_CONCURRENCY background INSERT workers against the leader until
# DURATION seconds have elapsed, so the leader has a steady stream of
# non-empty AppendEntries to replicate for that whole window. Blocks until
# they're done (same pattern as 02-latency-distribution.sh's worker pool),
# so the caller doesn't need to separately time/kill anything.
run_write_load() {
    local leader_port="$1"
    local end_time="$2"

    for ((w = 1; w <= WRITE_CONCURRENCY; w++)); do
        (
            local i=0
            local id_base=$((w * 10000000))
            while [ "$(date +%s)" -lt "$end_time" ]; do
                i=$((i + 1))
                curl -s -m5 -X POST "http://localhost:$leader_port/" \
                    -d "{\"queryType\":\"INSERT\",\"tableName\":\"bench\",\"attributes\":[],\"values\":[$((id_base + i)),$i]}" \
                    > /dev/null
            done
        ) &
    done
    wait
}

for size in "${CLUSTER_SIZES[@]}"; do
    run_dir="$RUN_ROOT/rpc-latency-n$size"

    for ((run = 1; run <= REPEATS; run++)); do
        echo "== clusterSize=$size run $run/$REPEATS =="
        start_cluster "$size" "$run_dir"

        leader_id=$(wait_for_leader "$run_dir")
        leader_log="$run_dir/node$leader_id.log"
        IFS=$'\t' read -r leader_port _ <<< "$(leader_endpoint "$run_dir")"

        sleep "$SETTLE_S"

        idle_start=$(wc -l < "$leader_log")
        sleep "$DURATION"
        idle_end=$(wc -l < "$leader_log")
        harvest "$leader_log" "$idle_start" "$idle_end" "$run" "$size" "idle"

        write_start=$idle_end
        run_write_load "$leader_port" "$(($(date +%s) + DURATION))"
        write_end=$(wc -l < "$leader_log")
        harvest "$leader_log" "$write_start" "$write_end" "$run" "$size" "writeLoad"

        stop_cluster "$run_dir"
        sleep "$COOLDOWN_S"
    done
done

echo
echo "Results written to $OUT_CSV"
awk -F'\t' 'NR>1 {n[$3]++; sum[$3]+=$6} END {for (s in n) printf "%s: captured %d RPC round trips, mean latency %.2f us\n", s, n[s], sum[s]/n[s]}' "$OUT_CSV"
