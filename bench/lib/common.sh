#!/usr/bin/env bash
# Shared helpers for the bench/ scripts: headless cluster lifecycle, leader
# discovery, and schema bootstrapping.

BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_ROOT="$(cd "$BENCH_DIR/.." && pwd)"
NODE_BIN="$REPO_ROOT/build/debug/src/distributed-database/databasenode"
RUN_ROOT="$BENCH_DIR/.run"
RESULTS_DIR="$BENCH_DIR/results"

BASE_HTTP_PORT=4000
BASE_RPC_PORT=5000

mkdir -p "$RESULTS_DIR"

require_binary() {
    if [ ! -x "$NODE_BIN" ]; then
        echo "error: $NODE_BIN not built. Run: cmake --build build/debug --target databasenode" >&2
        exit 1
    fi
}

# CREATE_TABLE is not replicated over the wire protocol (networking/msg.h's
# OPERATION (de)serialisation macro has no case for it), so every node's
# schema is seeded identically on disk up front instead of going through
# Raft. Only the "bench" table (id INT, val INT) used by these scripts is
# created this way.
bootstrap_schema() {
    local schema_dir="$1"
    rm -rf "$schema_dir"
    mkdir -p "$schema_dir"

    (
        cd "$schema_dir"
        nohup "$NODE_BIN" 4999 1 > boot.log 2>&1 &
        echo $! > boot.pid
    )

    for _ in $(seq 1 50); do
        leader=$(curl -s -m1 "http://localhost:4999/leader" 2>/dev/null || true)
        [[ "$leader" =~ ^[0-9]+$ ]] && break
        sleep 0.1
    done

    curl -s -m5 -X POST "http://localhost:4999/" \
        -d '{"queryType":"CREATE_TABLE","tableName":"bench","attributes":["id","val"],"types":["INT","INT"],"sizes":[4,4]}' \
        > /dev/null
    sleep 0.3

    kill -9 "$(cat "$schema_dir/boot.pid")" 2>/dev/null || true
    sleep 0.2
}

# Starts an N-node cluster headlessly in $run_dir. Writes a manifest.tsv of
# "nodeId<TAB>pid<TAB>httpPort<TAB>rpcPort" rows.
start_cluster() {
    local n="$1"
    local run_dir="$2"
    require_binary

    rm -rf "$run_dir"
    mkdir -p "$run_dir"

    local schema_dir="$RUN_ROOT/schema-bootstrap"
    bootstrap_schema "$schema_dir"

    : > "$run_dir/manifest.tsv"

    for ((id = 0; id < n; id++)); do
        mkdir -p "$run_dir/raft-db/$id/data"
        cp "$schema_dir/raft-db/0/data/"*.rfdb "$run_dir/raft-db/$id/data/"

        local http_port=$((BASE_HTTP_PORT + id))
        local rpc_port=$((BASE_RPC_PORT + id))
        local args=("$http_port" "$n")
        for ((peer = 0; peer < id; peer++)); do
            args+=("127.0.0.1:$((BASE_RPC_PORT + peer))")
        done
        if [ "$id" -ne $((n - 1)) ]; then
            args+=("$rpc_port")
        fi

        (
            cd "$run_dir"
            nohup "$NODE_BIN" "${args[@]}" > "node$id.log" 2>&1 &
            echo $! > "node$id.pid"
        )
        local pid
        pid=$(cat "$run_dir/node$id.pid")
        printf "%d\t%d\t%d\t%d\n" "$id" "$pid" "$http_port" "$rpc_port" >> "$run_dir/manifest.tsv"
    done

    wait_for_leader "$run_dir" 15 > /dev/null
}

stop_cluster() {
    local run_dir="$1"
    [ -f "$run_dir/manifest.tsv" ] || return 0
    while IFS=$'\t' read -r _ pid _ _; do
        kill -9 "$pid" 2>/dev/null || true
    done < "$run_dir/manifest.tsv"
}

# Prints the current leader's node id once one is known, polling until
# timeout_s elapses.
wait_for_leader() {
    local run_dir="$1"
    local timeout_s="${2:-15}"
    local deadline=$((SECONDS + timeout_s))
    while [ "$SECONDS" -lt "$deadline" ]; do
        while IFS=$'\t' read -r _ _ http_port _; do
            local leader
            leader=$(curl -s -m1 "http://localhost:$http_port/leader" 2>/dev/null || true)
            if [[ "$leader" =~ ^[0-9]+$ ]]; then
                echo "$leader"
                return 0
            fi
        done < "$run_dir/manifest.tsv"
        sleep 0.2
    done
    echo "error: no leader elected within ${timeout_s}s" >&2
    return 1
}

# Prints the current leader's HTTP port and pid as "port<TAB>pid".
leader_endpoint() {
    local run_dir="$1"
    local leader_id
    leader_id=$(wait_for_leader "$run_dir") || return 1
    awk -F'\t' -v id="$leader_id" '$1==id {print $3"\t"$2}' "$run_dir/manifest.tsv"
}

# Prints the HTTP port of a node that is not the current leader.
follower_http_port() {
    local run_dir="$1"
    local leader_id
    leader_id=$(wait_for_leader "$run_dir") || return 1
    awk -F'\t' -v id="$leader_id" '$1!=id {print $3; exit}' "$run_dir/manifest.tsv"
}

# Runs wrk at each given concurrency against the leader, BENCH_REPEATS times
# per concurrency (default 5, override via env), appending
# "clusterSize<TAB>concurrency<TAB>run<TAB>reqPerSec<TAB>p50Ms<TAB>p95Ms<TAB>p99Ms"
# rows to out_csv - one row per (concurrency, run) pair, 1-indexed. Duration
# per wrk invocation is BENCH_DURATION seconds (default 10). Repeats exist so
# the notebooks can plot mean +/- stddev instead of a single noisy sample.
run_throughput_sweep() {
    local run_dir="$1"
    local cluster_size="$2"
    local out_csv="$3"
    shift 3
    local concurrencies=("$@")
    local duration="${BENCH_DURATION:-10}"
    local repeats="${BENCH_REPEATS:-5}"

    local leader_id
    leader_id=$(wait_for_leader "$run_dir")
    local port
    port=$(awk -F'\t' -v id="$leader_id" '$1==id {print $3}' "$run_dir/manifest.tsv")

    for c in "${concurrencies[@]}"; do
        local threads=$c
        [ "$threads" -gt 8 ] && threads=8

        for ((r = 1; r <= repeats; r++)); do
            local out
            out=$(wrk -t"$threads" -c"$c" -d"${duration}s" --latency \
                -s "$BENCH_DIR/lib/insert.lua" "http://localhost:$port/")

            local reqs_per_sec p50 p95 p99
            reqs_per_sec=$(echo "$out" | awk '/Requests\/sec:/ {print $2}')
            p50=$(echo "$out" | awk -F= '/P50_MS=/ {print $2}')
            p95=$(echo "$out" | awk -F= '/P95_MS=/ {print $2}')
            p99=$(echo "$out" | awk -F= '/P99_MS=/ {print $2}')

            printf "%d\t%d\t%d\t%s\t%s\t%s\t%s\n" \
                "$cluster_size" "$c" "$r" "$reqs_per_sec" "$p50" "$p95" "$p99" >> "$out_csv"
        done
    done
}
