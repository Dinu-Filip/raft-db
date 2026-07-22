# Benchmark harness

Scripts to measure the 5 benchmarks below against a headless, disk-backed
cluster of `databasenode` processes. All state lives under `bench/.run/`
(scratch, gitignored) and results under `bench/results/` (gitignored).

## Prerequisites

- `cmake --build build/debug --target databasenode` (or `build/release`,
  edit `NODE_BIN` in `lib/common.sh` if using a release build)
- `wrk` (`sudo apt-get install wrk`) for benchmarks 1/4
- `curl`, `awk`, `bc`, `column` (standard on most Linux systems)

## Usage

```
./bench/run-all.sh                    # everything, in sequence
./bench/01-throughput-sweep.sh        # req/s vs concurrency, N=1 and N=3
./bench/02-latency-distribution.sh [concurrency] [clusterSize]
./bench/03-failover-recovery.sh [clusterSize]
./bench/04-cluster-size-throughput.sh [concurrency]
./bench/05-read-your-writes.sh [clusterSize] [iterations]
```

Each script starts its own cluster in `bench/.run/<name>/` and tears it down
on completion. Results append to CSVs in `bench/results/`.

## Repeated trials

Every benchmark repeats each configuration `BENCH_REPEATS` times (default 5
for benchmarks 1/2/3/4, 3 for benchmark 5, since it restarts a whole cluster
per repeat and each repeat already contains multiple write/read iterations)
and writes one CSV row per repeat, tagged with a `run` column. Benchmarks
2, 3 and 5 restart the cluster fresh for every repeat so trials are
independent; 1/4 re-run `wrk` against the same already-started cluster,
since only the load generator (not cluster state) needs to vary between
repeats. Override with e.g. `BENCH_REPEATS=10 ./bench/01-throughput-sweep.sh`.
The notebooks use the `run` column to plot mean +/- stddev instead of a
single (possibly noisy) sample per configuration - except benchmark 2, which
records every individual request's latency (see below) and is plotted as a
histogram instead.

## Plotting results

Each benchmark has a matching notebook in `bench/notebooks/` that reads its
CSV from `bench/results/` and plots it with matplotlib.

```
cd bench/notebooks
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
jupyter notebook   # open 01_throughput_sweep.ipynb etc.
```

## What each one measures

1. **Throughput sweep** — `wrk` write load at concurrency 1/10/50/100,
   single-node vs a 3-node cluster. Look at where `reqPerSec` stops
   climbing >5% between steps — that plateau, not the peak, is the number
   that matters.
2. **Latency distribution** — write latency at a fixed concurrency (pass the
   plateau from #1), recording every single request's latency rather than
   an aggregate summary. Driven by a pool of `curl` workers timed directly
   with wall-clock `date` calls, not `wrk` - `wrk`'s Lua scripting exposes a
   raw per-request histogram API (`latency(i)`), but it's undocumented
   beyond one line in wrk's own `SCRIPTING` doc and didn't behave sanely
   when probed directly, so it wasn't trustworthy to build this on. The
   notebook plots the pooled per-request values as a histogram and reports
   p50/p90/p99 as numbers.
3. **Failover recovery** — `kill -9`s the leader mid-run and times from the
   kill to the first successful write against the new leader.
4. **Cluster size vs throughput** — the same sweep at N=3 and N=5 to show
   the replication-cost tradeoff.
5. **Read-your-writes** — writes a uniquely-valued row to the leader, then
   polls a follower's local `SELECT` until it appears, recording the
   staleness window.

## Known limitations (not fixed, routed around instead)

- **`CREATE_TABLE` is not replicated over the network protocol.**
  `networking/msg.h`'s `OPERATION` wire (de)serialization macro has no
  `case CREATE_TABLE` (only commented out) — encoding or decoding a
  `CREATE_TABLE` log entry over AppendEntries silently fails. Since schema
  creation isn't part of what these benchmarks measure, `lib/common.sh`'s
  `bootstrap_schema` works around it by creating the `bench` table (`id
  INT, val INT`) via a disposable single-node instance and seeding every
  cluster node's data directory with the resulting files before startup,
  instead of sending `CREATE_TABLE` through the real cluster.
- **Conditioned `SELECT`/`UPDATE`/`DELETE` corrupt the heap.**
  `parseTwoArgCondition`/`parseOneArgCondition` (`client-handling/input.c`)
  dereference an `Operand` they never allocate. None of these 5 benchmarks
  need a `WHERE` clause, so benchmark 5 uses a full-table `SELECT` and
  filters client-side instead of fixing this.

## Core changes this harness relies on

A few pre-existing bugs made every one of these benchmarks impossible to
run at all, and were fixed directly in `src/`:

- `main.c` was dead scratch code that never called `start()`; `databasenode`
  couldn't run a cluster before this fix.
- `DB_BASE_DIRECTORY` was a hardcoded `../../../raft-db`, unrelated to the
  `raft-db/<id>/data` directories `start.c` actually creates, and ignored
  node id entirely — every node's storage collided on the same file.
- `parseQueryTypes` (`client-handling/input.c`) stored raw `AttributeType`
  ints where `QueryTypeDescriptor*` pointers were expected, so any
  `CREATE_TABLE` request segfaulted on execution.

Separately, write acknowledgement was changed from "leader appended the
entry to its own log" to "a majority has committed it" — deferred via a
pending-writes queue flushed each poll iteration in
`client-handling/server.c`, so it doesn't block the HTTP event loop while
waiting. This is what makes benchmark 2's tail latencies reflect real
replication stalls instead of a no-op leader append, and what makes
benchmark 5's staleness window meaningful.

The first round of benchmark numbers exposed a ~20ms latency floor on every
write, present even at concurrency 1 with an idle cluster. The cause:
`client-handling/server.c`'s event loop ran `mg_mgr_poll(&mgr, 20)` in a
blind `for(;;)` loop and only checked the pending-writes queue once per
poll tick, and the leader only recomputed its commit index from `raft.c`'s
own periodic 5ms tick — so a write's HTTP reply was gated on two unrelated
timers instead of the actual commit event. Fixed by making both event-driven:
`raft/callbacks.c`'s `handleAppendEntriesResponse` now calls
`updateCommitIndex()` immediately after a majority-worthy response arrives
(previously only `raft.c`'s tick did), and `raft-node.c`'s `setCommitIndex`
calls a registered listener (`client-handling/server.c`'s
`notifyWriteProgress`) whenever it actually advances, which uses mongoose's
`mg_wakeup()` to interrupt `mg_mgr_poll` immediately instead of waiting out
its timeout. `mg_mgr_poll`'s timeout is now just a 250ms backstop for cases
with no further raft traffic to trigger a wakeup (e.g. noticing a lost-
leadership or command-timeout error). This dropped p50 write latency at
concurrency 1 from ~20ms to ~2.5ms in local testing. Also fixed while in
there: the pending-writes table (`MAX_PENDING_WRITES = 4096`) used to
`assert()`-crash the whole node if it filled up under sustained overload;
it now replies to the client with an overload error instead.
