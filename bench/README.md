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
./bench/06-rpc-latency.sh ["clusterSizes"] [writeConcurrency]  # e.g. "3 5 7" 20, default "3 5" 10
```

Each script starts its own cluster in `bench/.run/<name>/` and tears it down
on completion. Results append to CSVs in `bench/results/`.

## Repeated trials

Every benchmark repeats each configuration `BENCH_REPEATS` times (default 5
for benchmarks 1/2/3/4/6, 3 for benchmark 5, since it restarts a whole cluster
per repeat and each repeat already contains multiple write/read iterations)
and writes one CSV row per repeat, tagged with a `run` column. Benchmarks
2, 3, 5 and 6 restart the cluster fresh for every repeat so trials are
independent; 1/4 re-run `wrk` against the same already-started cluster,
since only the load generator (not cluster state) needs to vary between
repeats. Override with e.g. `BENCH_REPEATS=10 ./bench/01-throughput-sweep.sh`.
The notebooks use the `run` column to plot mean +/- stddev instead of a
single (possibly noisy) sample per configuration - except benchmarks 2 and 6,
which record every individual request's/RPC's latency (see below) and are
plotted as a histogram instead.

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
6. **RPC latency** — round-trip time of the peer-to-peer AppendEntries RPC
   (`networking/rpc.c`), not the client-facing HTTP API the other 5
   benchmarks measure. Every AppendEntries the leader sends carries its own
   monotonic clock reading; the follower echoes it back unchanged in
   AppendEntriesResponse, so the leader can compute round-trip time against
   its own clock alone (see "Core changes" below) - no cross-node clock sync
   needed. Each repeat runs two phases on the same cluster, tagged by a
   `scenario` column: `idle` (no client traffic, every AppendEntries is an
   empty heartbeat) and `writeLoad` (background clients hammering the leader
   with INSERTs, so AppendEntries mostly carry real entries - `numEntries`
   is logged per sample too). Idle-only latency undersells what a client
   actually experiences, since a write has to travel this same RPC to reach
   a majority; comparing the two phases directly shows how much of the idle
   number is heartbeat-queue noise versus what real replication costs. Each
   RPC's latency is further split into `queueUs` (network transit +
   follower-side processing + this leader's own per-peer job-queue wait) and
   `lockWaitUs` (time spent specifically waiting on `raftNodeLock` once the
   response reaches the front of that queue), to separate raft-level lock
   contention from everything upstream of it. Swept across cluster sizes
   (default N=3 and N=5) to show how replication fan-out affects per-peer
   RPC latency.

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
  dereference an `Operand` they never allocate. None of these 6 benchmarks
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

For benchmark 6, the wire protocol's `AppendEntries`/`AppendEntriesResponse`
messages (`networking/msg.h`) gained a `sentAtNs` field - the leader's own
`CLOCK_MONOTONIC` reading (`utils.c`'s `monotonicNs()`) at send time, echoed
back unchanged by the follower. `raft/callbacks.c`'s
`handleAppendEntriesResponse` diffs it against a fresh `monotonicNs()` call
and logs the result as `RPC_LATENCY peer=<id> rpc=append_entries
numEntries=<n> latencyUs=<v> queueUs=<v> lockWaitUs=<v>`, which the benchmark
script greps out of the leader's log. This piggybacks on every AppendEntries
the cluster already sends (heartbeats included), rather than adding a
separate ping/pong RPC, so the measured latency is exactly what real Raft
traffic experiences. `queueUs`/`lockWaitUs` come from a second timestamp
(`networking/worker.c`'s `runNodeWorker`, taken the instant a response is
popped off its peer's job queue, threaded through `execute()` into
`handleAppendEntriesResponse`): `queueUs` is that dequeue time minus
`sentAtNs`, `lockWaitUs` is the time from dequeue to actually acquiring
`raftNodeLock` just after.

An early idle-cluster run of benchmark 6 (at the original 5ms `raftMain`
tick, see below) showed round-trip latency was clearly bimodal - most
AppendEntries around 400-500us, but 27% (N=3) to 42% (N=5) of them instead
landing around 5.5-5.8ms, suspiciously close to one tick period. Splitting
`queueUs` from `lockWaitUs` on those slow samples showed the delay was
almost entirely `queueUs` (`lockWaitUs` stayed under a few us even in the
slow mode) - so it wasn't `raftNodeLock` contention on the leader, it was
somewhere in the network/queue path.

The actual cause: every `NetworkNode` has exactly one job queue and one
worker thread (`networking/worker.c`), shared between outbound sends to that
peer and processing of inbound messages from that peer. At a 5ms tick, the
leader enqueues a heartbeat `SEND` job to every peer's queue that often,
competing with `EXECUTE` jobs for that peer's `AppendEntriesResponse`s on
the same FIFO - frequently enough that a meaningful fraction of responses
get stuck behind heartbeat traffic before they're even dequeued.
`raft.c`'s `MAIN_THREAD_SLEEP_US` (the tick period) was set to 5000 (5ms)
despite nothing actually requiring that cadence: log propagation
(`sendAllAppendEntries` is called directly from `leaderHandleClientRequest`
on every write) and commit-index advancement (`updateCommitIndex` from
`handleAppendEntriesResponse` on every majority-worthy response, see above)
are both already event-driven, so the tick's only remaining jobs are
heartbeat keepalive and checking `shouldCallElection()` - both only need to
run comfortably faster than `RANDOM_ELECTION_TIME_MIN` (150ms,
`elections.c`), not at 5ms. A quick single-trial comparison at 5/10/15/20ms
showed the tail shrinking as the tick lengthened (5ms: p90 ~5.6ms; 10ms:
p90 ~0.5ms; 20ms: p90 ~0.6ms), so `MAIN_THREAD_SLEEP_US` was changed to
15000 (15ms) - still a ~10x margin under the election timeout floor,
matching the usual heartbeat:electionTimeout ratio.

That single-trial comparison undersold how this actually behaves, though.
Repeated, longer idle-only runs at a fixed tick value show the queue
contention isn't a smooth function of load or tick period at all - it's
**bistable**. A given idle run either stays clean the entire time (mean
~350-500us) or, at some point during the run - not necessarily near the
start - locks into a persistent degraded state (mean ~3-8ms) that does not
self-recover once entered. Across repeated 5-repeat runs of benchmark 6 at
the 15ms tick, anywhere from 1/10 to 4/10 idle repeats landed in the locked
state in a given run of the script; the other repeats stayed clean the
whole way through. Because roughly half of even a "locked" repeat's samples
are still fast, the per-sample **median** stays low (roughly 300-700us)
regardless of lock state - it's specifically the slow half of a locked
repeat that gets much slower, which is what makes `fracOver1ms`/mean read
as "how often did the tail get stuck this run" rather than a stable
architectural constant. `bench/notebooks/06_rpc_latency.ipynb` shows the
per-repeat breakdown directly. Working theory at the time: the
strictly-periodic heartbeat gradually phase-locks against
network/thread-scheduling timing on the shared per-peer job queue
(`networking/worker.c` - one queue and one worker thread per peer, shared
between outbound sends and inbound response processing), and irregular
write-triggered sends break that phase relationship, consistent with
`writeLoad` samples not showing the same bimodality. Lengthening the tick
reduces how often the lock happens but doesn't eliminate it, and no settle
delay reliably dodges it either, since it can occur well after sampling
starts.

That theory was tested directly: each `NetworkNode` (`rpc.h`) now has
separate `sendQueue`/`sendWorkerThread` and `executeQueue`/
`executeWorkerThread` instead of one shared queue/thread
(`initialiseRpc` in `rpc.c` spawns both; `worker.c`'s `runSendWorker`/
`runExecuteWorker` each drain only their own queue), so a peer's outbound
heartbeat traffic can no longer sit in front of that same peer's inbound
response processing in one FIFO. **Result: the split reduces the lock's
severity but does not eliminate it.** A 10-repeat run after the split
still had 2/10 idle repeats lock (both at N=5) - not a clear improvement
in *frequency* over the 1/10-4/10 range seen before the split, at this
sample size - but those locked repeats plateaued at 14%/28% of samples
over 1ms, versus a consistent ~49.5% every time it locked pre-split. So
the shared queue was a real contributor to how bad the lock gets, but not
the root cause of the lock occurring at all. Since no socket anywhere in
this codebase sets `TCP_NODELAY`, Nagle's algorithm is active on every RPC
connection, which is a classic source of exactly this kind of
periodic-traffic pathology and the next thing worth checking - but
confirming that (or another OS/TCP-level explanation) needs its own
dedicated investigation, out of scope here. The split was kept despite not
being a full fix: it's a real (if partial) improvement, is arguably
cleaner separation of concerns on its own merits, and the extra
thread-per-peer cost is modest.
