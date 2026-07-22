# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A fault-tolerant distributed DBMS in C17, implementing the RAFT consensus algorithm (per the Ongaro/Ousterhout paper) over a cluster of nodes, plus a from-scratch paged-file database engine and its own binary networking/RPC protocol. Follows a BASE (not ACID) consistency model — followers may serve stale data until the log converges. Clients query the cluster over HTTP with JSON bodies.

## Build

```sh
make            # cmake --preset debug && cmake --build --preset debug   -> build/debug
make release    # -Werror build                                          -> build/release
make clean
```

Binaries land in `build/debug/src/distributed-database/`:
- `databasenode` — the real node executable (entry point `main.c` -> `start.c`)
- `dbtest` — ad-hoc test binary (see Testing below)
- `networkingtest` — networking layer test binary (`networking/networking-test.c`)

`src/lib` builds as a static library (`lib`) that `distributed-database` links against.

## Running a cluster

```sh
./run <NODE_COUNT> [BASE_RPC_PORT=5000] [BASE_SERVER_PORT=4000] [EXEC=./build/debug/src/distributed-database/databasenode]
```

Spins up `NODE_COUNT` nodes in a tmux session (one pane each), wiring their RPC addresses together. Each node process is invoked as:

```
databasenode <CLIENT_HANDLING_PORT> <NODE_COUNT> <ADDR_0> ... <ADDR_{k-1}> [<RPC_PORT>]
```

where the addresses are `ip:port` of already-started peers with lower ids; the last node in the cluster has no trailing RPC port (it doesn't need to accept new peer connections). Each node persists its RAFT log/state under `raft-db/<nodeId>/`.

## Testing

There is no test runner/framework — tests are individual `void testX(void)` functions (one per file) under `src/distributed-database/test/{index,operations,sql,table}/`, using the light assertion macros in `src/lib/test-library.h` (`ASSERT_EQ`, `ASSERT_STR_EQ`, `START_OUTER_TEST`/`FINISH_OUTER_TEST`, `PRINT_SUMMARY`).

To run one or more tests: include the relevant test header(s) in `src/distributed-database/main.c` and call the test function(s) from `main()`, then `make && ./build/debug/src/distributed-database/databasenode`. (The `dbtest` target links every non-`main.c`/non-`*-test.c` source but does not itself call any tests — `main.c` is the actual driver, so whichever test calls are currently wired into it are "what runs".)

`networkingtest` (`src/distributed-database/networking/networking-test.c`) exercises the networking layer standalone in the same way.

## Formatting / lint (CI-enforced)

```sh
make format           # clang-format -i over all src/**/*.[ch] (Google style, see .clang-format)
make format-newline   # every file under src/ must end with a trailing newline
make iwyu             # include-what-you-use check (uses iwyu.imp mapping file)
```

Note: `.gitlab-ci.yml`, top-level `Makefile` targets `valgrind*`/`build-all`/`build-led-blink` and the top-level project name `armv8` are leftover from a template/prior assignment repo (assembler/emulator) this project was bootstrapped from — they don't apply to the database code. The `format`, `format-newline`, and `iwyu` targets/CI stages are the ones that are live and apply to `src/`.

## Architecture

Everything lives under `src/distributed-database/`, split into four layers plus a shared `src/lib`:

**`raft/`** — consensus core. `raft-node.h` defines the global `RaftNode node` singleton (term, votedFor, log, commit/applied indices, next/matchIndex per follower) guarded by `raftNodeLock`; all state mutations go through its setters (`setCurrentTerm`, `setVotedFor`, `setCommitIndex`, ...) which also persist to disk via `persistent-store.h`. `raft.c`/`elections.c` drive the election timeout and AppendEntries loop (`runRaftMain`, run on its own thread from `start.c`); `callbacks.c` handles inbound RPC responses; `log-table.h`/`log-entry.h` model the replicated log.

**`networking/`** — hand-rolled binary RPC protocol between nodes (distinct from the client-facing HTTP API). `msg.h` defines the wire message struct and `PARSE_*`/encode macros for a `ReadBuff`. `rpc.c` owns each peer as a `NetworkNode` (socket, per-node thread, mutex) and does connect/listen/send/receive. `worker.c` decouples network I/O from processing: `queueSend`/`queueSendAll` and `queueExecute` push onto queues drained by `runWorker` (the main thread), which calls into `execute.c` to apply an incoming `Msg` (RAFT RPCs, log entries, etc.) — this keeps all RAFT/table state mutation on one thread even though sends/receives happen on per-peer threads.

**`client-handling/`** — the external-facing HTTP+JSON API (via vendored mongoose, `lib/third-party/`). `server.c` runs `runClientHandlingServer` on its own thread; `input.c` parses a request body into an `Operation` (`parseOperationJson`) and serializes a `QueryResult` back to JSON (`queryResultStringify`).

**`table/`** — the storage/query engine, independent of networking/raft:
- `core/` — the paged file format: `table.c` (table header, page size 4KB, one `.rfdb` file per table, `openTable`/`initialiseTable`), `pages.c` (slotted-page layout: `RecordSlotArray`, free-space tracking, defragmentation), `record.c`/`field.c` (fixed- and variable-length field encoding within a record), `recordArray.c` (iteration/result sets).
- `index/b+-tree.c` — a B+-tree secondary index (`Index`/`Node`), keyed by `KeyId` (supports a repeated-key case disambiguated by a global id, see `GLOBAL_ID_WIDTH`/`GLOBAL_ID_NAME`), stored as its own paged file alongside table data.
- `operations/` — one file per SQL-ish verb (`insert.c`, `select.c`, `update.c`, `delete.c`, `createTable.c`) operating on the `Operation`/`Condition`/`QueryResult` types from `operation.h`; `sqlToOperation.c` parses a SQL string into an `Operation`, which is also the struct produced by `client-handling/input.c` from JSON and the struct that gets serialized into RAFT log entries for replication.
- `conditions.c`, `schema.c`, `db-utils.c` — WHERE-clause evaluation, table schema (attribute name/type/size) storage, and shared path/file helpers.

**`start.c`** wires all four layers together per-process: parses CLI args into `(clientHandlingPort, nodeId, peer addresses, rpcPort)`, creates the `raft-db/<nodeId>/data` directory, then starts (each on its own thread) the RPC client/server, the client-handling HTTP server, the RAFT main loop, and the ping thread, while running the `worker` loop on the main thread.

**`src/lib`** — reusable, project-agnostic utilities linked as a static lib: `hashmap.c`/`function-hashmap.c`, `int-list.c`, `queue.c`/`concurrent/queue.c` (thread-safe queue, used by `worker.c`), `io.c`, `utils.c`, `log.h` (the `LOG(...)` macro used throughout), plus vendored `third-party/cJSON` and `third-party/mongoose`.

Data flow for a client write: HTTP request -> `client-handling` parses JSON to `Operation` -> proposed as a RAFT log entry by the leader -> replicated via `networking`/`raft` AppendEntries -> once committed, applied to the paged `table/` files on each node via `operations/`.
