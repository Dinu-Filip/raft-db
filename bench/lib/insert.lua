-- wrk load-generator script: fires unique INSERT bodies against the "bench"
-- table (created by lib/common.sh's bootstrap_schema) and, on completion,
-- prints exact p50/p95/p99 latency in milliseconds.

local nextThreadId = 0
local counter = 0

wrk.method = "POST"
wrk.headers["Content-Type"] = "application/json"

function setup(thread)
    thread:set("id", nextThreadId)
    nextThreadId = nextThreadId + 1
end

function request()
    counter = counter + 1
    local uniqueId = id * 10000000 + counter
    wrk.body = string.format(
        '{"queryType":"INSERT","tableName":"bench","attributes":[],"values":[%d,%d]}',
        uniqueId, counter)
    return wrk.format()
end

function done(summary, latency, requests)
    io.write(string.format("P50_MS=%.2f\n", latency:percentile(50) / 1000))
    io.write(string.format("P95_MS=%.2f\n", latency:percentile(95) / 1000))
    io.write(string.format("P99_MS=%.2f\n", latency:percentile(99) / 1000))
end
