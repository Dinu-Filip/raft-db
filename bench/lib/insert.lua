-- wrk load-generator script: fires unique INSERT bodies against the "bench"
-- table (created by lib/common.sh's bootstrap_schema) and, on completion,
-- prints exact p50/p95/p99 latency in milliseconds plus an error count.

local threads = {}
local nextThreadId = 0
local counter = 0

wrk.method = "POST"
wrk.headers["Content-Type"] = "application/json"

function setup(thread)
    thread:set("id", nextThreadId)
    nextThreadId = nextThreadId + 1
    thread:set("errors", 0)
    table.insert(threads, thread)
end

function request()
    counter = counter + 1
    local uniqueId = id * 10000000 + counter
    wrk.body = string.format(
        '{"queryType":"INSERT","tableName":"bench","attributes":[],"values":[%d,%d]}',
        uniqueId, counter)
    return wrk.format()
end

-- A non-committed write (stale leader after a mid-run election, follower
-- rejection, pending-write table full, commit timeout) still gets a fast
-- 200 OK with an "error" body - without checking for it, those near-instant
-- rejections would masquerade as extra throughput instead of showing up as
-- failures.
function response(status, headers, body)
    if status ~= 200 or not body:find('"success"', 1, true) then
        errors = errors + 1
    end
end

function done(summary, latency, requests)
    local totalErrors = 0
    for _, thread in ipairs(threads) do
        totalErrors = totalErrors + (thread:get("errors") or 0)
    end
    io.write(string.format("P50_MS=%.2f\n", latency:percentile(50) / 1000))
    io.write(string.format("P95_MS=%.2f\n", latency:percentile(95) / 1000))
    io.write(string.format("P99_MS=%.2f\n", latency:percentile(99) / 1000))
    io.write(string.format("ERRORS=%d\n", totalErrors))
end
