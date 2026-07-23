#!/usr/bin/env bash
# Runs all 5 benchmarks in sequence.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

./01-throughput-sweep.sh
./02-latency-distribution.sh
./03-failover-recovery.sh
./04-cluster-size-throughput.sh
./05-read-your-writes.sh

echo
echo "All benchmarks finished. Results in $(cd results && pwd)/"
