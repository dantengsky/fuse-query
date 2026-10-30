#!/usr/bin/env bash
# Regression test for aggregate state leaks (upstream #19543/#19544 and #20145/#20146).
# - DISTINCT aggregates over nullable arguments re-initialized their state during merge
#   and leaked the previous distinct set once per group.
# - min/max/any over heap-backed types (Array, Binary, Bitmap, ...) never dropped their state.
# Each query is run repeatedly; jemalloc allocated bytes must not keep growing.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

allocated() {
	# system.malloc_stats_totals returns one row per node in cluster mode; sum them.
	echo "SELECT sum(value) FROM system.malloc_stats_totals WHERE name LIKE 'stats.allocated%'" | $BENDSQL_CLIENT_CONNECT
}

check() {
	local name=$1
	local query=$2
	# Warm up to reduce noise from one-time allocations.
	for i in $(seq 1 2); do
		echo "$query" | $BENDSQL_CLIENT_CONNECT
	done
	sleep 2
	local before
	before=$(allocated)
	for i in $(seq 1 3); do
		echo "$query" | $BENDSQL_CLIENT_CONNECT
	done
	sleep 2
	local after
	after=$(allocated)
	if ! [[ "$before" =~ ^[0-9]+$ && "$after" =~ ^[0-9]+$ ]]; then
		echo "$name: failed to read allocated bytes (before='$before', after='$after')"
		return
	fi
	local diff_mib=$(((after - before) / 1048576))
	if [ "$diff_mib" -lt 64 ]; then
		echo "$name: allocated growth is less than 64 MiB"
	else
		echo "$name: allocated grew by ${diff_mib} MiB"
	fi
}

check "string_agg distinct nullable" "SELECT /*+ set_var(max_threads=16) */ (number % 400000)::string AS k, STRING_AGG(DISTINCT if(number % 7 = 0, NULL, concat('wm_name_', (number % 97)::string, '_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'))) FROM numbers_mt(1600000) GROUP BY 1 IGNORE_RESULT"

check "array_agg distinct nullable" "SELECT /*+ set_var(max_threads=16) */ (number % 400000)::string AS k, ARRAY_AGG(DISTINCT if(number % 7 = 0, NULL, concat('event_', (number % 5)::string))) FROM numbers_mt(1600000) GROUP BY 1 IGNORE_RESULT"

check "sum distinct nullable" "SELECT /*+ set_var(max_threads=16) */ number % 400000 AS k, SUM(DISTINCT if(number % 7 = 0, NULL, number % 1000)) FROM numbers_mt(1600000) GROUP BY 1 IGNORE_RESULT"

check "max array" "SELECT /*+ set_var(max_threads=16) */ number % 400000 AS k, MAX([number % 1000, number % 997]) FROM numbers_mt(1600000) GROUP BY 1 IGNORE_RESULT"

check "min binary" "SELECT /*+ set_var(max_threads=16) */ number % 400000 AS k, MIN(to_binary(concat('bin_', (number % 1000)::string))) FROM numbers_mt(1600000) GROUP BY 1 IGNORE_RESULT"

check "intersect_count bitmap" "SELECT /*+ set_var(max_threads=16) */ number % 400000 AS k, intersect_count(1, 2)(build_bitmap([number % 1000]), number % 3) FROM numbers_mt(1600000) GROUP BY 1 IGNORE_RESULT"
