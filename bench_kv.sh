#!/bin/bash

bench_driver=./target/release/databend-metabench
bench_log=bench.log
result_log=result.log

#meta_rpc_addr="127.0.0.1:9191,127.0.0.1:28302,127.0.0.1:28202"
#etcd_rpc_addr="http://127.0.0.1:2379,http://127.0.0.1:3379,http://127.0.0.1:4379"

meta_rpc_addr="127.0.0.1:9191"
etcd_rpc_addr="http://127.0.0.1:2379"

rm ${bench_log}
rm ${result_log}

function bench() {
	local rpc_action=$1
	local rpc_addr=$2
	local backend=$3
	local num_client=$4
	local steps=$5

	echo "Action: ${rpc_action}, address: ${rpc_addr}, backend {$backend}" | tee -a $result_log

	$bench_driver --grpc-api-address $rpc_addr \
		--log-level error \
		--client ${num_client} --number ${steps}\
		--rpc $rpc_action --backend $backend 2>&1 | tee -a $bench_log

	# last line is the metrics (or some error message)
	tail -n1 $bench_log >> $result_log
}

function scenario() {
	local rpc_action=$1
	local num_client=$2
	local steps=$3

	echo "Scenario : ${rpc_action}" | tee -a $result_log
	echo "" | tee -a $result_log

	for i in {1..2}
	do
		echo "Iteration $i:" | tee -a $result_log
		echo "=============" | tee -a $result_log
		bench $rpc_action $meta_rpc_addr "default" $num_client $steps
		bench $rpc_action $etcd_rpc_addr "etcd" $num_client $steps
	done
	echo "" | tee -a $result_log
}

scenario "upsert_kv" 500 10
scenario "get" 500 10
# shrink test scale, otherwise local disk will be full
scenario "table_copy_file:{\"file_cnt\":5000}" 10 100



#scenario "upsert_kv" 100 10
#scenario "get" 100 10

#scenario "upsert_kv" 10 10
#scenario "get" 10 10

