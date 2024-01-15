#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

# mariadb mysql client has some bug, please use mysql official client
# mysql --version
# mysql  Ver 8.0.32-0ubuntu0.20.04.2 for Linux on x86_64 ((Ubuntu))
echo "drop table if exists t1;" |  $BENDSQL_CLIENT_CONNECT
echo "drop stage if exists s1;" |  $BENDSQL_CLIENT_CONNECT
echo "drop stage if exists s0;" |  $BENDSQL_CLIENT_CONNECT

echo "CREATE TABLE t1(a Int, b bool) Engine = Fuse;" |  $BENDSQL_CLIENT_CONNECT

echo "INSERT /*+ SET_VAR(deduplicate_label='insert-test') */ INTO t1 (a, b) VALUES(1, false)" | $BENDSQL_CLIENT_CONNECT
echo "INSERT /*+ SET_VAR(deduplicate_label='insert-test') */ INTO t1 (a, b) VALUES(1, false)" | $BENDSQL_CLIENT_CONNECT
echo "select * from t1" | $BENDSQL_CLIENT_CONNECT

echo "CREATE STAGE s0;" | $BENDSQL_CLIENT_CONNECT
echo "copy /*+SET_VAR(deduplicate_label='copy-test')*/ into @s0 from (select * from t1);" | $MYSQL_CLINEENRT_CONNECT
echo "select * from @s0;" | $MYSQL_CLINEENRT_CONNECT
echo "CREATE STAGE s1;" | $MYSQL_CLINEENRT_CONNECT
echo "copy /*+SET_VAR(deduplicate_label='copy-test')*/ into @s1 from (select * from t1);" | $MYSQL_CLINEENRT_CONNECT
echo "select * from @s1;" | $MYSQL_CLINEENRT_CONNECT

echo "UPDATE /*+ SET_VAR(deduplicate_label='update-test') */ t1 SET a = 20 WHERE b = false;" | $BENDSQL_CLIENT_CONNECT
echo "UPDATE /*+ SET_VAR(deduplicate_label='update-test') */ t1 SET a = 30 WHERE b = false;" | $BENDSQL_CLIENT_CONNECT
echo "select * from t1" | $BENDSQL_CLIENT_CONNECT

echo "replace /*+ SET_VAR(deduplicate_label='replace-test') */ into t1 on(a,b) values(40,false);" | $BENDSQL_CLIENT_CONNECT
echo "replace /*+ SET_VAR(deduplicate_label='replace-test') */ into t1 on(a,b) values(50,false);" | $BENDSQL_CLIENT_CONNECT
echo "select * from t1 order by a" | $BENDSQL_CLIENT_CONNECT
