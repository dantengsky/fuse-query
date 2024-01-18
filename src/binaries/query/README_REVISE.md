
./target/release/databend-revise  \
  -c  scripts/ci/deploy/config/databend-query-node-1.toml   \
  --revise-db default \
  --revise-tables t,t2 \
  --revise-parallel 10 \
  --revise_output_file /tmp/otuput
  
  

-c 指定query config; 必选； 请指定正确的meta sever; 并建议隔离cluster

--revise-db 指定要扫描的数据库的名字；可选，缺省为扫描所有非系统db
--revise-tables 指定要扫描的表的名字；可选，缺省为扫描所有表(engine类型不匹配的，会自动跳过)；多个表名逗号分隔
--revise-parallel 执行扫描的并行度；最小并行单元是一张表的一个segment; 缺省为100
--revise_output_file 扫描结果的输出文件；缺省为 /tmp/output


执行期间，不会开启对外的服务，不会对s3/meta server有任何写入操作


