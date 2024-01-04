ulimit -Sn 65535
host1="192.168.1.2"
host2="192.168.1.2"
host3="192.168.1.2"
etcd --name infra0 \
    --log-level error\
    --max-txn-ops 20000 \
    --max-request-bytes 15728640 \
    --heartbeat-interval  1000 \
    --election-timeout 6000 \
    --quota-backend-bytes=$((16*1024*1024*1024)) \
    --initial-advertise-peer-urls http://${host1}:2380 \
    --listen-peer-urls http://${host1}:2380 \
    --listen-client-urls http://${host1}:2379,http://127.0.0.1:2379 \
    --advertise-client-urls http://${host1}:2379 \
    --initial-cluster-token etcd-cluster-1 \
    --initial-cluster infra0=http://${host2}:2380,infra1=http://${host2}:2381,infra2=http://${host3}:2382 \
    --initial-cluster-state new
