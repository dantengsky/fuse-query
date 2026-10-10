// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::sync::Arc;

use databend_common_base::base::get_free_tcp_port;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_settings::FlightKeepAliveParams;
use databend_meta_client::types::NodeInfo;
use databend_query::clusters::Cluster;
use databend_query::clusters::ClusterHelper;
use databend_query::clusters::FlightParams;
use databend_query::servers::flight::v1::actions::FINISH_QUERY;
use databend_query::servers::flight::v1::actions::FinishQueryPacket;
use databend_query::test_kits::*;

fn node(id: &str, flight_address: &str) -> Arc<NodeInfo> {
    let secret = GlobalConfig::instance().query.node_secret.clone();
    Arc::new(NodeInfo::create(
        id.to_string(),
        secret,
        String::new(),
        flight_address.to_string(),
        String::new(),
        String::new(),
        String::new(),
    ))
}

fn flight_params(concurrent: bool) -> FlightParams {
    FlightParams {
        timeout: 5,
        retry_times: 0,
        retry_interval: 0,
        keep_alive: FlightKeepAliveParams::default(),
        concurrent,
    }
}

fn finish_packets(ids: &[&str]) -> HashMap<String, FinishQueryPacket> {
    ids.iter()
        .map(|id| {
            (id.to_string(), FinishQueryPacket {
                query_id: format!("query-{id}"),
                cause: "test".to_string(),
            })
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_do_action_reports_unreachable_node_in_both_modes() -> anyhow::Result<()> {
    let _fixture = TestFixture::setup().await?;
    // Nothing listens on these ports: both nodes are unreachable, whichever is tried first.
    let nodes = vec![
        node("alive", &format!("127.0.0.1:{}", get_free_tcp_port())),
        node("dead", &format!("127.0.0.1:{}", get_free_tcp_port())),
    ];
    let cluster = Cluster::create(nodes, "alive".to_string());

    for concurrent in [false, true] {
        let result: databend_common_exception::Result<HashMap<String, ()>> = cluster
            .do_action(
                FINISH_QUERY,
                finish_packets(&["alive", "dead"]),
                flight_params(concurrent),
            )
            .await;

        let error = result.expect_err("an unreachable node must fail the whole action");
        assert_eq!(
            error.code(),
            ErrorCode::CANNOT_CONNECT_NODE,
            "concurrent = {concurrent}, got {error}"
        );
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_do_action_unknown_node_fails_before_dispatch() -> anyhow::Result<()> {
    let _fixture = TestFixture::setup().await?;
    let address = format!("127.0.0.1:{}", get_free_tcp_port());

    let cluster = Cluster::create(vec![node("n1", &address)], "n1".to_string());

    for concurrent in [false, true] {
        let result: databend_common_exception::Result<HashMap<String, ()>> = cluster
            .do_action(
                FINISH_QUERY,
                finish_packets(&["n1", "missing"]),
                flight_params(concurrent),
            )
            .await;

        let error = result.expect_err("a node outside the cluster must be rejected");
        assert_eq!(error.code(), ErrorCode::NOT_FOUND_CLUSTER_NODE);
    }

    Ok(())
}
