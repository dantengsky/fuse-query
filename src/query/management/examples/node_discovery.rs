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

//! Node discovery example for a cluster made of:
//!
//!   - `databend-meta`  v1.2.879-nightly      (the meta service)
//!   - `databend-query` v1.2.911-nightly      (the query nodes)
//!
//! Run with:
//!
//! ```bash
//! cargo run -p databend-common-management --example node_discovery
//!
//! # or point it at a real cluster:
//! METASRV_ENDPOINT=127.0.0.1:9191 \
//! METASRV_USERNAME=root \
//! METASRV_PASSWORD= \
//! TENANT=test_tenant \
//!     cargo run -p databend-common-management --example node_discovery
//! ```
//!
//! ## Why this differs from the old `__fd_clusters_v3` example
//!
//! The original `node_discovery.rs` example was written against the *legacy*
//! self-managed cluster layout used by databend-query up to ~v1.2.636:
//!
//! ```text
//! __fd_clusters_v3/<tenant>/<cluster_id>/databend_query/<node_id> -> NodeInfo (json)
//! ```
//!
//! Since `feat(cluster): support system-managed cluster (#17051)`, query nodes
//! (v1.2.770+, which includes the v1.2.911-nightly nodes targeted here) register
//! themselves under the *system-managed warehouse* ("v6") layout instead:
//!
//! ```text
//! __fd_clusters_v6/<tenant>/online_nodes/<node_id>                     -> NodeInfo (json)
//! __fd_clusters_v6/<tenant>/online_clusters/<wh>/<cluster>/<node_id>   -> NodeInfo (json)
//! ```
//!
//! `online_nodes` is the authoritative list of every live node of a tenant, so
//! that is what we enumerate here. `online_clusters` only mirrors the nodes that
//! have already been assigned to a warehouse/cluster, so it is skipped to avoid
//! double counting.
//!
//! Note on compatibility: the meta-client used by this crate
//! (`databend-meta-client` v260205.x) requires the meta-server to be at least
//! `MIN_SERVER_VERSION = 1.2.770` during the gRPC handshake. v1.2.879-nightly
//! satisfies that, so the connection succeeds.

use std::collections::BTreeMap;
use std::time::Duration;

use databend_common_base::base::escape_for_key;
use databend_common_base::base::unescape_for_key;
use databend_common_meta_store::MetaStoreProvider;
use databend_meta_client::RpcClientConf;
use databend_meta_client::kvapi::KvApiExt;
use databend_meta_client::kvapi::ListOptions;
use databend_meta_client::types::NodeInfo;
use databend_meta_client::types::NodeType;
use databend_meta_runtime::DatabendRuntime;

/// Key prefix for system-managed cluster nodes (databend-query >= v1.2.770).
///
/// Must stay in sync with `WAREHOUSE_API_KEY_PREFIX` in
/// `src/query/management/src/warehouse/warehouse_mgr.rs`.
const WAREHOUSE_API_KEY_PREFIX: &str = "__fd_clusters_v6";

/// The subtree under a tenant that holds the authoritative per-node entries.
const ONLINE_NODES_SUBTREE: &str = "online_nodes";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // ---- Connection settings (overridable via env vars) ----------------------
    let endpoint = env_or("METASRV_ENDPOINT", "127.0.0.1:9191");
    let username = env_or("METASRV_USERNAME", "root");
    let password = env_or("METASRV_PASSWORD", "");
    let tenant = env_or("TENANT", "test_tenant");

    println!("connecting to databend-meta at {endpoint} (tenant: {tenant}) ...");

    // ---- Build the gRPC meta-client -----------------------------------------
    // `RpcClientConf` here is `databend_meta_client::RpcClientConf` (the one the
    // store crate consumes), not the query-config wrapper of the same name.
    let mut rpc_conf = RpcClientConf::empty();
    rpc_conf.endpoints = vec![endpoint.clone()];
    rpc_conf.username = username;
    rpc_conf.password = password;
    rpc_conf.timeout = Some(Duration::from_secs(10));

    // A non-empty `endpoints` selects the remote (gRPC) backend; the handshake
    // against the v1.2.879 meta-server happens lazily on the first RPC below.
    let meta_store = MetaStoreProvider::new(rpc_conf)
        .create_meta_store::<DatabendRuntime>()
        .await?;

    // ---- List every live node of the tenant ---------------------------------
    // Layout: __fd_clusters_v6/<tenant>/online_nodes/<escaped_node_id> -> NodeInfo
    let nodes_prefix = format!(
        "{}/{}/{}",
        WAREHOUSE_API_KEY_PREFIX,
        escape_for_key(&tenant)?,
        ONLINE_NODES_SUBTREE,
    );

    let entries = meta_store
        .list_kv_collect(ListOptions::unlimited(&nodes_prefix))
        .await?;

    if entries.is_empty() {
        println!(
            "no online nodes found under '{nodes_prefix}'.\n\
             - is the tenant correct?\n\
             - are the query nodes (v1.2.911) actually running and registered?"
        );
        return Ok(());
    }

    // ---- Group by warehouse -> cluster --------------------------------------
    // Unassigned nodes (freshly started, not yet placed in a warehouse) carry
    // empty `warehouse_id`/`cluster_id`; bucket those under "<unassigned>".
    let mut grouped: BTreeMap<String, BTreeMap<String, Vec<NodeInfo>>> = BTreeMap::new();

    for (key, seq_v) in entries {
        let mut node: NodeInfo = serde_json::from_slice(&seq_v.data)?;

        // The node id is the last path segment; prefer the key as the source of
        // truth (it is what the meta service is keyed on) and fall back to it
        // when the serialized `id` is empty.
        if let Some(escaped_id) = key.strip_prefix(&format!("{nodes_prefix}/")) {
            let id = unescape_for_key(escaped_id)?;
            if node.id.is_empty() {
                node.id = id;
            }
        }

        let warehouse = bucket_label(&node.warehouse_id);
        let cluster = bucket_label(&node.cluster_id);

        grouped
            .entry(warehouse)
            .or_default()
            .entry(cluster)
            .or_default()
            .push(node);
    }

    // ---- Print the discovered topology --------------------------------------
    let total: usize = grouped
        .values()
        .flat_map(|clusters| clusters.values())
        .map(Vec::len)
        .sum();
    println!("discovered {total} node(s) for tenant '{tenant}':\n");

    for (warehouse, clusters) in &grouped {
        println!("warehouse: {warehouse}");
        for (cluster, nodes) in clusters {
            println!("  cluster: {cluster} ({} node(s))", nodes.len());
            for node in nodes {
                println!(
                    "    - {} [{}] flight={} http={} discovery={} binary={}",
                    node.id,
                    node_type_label(node),
                    show(&node.flight_address),
                    show(&node.http_address),
                    show(&node.discovery_address),
                    show(&node.binary_version),
                );
            }
        }
    }

    Ok(())
}

/// Reads an environment variable, falling back to `default` when unset or empty.
fn env_or(key: &str, default: &str) -> String {
    match std::env::var(key) {
        Ok(v) if !v.is_empty() => v,
        _ => default.to_string(),
    }
}

/// Maps an empty warehouse/cluster id to a readable placeholder.
fn bucket_label(id: &str) -> String {
    if id.is_empty() {
        "<unassigned>".to_string()
    } else {
        id.to_string()
    }
}

/// Renders an empty optional-ish string field as "-".
fn show(s: &str) -> &str {
    if s.is_empty() { "-" } else { s }
}

/// A short, version-agnostic label for the node's management type.
fn node_type_label(node: &NodeInfo) -> &'static str {
    match node.node_type {
        NodeType::SystemManaged => "system-managed",
        NodeType::SelfManaged => "self-managed",
    }
}
