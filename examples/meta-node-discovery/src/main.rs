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

//! Standalone node-discovery example.
//!
//! Connects to a `databend-meta` v1.2.879-nightly service and lists the live
//! `databend-query` v1.2.911-nightly nodes of a tenant, reading the raft KV
//! store directly over gRPC.
//!
//! This binary depends only on the public `databend-meta-client` crate (which
//! bundles its own tokio
//! runtime), so it is a faithful, copy-pastable reference for an external tool.

use std::collections::BTreeMap;
use std::time::Duration;

use anyhow::Context;
use databend_meta_client::MetaGrpcClient;
use databend_meta_client::RpcClientConf;
use databend_meta_client::runtime_api::TokioRuntime;
use databend_meta_client::types::NodeInfo;
use databend_meta_client::types::NodeType;
use futures::StreamExt;

/// Key prefix for system-managed cluster nodes (databend-query >= v1.2.770).
const WAREHOUSE_API_KEY_PREFIX: &str = "__fd_clusters_v6";

/// Subtree under a tenant that holds the authoritative per-node entries.
const ONLINE_NODES_SUBTREE: &str = "online_nodes";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // ---- Connection settings (overridable via env vars) ---------------------
    let endpoint = env_or("METASRV_ENDPOINT", "127.0.0.1:9191");
    let username = env_or("METASRV_USERNAME", "root");
    let password = env_or("METASRV_PASSWORD", "root");
    let tenant = env_or("TENANT", "test_tenant");

    println!("connecting to databend-meta at {endpoint} (tenant: {tenant}) ...");

    // ---- Build the gRPC meta-client -----------------------------------------
    // The client crate ships its own `TokioRuntime` (a `RuntimeApi` impl), so we
    // do not need databend's internal runtime crate here.
    let mut rpc_conf = RpcClientConf::empty();
    rpc_conf.endpoints = vec![endpoint.clone()];
    rpc_conf.username = username;
    rpc_conf.password = password;
    rpc_conf.timeout = Some(Duration::from_secs(10));

    let client = MetaGrpcClient::<TokioRuntime>::try_new(&rpc_conf)
        .map_err(|e| anyhow::anyhow!("failed to create meta-client: {e}"))?;

    // ---- List every live node of the tenant ---------------------------------
    // Layout: __fd_clusters_v6/<tenant>/online_nodes/<escaped_node_id> -> NodeInfo
    let nodes_prefix = format!(
        "{}/{}/{}",
        WAREHOUSE_API_KEY_PREFIX,
        escape_for_key(&tenant),
        ONLINE_NODES_SUBTREE,
    );

    // `ClientHandle::list` streams `StreamItem { key, value }` items. The first
    // RPC triggers the version handshake against the v1.2.879 meta-server.
    let mut stream = client
        .list(&nodes_prefix)
        .await
        .map_err(|e| anyhow::anyhow!("list_kv failed (is the meta endpoint reachable?): {e}"))?;

    let mut grouped: BTreeMap<String, BTreeMap<String, Vec<NodeInfo>>> = BTreeMap::new();
    let mut total = 0usize;

    while let Some(item) = stream.next().await {
        let item = item.map_err(|status| anyhow::anyhow!("stream error: {status}"))?;

        // `value` is None only for tombstones; live node entries always carry data.
        let Some(seq_v) = item.value else {
            continue;
        };

        let mut node: NodeInfo = serde_json::from_slice(&seq_v.data)
            .with_context(|| format!("decode NodeInfo at key '{}'", item.key))?;

        // The node id is the last path segment of the key; prefer it as the
        // source of truth and fall back to it when the serialized id is empty.
        if let Some(escaped_id) = item.key.strip_prefix(&format!("{nodes_prefix}/")) {
            let id = unescape_for_key(escaped_id);
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
        total += 1;
    }

    if total == 0 {
        println!(
            "no online nodes found under '{nodes_prefix}'.\n\
             - is the tenant correct?\n\
             - are the query nodes (v1.2.911) actually running and registered?"
        );
        return Ok(());
    }

    // ---- Print the discovered topology --------------------------------------
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

/// A short label for the node's management type.
fn node_type_label(node: &NodeInfo) -> &'static str {
    match node.node_type {
        NodeType::SystemManaged => "system-managed",
        NodeType::SelfManaged => "self-managed",
    }
}

/// Escapes special characters in a meta KV key segment.
///
/// All characters except digits, ASCII letters and `_` are encoded as `%XX`
/// where `XX` is the lowercase hex of the byte. Inlined from databend's internal
/// `databend_common_base::base::escape_for_key` so this example stays standalone;
/// it must match that logic exactly to build the same keys the query nodes use.
fn escape_for_key(key: &str) -> String {
    fn hex(num: u8) -> u8 {
        match num {
            0..=9 => b'0' + num,
            _ => b'a' + (num - 10),
        }
    }

    let mut out = Vec::with_capacity(key.len());
    for b in key.as_bytes() {
        match b {
            b'0'..=b'9' | b'_' | b'a'..=b'z' | b'A'..=b'Z' => out.push(*b),
            _ => {
                out.push(b'%');
                out.push(hex(*b / 16));
                out.push(hex(*b % 16));
            }
        }
    }
    // `out` only ever contains ASCII, so this is always valid UTF-8.
    String::from_utf8(out).expect("escaped key is valid utf8")
}

/// Reverse of [`escape_for_key`].
fn unescape_for_key(key: &str) -> String {
    fn unhex(num: u8) -> u8 {
        match num {
            b'0'..=b'9' => num - b'0',
            b'a'..=b'f' => num - b'a' + 10,
            b'A'..=b'F' => num - b'A' + 10,
            _ => 0,
        }
    }

    let bytes = key.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            out.push(unhex(bytes[i + 1]) * 16 + unhex(bytes[i + 2]));
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).unwrap_or_else(|_| key.to_string())
}
