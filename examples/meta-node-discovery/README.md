# `meta-node-discovery` — standalone node-discovery reference app

A **self-contained, single-crate** example showing how to discover the live
query nodes of a Databend cluster by reading the meta-service KV store over
gRPC.

It is intentionally decoupled from the main `databend` workspace: it depends
**only** on the public `databend-meta-client` git crate (which is exactly what a
third-party tool would use), so you can copy this directory out, `cargo run`,
and study it in isolation.

Target cluster:

- `databend-meta`  **v1.2.879-nightly**  (meta service)
- `databend-query` **v1.2.911-nightly**  (query nodes)

## Run

```bash
cd examples/meta-node-discovery

# defaults: 127.0.0.1:9191, user root, tenant test_tenant
cargo run

# or point at a real cluster
METASRV_ENDPOINT=127.0.0.1:9191 \
METASRV_USERNAME=root \
METASRV_PASSWORD=root \
TENANT=test_tenant \
    cargo run
```

## What it reads

Query nodes >= v1.2.770 (system-managed cluster, PR #17051 — includes the
v1.2.911 nodes targeted here) register under the "v6" warehouse layout:

```text
__fd_clusters_v6/<tenant>/online_nodes/<escaped_node_id>                   -> NodeInfo (json)
__fd_clusters_v6/<tenant>/online_clusters/<warehouse>/<cluster>/<node_id>  -> NodeInfo (json)
```

`online_nodes` is the authoritative list of every live node of a tenant, so the
app lists that prefix and groups the results by warehouse -> cluster.

Older query nodes (up to ~v1.2.636) used a different "v3" layout
(`__fd_clusters_v3/<tenant>/<cluster_id>/databend_query/<node_id>`); this app
targets the v6 layout and will not discover v3-era nodes.

## Version compatibility note

The `databend-meta-client` used here requires the meta-server to be at least
`MIN_SERVER_VERSION = 1.2.770` during the gRPC handshake; v1.2.879 satisfies
that, so the connection succeeds.

## Why a standalone crate

To stay a faithful, copy-pastable reference for an external tool, this crate
depends only on the public `databend-meta-client` and avoids any internal
databend crate. Concretely it:

- depends only on `databend-meta-client` (pinned to the same git tag the
  databend workspace uses),
- builds the gRPC client with the client crate's own `TokioRuntime`
  (`databend_meta_client::runtime_api::TokioRuntime`) instead of databend's
  internal runtime,
- inlines the `escape_for_key` / `unescape_for_key` helpers (they live in an
  internal databend crate and are not published), and
- detaches itself from the parent workspace via an empty `[workspace]` table in
  its `Cargo.toml`.
