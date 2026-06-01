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

Query nodes >= v1.2.770 (system-managed cluster, PR #17051) register under the
"v6" warehouse layout:

```text
__fd_clusters_v6/<tenant>/online_nodes/<escaped_node_id> -> NodeInfo (json)
```

`online_nodes` is the authoritative list of every live node of a tenant, so the
app lists that prefix and groups the results by warehouse -> cluster.

## Why a separate crate (not a `--example` of the workspace)

The in-tree variant at `src/query/management/examples/node_discovery.rs` reuses
internal databend crates (`databend-common-meta-store`, `databend-meta-runtime`,
`databend-common-base`). This standalone version instead:

- depends only on `databend-meta-client` (pinned to the same git tag the
  workspace uses),
- builds the gRPC client with the client crate's own `TokioRuntime`
  (`databend_meta_client::runtime_api::TokioRuntime`) instead of databend's
  internal runtime,
- inlines the `escape_for_key` / `unescape_for_key` helpers (they live in an
  internal databend crate and are not published), and
- detaches itself from the parent workspace via an empty `[workspace]` table in
  its `Cargo.toml`.
