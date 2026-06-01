# Examples

Standalone, copy-pastable examples for working with a Databend cluster.

## `meta-node-discovery/`

Discover the live `databend-query` nodes of a cluster by reading the
`databend-meta` service's KV store over gRPC.

Target cluster:

- `databend-meta`  **v1.2.879-nightly**  (meta service)
- `databend-query` **v1.2.911-nightly**  (query nodes)

This is a **self-contained single crate** with its own `[workspace]`, depending
only on the public `databend-meta-client` git crate — no internal databend
crates. Copy the directory out and `cargo run`.

```bash
cd examples/meta-node-discovery
cargo run
# or:
METASRV_ENDPOINT=127.0.0.1:9191 METASRV_USERNAME=root METASRV_PASSWORD=root TENANT=test_tenant cargo run
```

See `meta-node-discovery/README.md` for details.

### Related: the in-tree variant

There is a second, equivalent implementation wired into the workspace at
`src/query/management/examples/node_discovery.rs`. It reuses internal databend
crates (`databend-common-meta-store`, `databend-meta-runtime`,
`databend-common-base`) and runs via Cargo's example mechanism:

```bash
cargo run -p databend-common-management --example node_discovery
```

| | `examples/meta-node-discovery/` | `src/query/management/examples/node_discovery.rs` |
| --- | --- | --- |
| Dependencies | only public `databend-meta-client` | internal databend crates |
| gRPC runtime | client crate's `TokioRuntime` | internal `DatabendRuntime` |
| Builds standalone | yes (own workspace) | no (part of the workspace) |
| Best for | external/third-party reference | in-repo usage |

Both read the same KV layout and produce the same output.

## Node-registration KV layout (background)

Query nodes **v1.2.770+** (system-managed cluster, PR #17051 — includes the
v1.2.911 nodes targeted here) register under the "v6" warehouse layout:

```text
__fd_clusters_v6/<tenant>/online_nodes/<escaped_node_id>                   -> NodeInfo (json)
__fd_clusters_v6/<tenant>/online_clusters/<warehouse>/<cluster>/<node_id>  -> NodeInfo (json)
```

`online_nodes` is the authoritative list of every live node of a tenant, so the
examples list that prefix and group by warehouse → cluster.

Older query nodes (up to ~v1.2.636) used a different "v3" layout
(`__fd_clusters_v3/<tenant>/<cluster_id>/databend_query/<node_id>`); these
examples target the v6 layout and will not discover v3-era nodes.

## Version compatibility note

The `databend-meta-client` used here requires the meta-server to be at least
`MIN_SERVER_VERSION = 1.2.770` during the gRPC handshake; v1.2.879 satisfies
that, so the connection succeeds.
