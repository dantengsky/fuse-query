// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fmt;
use std::fmt::Display;
use std::fmt::Formatter;

use common_exception::prelude::ErrorCode;
use common_metatypes::Database;
use common_metatypes::SeqValue;
use common_metatypes::Table;
use common_tracing::tracing;
use serde::Deserialize;
use serde::Serialize;

use crate::meta_service::placement::rand_n_from_m;
use crate::meta_service::ClientRequest;
use crate::meta_service::ClientResponse;
use crate::meta_service::Cmd;
use crate::meta_service::NodeId;
use crate::meta_service::Placement;

/// seq number key to generate seq for the value of a `generic_kv` record.
const SEQ_GENERIC_KV: &str = "generic_kv";
/// seq number key to generate database id
const SEQ_DATABASE_ID: &str = "database_id";
/// seq number key to generate table id
// const SEQ_TABLE_ID: &str = "table_id";

/// Replication defines the replication strategy.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Replication {
    /// n-copies mode.
    Mirror(u64),
}

impl Default for Replication {
    fn default() -> Self {
        Replication::Mirror(1)
    }
}

/// The state machine of the `MemStore`.
/// It includes user data and two raft-related informations:
/// `last_applied_logs` and `client_serial_responses` to achieve idempotence.
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct StateMachine {
    /// raft state: last applied log.
    pub last_applied_log: u64,

    /// raft state: A mapping of client IDs to their state info:
    /// (serial, ClientResponse)
    /// This is used to de-dup client request, to impl  idempotent operations.
    pub client_serial_responses: HashMap<String, (u64, ClientResponse)>,

    /// The file names stored in this cluster
    pub keys: BTreeMap<String, String>,

    /// storage of auto-incremental number.
    pub sequences: BTreeMap<String, u64>,

    // cluster nodes, key distribution etc.
    pub slots: Vec<Slot>,
    pub nodes: HashMap<NodeId, Node>,

    pub replication: Replication,

    /// db name to database mapping
    pub databases: BTreeMap<String, Database>,

    /// table id to table mapping
    pub tables: BTreeMap<u64, Table>,

    /// A kv store of all other general purpose information.
    /// The value is tuple of a monotonic sequence number and userdata value in string.
    /// The sequence number is guaranteed to increment(by some value greater than 0) everytime the record changes.
    pub kv: BTreeMap<String, (u64, Vec<u8>)>,
}

#[derive(Debug, Default, Clone)]
pub struct MetaBuilder {
    /// The number of slots to allocated.
    initial_slots: Option<u64>,
    /// The replication strategy.
    replication: Option<Replication>,
}

impl MetaBuilder {
    /// Set the number of slots to boot up a cluster.
    pub fn slots(mut self, n: u64) -> Self {
        self.initial_slots = Some(n);
        self
    }

    /// Specifies the cluster to replicate by mirror `n` copies of every file.
    pub fn mirror_replication(mut self, n: u64) -> Self {
        self.replication = Some(Replication::Mirror(n));
        self
    }

    pub fn build(self) -> common_exception::Result<StateMachine> {
        let initial_slots = self.initial_slots.unwrap_or(3);
        let replication = self.replication.unwrap_or(Replication::Mirror(1));

        let mut m = StateMachine {
            last_applied_log: 0,
            client_serial_responses: Default::default(),
            keys: BTreeMap::new(),
            sequences: BTreeMap::new(),
            slots: Vec::with_capacity(initial_slots as usize),
            nodes: HashMap::new(),
            replication,
            databases: BTreeMap::new(),
            tables: BTreeMap::new(),
            kv: BTreeMap::new(),
        };
        for _i in 0..initial_slots {
            m.slots.push(Slot::default());
        }
        Ok(m)
    }
}

impl StateMachine {
    pub fn builder() -> MetaBuilder {
        MetaBuilder {
            ..Default::default()
        }
    }

    /// Internal func to get an auto-incr seq number.
    /// It is just what Cmd::IncrSeq does and is also used by Cmd that requires
    /// a unique id such as Cmd::AddDatabase which needs make a new database id.
    fn incr_seq(&mut self, key: &str) -> u64 {
        let prev = self.sequences.get(key);
        let curr = match prev {
            Some(v) => v + 1,
            None => 1,
        };
        self.sequences.insert(key.to_string(), curr);
        tracing::debug!("applied IncrSeq: {}={}", key, curr);

        curr
    }

    /// Apply an log entry to state machine.
    ///
    /// If a duplicated log entry is detected by checking data.txid, no update
    /// will be made and the previous resp is returned. In this way a client is able to re-send a
    /// command safely in case of network failure etc.
    #[tracing::instrument(level = "trace", skip(self))]
    pub fn apply(&mut self, index: u64, data: &ClientRequest) -> anyhow::Result<ClientResponse> {
        self.last_applied_log = index;
        if let Some(ref txid) = data.txid {
            if let Some((serial, resp)) = self.client_serial_responses.get(&txid.client) {
                if serial == &txid.serial {
                    return Ok(resp.clone());
                }
            }
        }

        let resp = self.apply_non_dup(data)?;

        if let Some(ref txid) = data.txid {
            self.client_serial_responses
                .insert(txid.client.clone(), (txid.serial, resp.clone()));
        }
        Ok(resp)
    }

    /// Apply an op into state machine.
    /// Already applied log should be filtered out before passing into this functino.
    /// This is the only entry to modify state machine.
    /// The `data` is always committed by raft before applying.
    #[tracing::instrument(level = "debug", skip(self))]
    pub fn apply_non_dup(
        &mut self,
        data: &ClientRequest,
    ) -> common_exception::Result<ClientResponse> {
        match data.cmd {
            Cmd::AddFile { ref key, ref value } => {
                if self.keys.contains_key(key) {
                    let prev = self.keys.get(key);
                    Ok((prev.cloned(), None).into())
                } else {
                    let prev = self.keys.insert(key.clone(), value.clone());
                    tracing::info!("applied AddFile: {}={}", key, value);
                    Ok((prev, Some(value.clone())).into())
                }
            }

            Cmd::SetFile { ref key, ref value } => {
                let prev = self.keys.insert(key.clone(), value.clone());
                tracing::info!("applied SetFile: {}={}", key, value);
                Ok((prev, Some(value.clone())).into())
            }

            Cmd::IncrSeq { ref key } => Ok(self.incr_seq(key).into()),

            Cmd::AddNode {
                ref node_id,
                ref node,
            } => {
                if self.nodes.contains_key(node_id) {
                    let prev = self.nodes.get(node_id);
                    Ok((prev.cloned(), None).into())
                } else {
                    let prev = self.nodes.insert(*node_id, node.clone());
                    tracing::info!("applied AddNode: {}={:?}", node_id, node);
                    Ok((prev, Some(node.clone())).into())
                }
            }

            Cmd::AddDatabase { ref name } => {
                // - If the db present, return it.
                // - Otherwise, create a new one with next seq number as database id, and add it in to store.
                if self.databases.contains_key(name) {
                    let prev = self.databases.get(name);
                    Ok((prev.cloned(), None).into())
                } else {
                    let db = Database {
                        database_id: self.incr_seq(SEQ_DATABASE_ID),
                        tables: Default::default(),
                    };

                    let prev = self.databases.insert(name.clone(), db.clone());
                    tracing::debug!("applied AddDatabase: {}={:?}", name, db);

                    Ok((prev, Some(db)).into())
                }
            }

            Cmd::UpsertKV {
                ref key,
                ref seq,
                ref value,
            } => {
                let prev = self.kv.get(key).cloned();
                let seq_matched = StateMachine::match_seq(&prev, seq);
                if !seq_matched {
                    return Ok((prev, None).into());
                }

                let new_seq = self.incr_seq(SEQ_GENERIC_KV);
                let record_value = (new_seq, value.clone());
                self.kv.insert(key.clone(), record_value.clone());
                tracing::debug!("applied UpsertKV: {}={:?}", key, record_value);

                Ok((prev, Some(record_value)).into())
            }

            Cmd::DeleteByKeyKV { ref key, ref seq } => {
                let prev = self.kv.get(key).cloned();
                let seq_matched = StateMachine::match_seq(&prev, seq);
                if !seq_matched {
                    return Ok((prev, None).into());
                }

                let prev = self.kv.remove(key);
                tracing::debug!("applied DeleteByKeyKV: {}={:?}", key, seq);
                Ok((prev, None).into())
            }

            Cmd::UpdateByKeyKV {
                ref key,
                ref seq,
                ref value,
            } => {
                let prev = self.kv.get(key).cloned();
                let seq_matched = prev
                    .as_ref()
                    .map_or_else(|| false, |s| seq.is_none() || s.0 == seq.unwrap_or(0));
                if !seq_matched {
                    return Ok((prev, None).into());
                }

                let new_seq = self.incr_seq(SEQ_GENERIC_KV);
                let record_value = (new_seq, value.clone());
                self.kv.insert(key.clone(), record_value.clone());
                tracing::debug!("applied UpdateByKeyKV: {}={:?}", key, seq);
                Ok((prev, Some(record_value)).into())
            }
        }
    }

    fn match_seq(prev: &Option<SeqValue>, seq: &Option<u64>) -> bool {
        if let Some(seq) = seq {
            if *seq == 0 {
                prev.is_none()
            } else {
                match prev {
                    Some(ref p) => *seq == (*p).0,
                    None => false,
                }
            }
        } else {
            // If seq is None, always override it.
            true
        }
    }

    /// Initialize slots by assign nodes to everyone of them randomly, according to replicationn config.
    pub fn init_slots(&mut self) -> common_exception::Result<()> {
        for i in 0..self.slots.len() {
            self.assign_rand_nodes_to_slot(i)?;
        }

        Ok(())
    }

    /// Assign `n` random nodes to a slot thus the files associated to this slot are replicated to the corresponding nodes.
    /// This func does not cnosider nodes load and should only be used when a Dfs cluster is initiated.
    /// TODO(xp): add another func for load based assignment
    pub fn assign_rand_nodes_to_slot(&mut self, slot_index: usize) -> common_exception::Result<()> {
        let n = match self.replication {
            Replication::Mirror(x) => x,
        } as usize;

        let mut node_ids = self.nodes.keys().collect::<Vec<&NodeId>>();
        node_ids.sort();
        let total = node_ids.len();
        let node_indexes = rand_n_from_m(total, n)?;

        let mut slot = self
            .slots
            .get_mut(slot_index)
            .ok_or_else(|| ErrorCode::InvalidConfig(format!("slot not found: {}", slot_index)))?;

        slot.node_ids = node_indexes.iter().map(|i| *node_ids[*i]).collect();

        Ok(())
    }

    #[tracing::instrument(level = "info", skip(self))]
    pub fn get_file(&self, key: &str) -> Option<String> {
        tracing::info!("meta::get_file: {}", key);
        let x = self.keys.get(key);
        tracing::info!("meta::get_file: {}={:?}", key, x);
        x.cloned()
    }

    pub fn get_node(&self, node_id: &NodeId) -> Option<Node> {
        let x = self.nodes.get(node_id);
        x.cloned()
    }

    pub fn get_database(&self, name: &str) -> Option<Database> {
        let x = self.databases.get(name);
        x.cloned()
    }

    pub fn get_kv(&self, key: &str) -> Option<SeqValue> {
        let x = self.kv.get(key);
        x.cloned()
    }

    pub fn mget_kv(&self, keys: &[impl AsRef<str>]) -> Vec<Option<SeqValue>> {
        keys.iter()
            .map(|key| self.kv.get(key.as_ref()).cloned())
            .collect()
    }

    pub fn prefix_list_kv(&self, prefix: &str) -> Vec<SeqValue> {
        self.kv
            .range(prefix.to_string()..)
            .take_while(|(k, _)| k.starts_with(prefix))
            .map(|v| v.1.clone())
            .collect()
    }
}

/// A slot is a virtual and intermediate allocation unit in a distributed storage.
/// The key of an object is mapped to a slot by some hashing algo.
/// A slot is assigned to several physical servers(normally 3 for durability).
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct Slot {
    pub node_ids: Vec<NodeId>,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq)]
pub struct Node {
    pub name: String,
    pub address: String,
}

impl Display for Node {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}={}", self.name, self.address)
    }
}

impl Placement for StateMachine {
    fn get_slots(&self) -> &[Slot] {
        &self.slots
    }

    fn get_node(&self, node_id: &u64) -> Option<&Node> {
        self.nodes.get(node_id)
    }
}
