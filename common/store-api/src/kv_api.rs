// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
//

use common_metatypes::SeqValue;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct UpsertKVActionResult {
    /// prev is the value before upsert.
    pub prev: Option<SeqValue>,
    /// result is the value after upsert.
    pub result: Option<SeqValue>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct GetKVActionResult {
    pub result: Option<SeqValue>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct MGetKVActionResult {
    pub result: Vec<Option<SeqValue>>,
}

pub type PrefixListReply = Vec<SeqValue>;

#[async_trait::async_trait]
pub trait KVApi: Sync + Send {
    async fn upsert_kv(
        &mut self,
        key: &str,
        seq: Option<u64>,
        value: Vec<u8>,
    ) -> common_exception::Result<UpsertKVActionResult>;

    async fn delete_kv(&mut self, key: &str, seq: Option<u64>) -> common_exception::Result<()>;
    async fn update_kv(
        &mut self,
        key: &str,
        seq: Option<u64>,
        value: Vec<u8>,
    ) -> common_exception::Result<()>;

    async fn get_kv(&mut self, key: &str) -> common_exception::Result<GetKVActionResult>;

    async fn mget_kv(&mut self, key: &[&str]) -> common_exception::Result<MGetKVActionResult>;

    async fn prefix_list_kv(&mut self, prefix: &str) -> common_exception::Result<PrefixListReply>;
}
