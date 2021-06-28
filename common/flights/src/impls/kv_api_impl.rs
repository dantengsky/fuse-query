// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
//

use std::convert::identity;

use common_exception::ErrorCode;
use common_exception::Result;
use common_metatypes::SeqValue;
use common_store_api::kv_api::MGetKVActionResult;
use common_store_api::kv_api::PrefixListReply;
use common_store_api::kv_api::UpsertKVActionResult;
use common_store_api::GetKVActionResult;
use common_store_api::KVApi;

use crate::action_declare;
use crate::RequestFor;
use crate::StoreClient;
use crate::StoreDoAction;
use crate::UpsertKVAction;

// Let take this API for a reference of the implementations of a store API

// - GetKV

// We wrap the "request of getting a kv" up here as GetKVAction,
// Technically we can use `String` directly, but as we are ...
// provides that StoreDoAction::GetKV is typed as `:: String -> StoreAction`

// The return type of GetKVAction is `GetActionResult`, which is defined by the KVApi,
// we use it directly here, but we can also wrap it up if needed.
//

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct GetKVAction {
    pub key: String,
}

// Explicitly defined (the request / reply relation)
// this can be simplified by using macro (see code below)
impl RequestFor for GetKVAction {
    type Reply = GetKVActionResult;
}

// Explicitly defined the converter for StoreDoAction
// It's implementations' choice, that they gonna using enum StoreDoAction as wrapper.
// This can be simplified by using macro (see code below)
impl From<GetKVAction> for StoreDoAction {
    fn from(act: GetKVAction) -> Self {
        StoreDoAction::GetKV(act)
    }
}

// - MGetKV

// Again, impl chooses to wrap it up
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct MGetKVAction {
    pub keys: Vec<String>,
}

// here we use a macro to simplify the declarations
action_declare!(MGetKVAction, MGetKVActionResult, StoreDoAction::MGetKV);

// - prefix list
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct PrefixListReq(pub String);
action_declare!(PrefixListReq, PrefixListReply, StoreDoAction::PrefixListKV);

// - delete by key
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct DeleteByKeyReq {
    pub key: String,
    pub seq: Option<u64>,
}

// we can choose another reply type (other than KVApi method's)
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct DeleteByKeyReply {
    pub prev: Option<SeqValue>,
    pub result: Option<SeqValue>,
}

action_declare!(
    DeleteByKeyReq,
    DeleteByKeyReply,
    StoreDoAction::DeleteByKeyKV
);

#[async_trait::async_trait]
impl KVApi for StoreClient {
    async fn upsert_kv(
        &mut self,
        key: &str,
        seq: Option<u64>,
        value: Vec<u8>,
    ) -> Result<UpsertKVActionResult> {
        self.do_action(UpsertKVAction {
            key: key.to_string(),
            seq,
            value,
        })
        .await
    }

    async fn delete_kv(&mut self, key: &str, seq: Option<u64>) -> Result<()> {
        let res = self
            .do_action(DeleteByKeyReq {
                key: key.to_string(),
                seq,
            })
            .await?;

        match (&res.prev, &res.result) {
            //            (Some(prev), None) if seq.is_none() => Ok(()),
            (Some(prev), None) if prev.0 == seq.map_or(0, identity) => Ok(()),
            _ => Err(ErrorCode::UnknownKey(format!(
                "unknown key {:?}, seq {:?}",
                key, seq
            ))),
        }
    }

    async fn get_kv(&mut self, key: &str) -> Result<GetKVActionResult> {
        self.do_action(GetKVAction {
            key: key.to_string(),
        })
        .await
    }

    async fn mget_kv(&mut self, keys: &[&str]) -> common_exception::Result<MGetKVActionResult> {
        self.do_action(MGetKVAction {
            keys: keys.iter().map(|k| k.to_string()).collect(),
        })
        .await
    }

    async fn prefix_list_kv(&mut self, prefix: &str) -> common_exception::Result<PrefixListReply> {
        self.do_action(PrefixListReq(prefix.to_string())).await
    }
}
