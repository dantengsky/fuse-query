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

use std::sync::Arc;

use common_base::base::escape_for_key;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_app::principal::NetworkPolicy;
use common_meta_kvapi::kvapi;
use common_meta_kvapi::kvapi::UpsertKVReq;
use common_meta_types::MatchSeq;
use common_meta_types::MatchSeqExt;
use common_meta_types::MetaError;
use common_meta_types::Operation;
use common_meta_types::SeqV;

use crate::network_policy::network_policy_api::NetworkPolicyApi;
use crate::serde::deserialize_struct;
use crate::serde::serialize_struct;

static NETWORK_POLICY_API_KEY_PREFIX: &str = "__fd_network_policies";

pub struct NetworkPolicyMgr {
    kv_api: Arc<dyn kvapi::KVApi<Error = MetaError>>,
    network_policy_prefix: String,
}

impl NetworkPolicyMgr {
    pub fn create(
        kv_api: Arc<dyn kvapi::KVApi<Error = MetaError>>,
        tenant: &str,
    ) -> Result<Self, ErrorCode> {
        if tenant.is_empty() {
            return Err(ErrorCode::TenantIsEmpty(
                "Tenant can not empty (while create network policy)",
            ));
        }

        Ok(NetworkPolicyMgr {
            kv_api,
            network_policy_prefix: format!("{}/{}", NETWORK_POLICY_API_KEY_PREFIX, tenant),
        })
    }

    fn make_network_policy_key(&self, name: &str) -> Result<String> {
        Ok(format!(
            "{}/{}",
            self.network_policy_prefix,
            escape_for_key(name)?
        ))
    }
}

#[async_trait::async_trait]
impl NetworkPolicyApi for NetworkPolicyMgr {
    #[async_backtrace::framed]
    #[minitrace::trace]
    async fn add_network_policy(&self, network_policy: NetworkPolicy) -> Result<u64> {
        let match_seq = MatchSeq::Exact(0);
        let key = self.make_network_policy_key(network_policy.name.as_str())?;
        let value = Operation::Update(serialize_struct(
            &network_policy,
            ErrorCode::IllegalNetworkPolicy,
            || "",
        )?);

        let kv_api = self.kv_api.clone();
        let upsert_kv = kv_api.upsert_kv(UpsertKVReq::new(&key, match_seq, value, None));

        let res_seq = upsert_kv.await?.added_seq_or_else(|v| {
            ErrorCode::NetworkPolicyAlreadyExists(format!(
                "NetworkPolicy already exists, seq [{}]",
                v.seq
            ))
        })?;

        Ok(res_seq)
    }

    #[async_backtrace::framed]
    #[minitrace::trace]
    async fn update_network_policy(
        &self,
        network_policy: NetworkPolicy,
        match_seq: MatchSeq,
    ) -> Result<u64> {
        let key = self.make_network_policy_key(network_policy.name.as_str())?;
        let value = Operation::Update(serialize_struct(
            &network_policy,
            ErrorCode::IllegalNetworkPolicy,
            || "",
        )?);

        let kv_api = self.kv_api.clone();
        let upsert_kv = kv_api
            .upsert_kv(UpsertKVReq::new(&key, match_seq, value, None))
            .await?;

        match upsert_kv.result {
            Some(SeqV { seq: s, .. }) => Ok(s),
            None => Err(ErrorCode::UnknownNetworkPolicy(format!(
                "Unknown NetworkPolicy, or seq not match {}",
                network_policy.name.clone()
            ))),
        }
    }

    #[async_backtrace::framed]
    #[minitrace::trace]
    async fn drop_network_policy(&self, name: &str, seq: MatchSeq) -> Result<()> {
        let key = self.make_network_policy_key(name)?;
        let kv_api = self.kv_api.clone();
        let res = kv_api
            .upsert_kv(UpsertKVReq::new(&key, seq, Operation::Delete, None))
            .await?;
        if res.prev.is_some() && res.result.is_none() {
            Ok(())
        } else {
            Err(ErrorCode::UnknownNetworkPolicy(format!(
                "Unknown NetworkPolicy {}",
                name
            )))
        }
    }

    #[async_backtrace::framed]
    #[minitrace::trace]
    async fn get_network_policy(&self, name: &str, seq: MatchSeq) -> Result<SeqV<NetworkPolicy>> {
        let key = self.make_network_policy_key(name)?;
        let res = self.kv_api.get_kv(&key).await?;
        let seq_value = res.ok_or_else(|| {
            ErrorCode::UnknownNetworkPolicy(format!("Unknown NetworkPolicy {}", name))
        })?;

        match seq.match_seq(&seq_value) {
            Ok(_) => Ok(SeqV::new(
                seq_value.seq,
                deserialize_struct(&seq_value.data, ErrorCode::IllegalNetworkPolicy, || "")?,
            )),
            Err(_) => Err(ErrorCode::UnknownNetworkPolicy(format!(
                "Unknown NetworkPolicy {}",
                name
            ))),
        }
    }

    #[async_backtrace::framed]
    #[minitrace::trace]
    async fn get_network_policies(&self) -> Result<Vec<NetworkPolicy>> {
        let values = self
            .kv_api
            .prefix_list_kv(&self.network_policy_prefix)
            .await?;

        let mut network_policies = Vec::with_capacity(values.len());
        for (_, value) in values {
            let network_policy =
                deserialize_struct(&value.data, ErrorCode::IllegalNetworkPolicy, || "")?;
            network_policies.push(network_policy);
        }
        Ok(network_policies)
    }
}
