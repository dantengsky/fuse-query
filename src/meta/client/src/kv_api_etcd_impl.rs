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

use databend_common_meta_kvapi::kvapi;
use databend_common_meta_kvapi::kvapi::GetKVReply;
use databend_common_meta_kvapi::kvapi::KVStream;
use databend_common_meta_kvapi::kvapi::MGetKVReply;
use databend_common_meta_kvapi::kvapi::UpsertKVReply;
use databend_common_meta_kvapi::kvapi::UpsertKVReq;
use databend_common_meta_types::protobuf::StreamItem;
use databend_common_meta_types::txn_condition::Target;
use databend_common_meta_types::txn_op::Request;
use databend_common_meta_types::txn_op_response::Response;
use databend_common_meta_types::InvalidReply;
use databend_common_meta_types::MetaError;
use databend_common_meta_types::Operation;
use databend_common_meta_types::SeqV;
use databend_common_meta_types::TxnDeleteByPrefixResponse;
use databend_common_meta_types::TxnDeleteResponse;
use databend_common_meta_types::TxnGetResponse;
use databend_common_meta_types::TxnPutResponse;
use databend_common_meta_types::TxnReply;
use databend_common_meta_types::TxnRequest;
use etcd_rs::Client as EtcdRsClient;
use etcd_rs::DeleteRequest;
use etcd_rs::KeyRange;
use etcd_rs::KeyValueOp;
use etcd_rs::PutRequest;
use etcd_rs::RangeRequest;
use etcd_rs::TxnCmp;
use etcd_rs::TxnOp;
use etcd_rs::TxnOpResponse;
use etcd_rs::TxnResponse;
use futures::StreamExt;
use futures::TryStreamExt;
use log::info;

pub struct EtcdRsClientWrapper(EtcdRsClient);

impl EtcdRsClientWrapper {
    pub fn new(client: EtcdRsClient) -> Self {
        EtcdRsClientWrapper(client)
    }
}

#[tonic::async_trait]
impl kvapi::KVApi for EtcdRsClientWrapper {
    type Error = MetaError;

    #[minitrace::trace]
    async fn upsert_kv(&self, act: UpsertKVReq) -> Result<UpsertKVReply, Self::Error> {
        let UpsertKVReq {
            key,
            seq,
            value,
            value_meta,
        } = act;

        use databend_common_meta_types::MatchSeq;
        match value {
            Operation::Delete => {
                match seq {
                    MatchSeq::Any | MatchSeq::GE(_) => {
                        let resp =
                            self.0.delete(key).await.map_err(|e| {
                                MetaError::from(InvalidReply::new("delete failed", &e))
                            })?;
                        let prev = resp.prev_kvs.first().map(|kv| SeqV {
                            seq: kv.version as u64,
                            meta: None,
                            data: kv.value.clone(),
                        });
                        Ok(UpsertKVReply::new(prev, None))
                    }
                    MatchSeq::Exact(_) => {
                        let resp =
                            self.0.delete(key).await.map_err(|e| {
                                MetaError::from(InvalidReply::new("delete failed", &e))
                            })?;
                        let prev = resp.prev_kvs.first().map(|kv| SeqV {
                            seq: kv.version as u64,
                            meta: None,
                            data: kv.value.clone(),
                        });
                        Ok(UpsertKVReply::new(prev, None))
                    }
                }
            }
            Operation::Update(value) => {
                // use etcd tx to retrieve the version after update
                let resp = self
                    .0
                    .put(PutRequest::new(key.clone(), value.clone()).prev_kv(true))
                    .await
                    .map_err(|e| MetaError::from(InvalidReply::new("put failed", &e)))?;

                let kv = resp.prev_kv;
                let result = SeqV {
                    seq: kv.version as u64 + 1,
                    meta: None,
                    data: value,
                };
                Ok(UpsertKVReply::new(None, Some(result)))
            }
            Operation::AsIs => {
                // NOTE: Assumes the key has been created before updated AsIs
                let mut put_req = PutRequest::new(key, "".to_string());
                put_req = put_req.ignore_value();
                let resp = self
                    .0
                    .put(put_req)
                    .await
                    .map_err(|e| MetaError::from(InvalidReply::new("put failed", &e)))?;
                let kv = resp.prev_kv;
                // seems the seq returns is not used (at least currently)
                let prev = SeqV {
                    seq: kv.mod_revision as u64,
                    meta: None,
                    data: kv.value,
                };
                Ok(UpsertKVReply::new(Some(prev), None))
            }
        }
    }
    async fn get_kv(&self, key: &str) -> Result<GetKVReply, Self::Error> {
        let resp = self.0.get(key).await.map_err(convert_err)?;
        let r = if let Some(kv) = resp.kvs.into_iter().next() {
            let seq_val = SeqV {
                seq: kv.version as u64,
                meta: None,
                data: kv.value,
            };
            Some(seq_val)
        } else {
            None
        };
        Ok(r)
    }

    #[minitrace::trace]
    async fn mget_kv(&self, keys: &[String]) -> Result<MGetKVReply, Self::Error> {
        let mut res = Vec::with_capacity(keys.len());
        for key in keys {
            let resp = self.get_kv(key).await?;
            res.push(resp);
        }
        Ok(res)
    }

    #[minitrace::trace]
    async fn list_kv(&self, prefix: &str) -> Result<KVStream<Self::Error>, Self::Error> {
        use etcd_rs::KeyRange;
        let resp = self
            .0
            .get(KeyRange::prefix(prefix))
            .await
            .map_err(convert_err)?;
        let res = resp.kvs.into_iter().map(|kv| {
            let seq_val = SeqV {
                seq: kv.version as u64,
                meta: None,
                data: kv.value,
            };
            Ok(StreamItem {
                key: String::from_utf8(kv.key).unwrap(),
                value: Some(seq_val.into()),
            })
        });

        Ok(futures::stream::iter(res).boxed())
    }

    #[minitrace::trace]
    async fn transaction(&self, txn: TxnRequest) -> Result<TxnReply, Self::Error> {
        let etc_req: etcd_rs::TxnRequest = TxnWrapper(txn).try_into()?;
        let resp = self
            .0
            .txn(etc_req)
            .await
            .map_err(|e| MetaError::from(InvalidReply::new("transaction failed", &e)))?;

        let resp = TxnRespWrapper(resp).try_into();
        let resp: TxnReply = resp?;

        Ok(resp)
    }
}

pub struct TxnWrapper(TxnRequest);
impl TryFrom<TxnWrapper> for etcd_rs::TxnRequest {
    type Error = MetaError;

    fn try_from(value: TxnWrapper) -> Result<Self, Self::Error> {
        let TxnRequest {
            condition,
            if_then,
            else_then,
        } = value.0;

        let mut etc_req = etcd_rs::TxnRequest::new();
        for cond in condition {
            let key_rang = KeyRange::key(cond.key);
            if let Some(target) = cond.target {
                match target {
                    Target::Value(v) => {
                        etc_req = etc_req.when_value(key_rang, TxnCmp::Equal, v);
                    }
                    Target::Seq(s) => {
                        // Eq = 0,
                        // Gt = 1,
                        // Ge = 2,
                        // Lt = 3,
                        // Le = 4,
                        // Ne = 5,
                        let cmp = match cond.expected {
                            0 => TxnCmp::Equal,
                            1 => TxnCmp::Greater,
                            2 => todo!(),
                            3 => TxnCmp::Less,
                            4 => todo!(),
                            5 => TxnCmp::NotEqual,
                            _ => unreachable!(),
                        };
                        etc_req = etc_req.when_version(key_rang, cmp, s as usize);
                    }
                }
            }
        }

        for then in if_then {
            if let Some(op) = then.request {
                match op {
                    Request::Get(k) => {
                        etc_req =
                            etc_req.and_then(TxnOp::Range(RangeRequest::new(KeyRange::key(k.key))));
                    }
                    Request::Put(put) => {
                        etc_req = etc_req.and_then(TxnOp::Put(PutRequest::new(put.key, put.value)));
                    }
                    Request::Delete(del) => {
                        etc_req = etc_req
                            .and_then(TxnOp::Delete(DeleteRequest::new(KeyRange::key(del.key))));
                    }
                    Request::DeleteByPrefix(del_prefix) => {
                        etc_req = etc_req.and_then(TxnOp::Delete(DeleteRequest::new(
                            KeyRange::prefix(del_prefix.prefix),
                        )));
                    }
                }
            }
        }

        for els in else_then {
            if let Some(op) = els.request {
                match op {
                    Request::Get(k) => {
                        etc_req =
                            etc_req.or_else(TxnOp::Range(RangeRequest::new(KeyRange::key(k.key))));
                    }
                    Request::Put(put) => {
                        etc_req = etc_req.or_else(TxnOp::Put(PutRequest::new(put.key, put.value)));
                    }
                    Request::Delete(del) => {
                        etc_req = etc_req
                            .or_else(TxnOp::Delete(DeleteRequest::new(KeyRange::key(del.key))));
                    }
                    Request::DeleteByPrefix(del_prefix) => {
                        etc_req = etc_req.or_else(TxnOp::Delete(DeleteRequest::new(
                            KeyRange::prefix(del_prefix.prefix),
                        )));
                    }
                }
            }
        }

        Ok(etc_req)
    }
}

pub struct TxnRespWrapper(TxnResponse);

impl TryFrom<TxnRespWrapper> for TxnReply {
    type Error = MetaError;

    fn try_from(value: TxnRespWrapper) -> Result<Self, Self::Error> {
        let resp = value.0;
        let mut repl = TxnReply::default();
        info!("TxnResponse success? {}", resp.succeeded);
        repl.success = resp.succeeded;
        repl.error = "".to_owned();
        repl.responses = resp
            .responses
            .into_iter()
            .map(|x| TxnOpRespW(x).into())
            .collect();
        Ok(repl)
    }
}

pub struct TxnOpRespW(TxnOpResponse);
impl From<TxnOpRespW> for databend_common_meta_types::TxnOpResponse {
    fn from(value: TxnOpRespW) -> Self {
        let v = value.0;
        let mut txn_resp = databend_common_meta_types::TxnOpResponse::default();
        match v {
            TxnOpResponse::Range(resp) => {
                if let Some(kv) = resp.kvs.into_iter().next() {
                    let seq_val = databend_common_meta_types::protobuf::SeqV {
                        seq: kv.version as u64,
                        meta: None,
                        data: kv.value,
                    };

                    let r = Response::Get(TxnGetResponse {
                        key: String::from_utf8(kv.key).unwrap(),
                        value: Some(seq_val),
                    });
                    txn_resp.response = Some(r);
                };
            }
            TxnOpResponse::Put(resp) => {
                let kv = resp.prev_kv;
                let seq_val = databend_common_meta_types::protobuf::SeqV {
                    seq: kv.version as u64,
                    meta: None,
                    data: kv.value,
                };
                let r = Response::Put(TxnPutResponse {
                    key: String::from_utf8(kv.key).unwrap(),
                    prev_value: Some(seq_val),
                });
                txn_resp.response = Some(r);
            }
            TxnOpResponse::Delete(resp) => {
                if resp.prev_kvs.len() == 1 {
                    let kv = resp.prev_kvs[0].clone();
                    let seq_val = databend_common_meta_types::protobuf::SeqV {
                        seq: kv.version as u64,
                        meta: None,
                        data: kv.value,
                    };
                    let r = Response::Delete(TxnDeleteResponse {
                        success: true,
                        key: String::from_utf8(kv.key).unwrap(),
                        prev_value: Some(seq_val),
                    });
                    txn_resp.response = Some(r);
                } else if resp.prev_kvs.len() > 1 {
                    let r = Response::DeleteByPrefix(TxnDeleteByPrefixResponse {
                        // unfortunately, "prefix" is lost
                        prefix: "".to_string(),
                        count: resp.prev_kvs.len() as u32,
                    });
                    txn_resp.response = Some(r);
                }
            }
            TxnOpResponse::Txn(_) => {
                unreachable!()
            }
        }

        txn_resp
    }
}

fn convert_err(e: etcd_rs::Error) -> MetaError {
    MetaError::from(InvalidReply::new(format!("err"), &e))
}
