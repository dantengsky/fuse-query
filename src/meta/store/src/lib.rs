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

use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use databend_common_grpc::RpcClientConf;
use databend_common_meta_client::ClientHandle;
use databend_common_meta_client::EtcdRsClientWrapper;
use databend_common_meta_embedded::MetaEmbedded;
use databend_common_meta_kvapi::kvapi;
use databend_common_meta_kvapi::kvapi::GetKVReply;
use databend_common_meta_kvapi::kvapi::KVStream;
use databend_common_meta_kvapi::kvapi::MGetKVReply;
use databend_common_meta_kvapi::kvapi::UpsertKVReply;
use databend_common_meta_kvapi::kvapi::UpsertKVReq;
use databend_common_meta_types::protobuf::WatchRequest;
use databend_common_meta_types::protobuf::WatchResponse;
use databend_common_meta_types::MetaError;
use databend_common_meta_types::TxnReply;
use databend_common_meta_types::TxnRequest;
use log::as_debug;
use log::info;
use tokio_stream::Stream;

pub type WatchStream =
    Pin<Box<dyn Stream<Item = Result<WatchResponse, MetaError>> + Send + 'static>>;

#[derive(Clone)]
pub struct MetaStoreProvider {
    rpc_conf: RpcClientConf,
}

/// MetaStore is impl with either a local embedded meta store, or a grpc-client of metasrv
#[derive(Clone)]
pub enum MetaStore {
    L(Arc<MetaEmbedded>),
    R(Arc<ClientHandle>),
    E(Arc<EtcdRsClientWrapper>),
}

impl MetaStore {
    pub fn arc(self) -> Arc<Self> {
        Arc::new(self)
    }

    pub fn is_local(&self) -> bool {
        match self {
            MetaStore::L(_) => true,
            _ => false,
        }
    }

    pub async fn get_local_addr(&self) -> std::result::Result<Option<String>, MetaError> {
        match self {
            MetaStore::L(_) => Ok(None),
            MetaStore::R(grpc_client) => {
                let client_info = grpc_client.get_client_info().await?;
                Ok(Some(client_info.client_addr))
            }
            _ => {
                todo!()
            }
        }
    }

    pub async fn watch(&self, request: WatchRequest) -> Result<WatchStream, MetaError> {
        match self {
            MetaStore::L(_) => unreachable!(),
            MetaStore::R(grpc_client) => {
                let streaming = grpc_client.request(request).await?;
                Ok(Box::pin(WatchResponseStream::create(streaming)))
            }
            MetaStore::E(_) => {
                todo!()
            }
        }
    }
}

#[async_trait::async_trait]
impl kvapi::KVApi for MetaStore {
    type Error = MetaError;

    async fn upsert_kv(&self, act: UpsertKVReq) -> Result<UpsertKVReply, MetaError> {
        match self {
            MetaStore::L(x) => x.upsert_kv(act).await,
            MetaStore::R(x) => x.upsert_kv(act).await,
            MetaStore::E(x) => x.upsert_kv(act).await,
        }
    }

    async fn get_kv(&self, key: &str) -> Result<GetKVReply, MetaError> {
        match self {
            MetaStore::L(x) => x.get_kv(key).await,
            MetaStore::R(x) => x.get_kv(key).await,
            MetaStore::E(x) => x.get_kv(key).await,
        }
    }

    async fn mget_kv(&self, key: &[String]) -> Result<MGetKVReply, MetaError> {
        match self {
            MetaStore::L(x) => x.mget_kv(key).await,
            MetaStore::R(x) => x.mget_kv(key).await,
            MetaStore::E(x) => x.mget_kv(key).await,
        }
    }

    async fn list_kv(&self, prefix: &str) -> Result<KVStream<MetaError>, MetaError> {
        match self {
            MetaStore::L(x) => x.list_kv(prefix).await,
            MetaStore::R(x) => x.list_kv(prefix).await,
            MetaStore::E(x) => x.list_kv(prefix).await,
        }
    }

    async fn transaction(&self, txn: TxnRequest) -> Result<TxnReply, MetaError> {
        match self {
            MetaStore::L(x) => x.transaction(txn).await,
            MetaStore::R(x) => x.transaction(txn).await,
            MetaStore::E(x) => x.transaction(txn).await,
        }
    }
}

impl MetaStoreProvider {
    pub fn new(rpc_conf: RpcClientConf) -> Self {
        MetaStoreProvider { rpc_conf }
    }

    pub async fn create_meta_store(&self) -> Result<MetaStore, MetaError> {
        if self.rpc_conf.local_mode() {
            info!(
                conf = as_debug!(&self.rpc_conf);
                "use embedded meta, data will be removed when process exits"
            );

            // NOTE: This can only be used for test: data will be removed when program quit.
            let meta_store = MetaEmbedded::get_meta().await?;
            Ok(MetaStore::L(meta_store))
        } else {
            info!(conf = as_debug!(&self.rpc_conf); "use remote meta");
            // let client = MetaGrpcClient::try_new(&self.rpc_conf)?;
            //            let eps: Vec<_> = self.rpc_conf.endpoints.iter().map(|e| e.into()).collect();
            let eps = vec!["http://127.0.0.1:2379".into()];
            let client = etcd_rs::Client::connect(etcd_rs::ClientConfig::new(eps))
                .await
                .unwrap();
            Ok(MetaStore::E(Arc::new(EtcdRsClientWrapper::new(client))))
        }
    }
}

pub struct WatchResponseStream<E, S>
where
    E: Into<MetaError> + Send + 'static,
    S: Stream<Item = Result<WatchResponse, E>> + Send + Unpin + 'static,
{
    inner: S,
}

impl<E, S> WatchResponseStream<E, S>
where
    E: Into<MetaError> + Send + 'static,
    S: Stream<Item = Result<WatchResponse, E>> + Send + Unpin + 'static,
{
    pub fn create(inner: S) -> WatchResponseStream<E, S> {
        WatchResponseStream { inner }
    }
}

impl<E, S> Stream for WatchResponseStream<E, S>
where
    E: Into<MetaError> + Send + 'static,
    S: Stream<Item = Result<WatchResponse, E>> + Send + Unpin + 'static,
{
    type Item = Result<WatchResponse, MetaError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx).map(|x| match x {
            None => None,
            Some(Ok(resp)) => Some(Ok(resp)),
            Some(Err(e)) => Some(Err(e.into())),
        })
    }
}
