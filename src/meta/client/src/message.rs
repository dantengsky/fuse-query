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

use std::fmt;
use std::fmt::Formatter;

use common_base::base::tokio::sync::oneshot::Sender;
use common_meta_kvapi::kvapi::GetKVReply;
use common_meta_kvapi::kvapi::GetKVReq;
use common_meta_kvapi::kvapi::ListKVReply;
use common_meta_kvapi::kvapi::ListKVReq;
use common_meta_kvapi::kvapi::MGetKVReply;
use common_meta_kvapi::kvapi::MGetKVReq;
use common_meta_kvapi::kvapi::UpsertKVReply;
use common_meta_kvapi::kvapi::UpsertKVReq;
use common_meta_types::protobuf::ClientInfo;
use common_meta_types::protobuf::ClusterStatus;
use common_meta_types::protobuf::ExportedChunk;
use common_meta_types::protobuf::StreamItem;
use common_meta_types::protobuf::WatchRequest;
use common_meta_types::protobuf::WatchResponse;
use common_meta_types::MetaClientError;
use common_meta_types::MetaError;
use common_meta_types::TxnReply;
use common_meta_types::TxnRequest;
use minitrace::Span;
use tonic::codegen::BoxStream;

use crate::grpc_client::RealClient;

/// A request that is sent by a meta-client handle to its worker.
pub struct ClientWorkerRequest {
    pub(crate) request_id: u64,

    /// For sending back the response to the handle.
    pub(crate) resp_tx: Sender<Response>,

    /// Request body
    pub(crate) req: Request,

    /// Tracing span for this request
    pub(crate) span: Span,
}

impl fmt::Debug for ClientWorkerRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientWorkerRequest")
            .field("request_id", &self.request_id)
            .field("req", &self.req)
            .finish()
    }
}

/// Mark an RPC to return a stream.
#[derive(Debug, Clone)]
pub struct Streamed<T>(pub T);

impl<T> Streamed<T> {
    pub fn into_inner(self) -> T {
        self.0
    }
}

/// Meta-client handle-to-worker request body
#[derive(Debug, Clone, derive_more::From)]
pub enum Request {
    /// Get KV
    Get(GetKVReq),

    /// Get multiple KV
    MGet(MGetKVReq),

    /// List KVs by key prefix
    List(ListKVReq),

    /// Get KV, returning a stream
    StreamGet(Streamed<GetKVReq>),

    /// Get multiple KV, returning a stream.
    StreamMGet(Streamed<MGetKVReq>),

    /// List KVs by key prefix, returning a stream.
    StreamList(Streamed<ListKVReq>),

    /// Update or insert KV
    Upsert(UpsertKVReq),

    /// Run a transaction on remote
    Txn(TxnRequest),

    /// Watch KV changes, expecting a Stream that reports KV change events
    Watch(WatchRequest),

    /// Export all data
    Export(ExportReq),

    /// Get a initialized grpc-client
    MakeClient(MakeClient),

    /// Get endpoints, for test
    GetEndpoints(GetEndpoints),

    /// Get cluster status, for metactl
    GetClusterStatus(GetClusterStatus),

    /// Get info about the client
    GetClientInfo(GetClientInfo),
}

impl Request {
    pub fn name(&self) -> &'static str {
        match self {
            Request::Get(_) => "Get",
            Request::MGet(_) => "MGet",
            Request::List(_) => "List",
            Request::StreamGet(_) => "StreamGet",
            Request::StreamMGet(_) => "StreamMGet",
            Request::StreamList(_) => "StreamList",
            Request::Upsert(_) => "Upsert",
            Request::Txn(_) => "Txn",
            Request::Watch(_) => "Watch",
            Request::Export(_) => "Export",
            Request::MakeClient(_) => "MakeClient",
            Request::GetEndpoints(_) => "GetEndpoints",
            Request::GetClusterStatus(_) => "GetClusterStatus",
            Request::GetClientInfo(_) => "GetClientInfo",
        }
    }
}

/// Meta-client worker-to-handle response body
#[derive(derive_more::TryInto)]
pub enum Response {
    Get(Result<GetKVReply, MetaError>),
    MGet(Result<MGetKVReply, MetaError>),
    List(Result<ListKVReply, MetaError>),
    StreamGet(Result<BoxStream<StreamItem>, MetaError>),
    StreamMGet(Result<BoxStream<StreamItem>, MetaError>),
    StreamList(Result<BoxStream<StreamItem>, MetaError>),
    Upsert(Result<UpsertKVReply, MetaError>),
    Txn(Result<TxnReply, MetaError>),
    Watch(Result<tonic::codec::Streaming<WatchResponse>, MetaError>),
    Export(Result<tonic::codec::Streaming<ExportedChunk>, MetaError>),
    MakeClient(Result<(RealClient, u64), MetaClientError>),
    GetEndpoints(Result<Vec<String>, MetaError>),
    GetClusterStatus(Result<ClusterStatus, MetaError>),
    GetClientInfo(Result<ClientInfo, MetaError>),
}

impl fmt::Debug for Response {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Response::Get(x) => {
                write!(f, "Get({:?})", x)
            }
            Response::MGet(x) => {
                write!(f, "MGet({:?})", x)
            }
            Response::List(x) => {
                write!(f, "List({:?})", x)
            }
            Response::StreamGet(x) => {
                write!(f, "StreamGet({:?})", x.as_ref().map(|_s| "<stream>"))
            }
            Response::StreamMGet(x) => {
                write!(f, "StreamMGet({:?})", x.as_ref().map(|_s| "<stream>"))
            }
            Response::StreamList(x) => {
                write!(f, "StreamList({:?})", x.as_ref().map(|_s| "<stream>"))
            }
            Response::Upsert(x) => {
                write!(f, "Upsert({:?})", x)
            }
            Response::Txn(x) => {
                write!(f, "Txn({:?})", x)
            }
            Response::Watch(x) => {
                write!(f, "Watch({:?})", x)
            }
            Response::Export(x) => {
                write!(f, "Export({:?})", x)
            }
            Response::MakeClient(x) => {
                write!(f, "MakeClient({:?})", x)
            }
            Response::GetEndpoints(x) => {
                write!(f, "GetEndpoints({:?})", x)
            }
            Response::GetClusterStatus(x) => {
                write!(f, "GetClusterStatus({:?})", x)
            }
            Response::GetClientInfo(x) => {
                write!(f, "GetClientInfo({:?})", x)
            }
        }
    }
}

impl Response {
    pub fn err(&self) -> Option<&(dyn std::error::Error + 'static)> {
        let e = match self {
            Response::Get(res) => res
                .as_ref()
                .err()
                .map(|x| x as &(dyn std::error::Error + 'static)),
            Response::MGet(res) => res
                .as_ref()
                .err()
                .map(|x| x as &(dyn std::error::Error + 'static)),
            Response::List(res) => res
                .as_ref()
                .err()
                .map(|x| x as &(dyn std::error::Error + 'static)),
            Response::StreamGet(res) => res
                .as_ref()
                .err()
                .map(|x| x as &(dyn std::error::Error + 'static)),
            Response::StreamMGet(res) => res
                .as_ref()
                .err()
                .map(|x| x as &(dyn std::error::Error + 'static)),
            Response::StreamList(res) => res
                .as_ref()
                .err()
                .map(|x| x as &(dyn std::error::Error + 'static)),
            Response::Upsert(res) => res
                .as_ref()
                .err()
                .map(|x| x as &(dyn std::error::Error + 'static)),
            Response::Txn(res) => res
                .as_ref()
                .err()
                .map(|x| x as &(dyn std::error::Error + 'static)),
            Response::Watch(res) => res
                .as_ref()
                .err()
                .map(|x| x as &(dyn std::error::Error + 'static)),
            Response::Export(res) => res
                .as_ref()
                .err()
                .map(|x| x as &(dyn std::error::Error + 'static)),
            Response::MakeClient(res) => res
                .as_ref()
                .err()
                .map(|x| x as &(dyn std::error::Error + 'static)),
            Response::GetEndpoints(res) => res
                .as_ref()
                .err()
                .map(|x| x as &(dyn std::error::Error + 'static)),
            Response::GetClusterStatus(res) => res
                .as_ref()
                .err()
                .map(|x| x as &(dyn std::error::Error + 'static)),
            Response::GetClientInfo(res) => res
                .as_ref()
                .err()
                .map(|x| x as &(dyn std::error::Error + 'static)),
        };
        e
    }
}

/// Export all data stored in metasrv
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct ExportReq {}

/// Get a grpc-client that is initialized and has passed handshake
///
/// This request is only used internally or for testing purpose.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct MakeClient {}

/// Get all meta server endpoints
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct GetEndpoints {}

/// Get cluster status
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct GetClusterStatus {}

/// Get info about client
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct GetClientInfo {}
