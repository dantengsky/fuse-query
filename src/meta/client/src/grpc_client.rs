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

use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use common_arrow::arrow_format::flight::data::BasicAuth;
use common_base::base::tokio::select;
use common_base::base::tokio::sync::mpsc;
use common_base::base::tokio::sync::mpsc::Receiver;
use common_base::base::tokio::sync::mpsc::Sender;
use common_base::base::tokio::sync::oneshot;
use common_base::base::tokio::sync::oneshot::Receiver as OneRecv;
use common_base::base::tokio::sync::oneshot::Sender as OneSend;
use common_base::base::tokio::time::sleep;
use common_base::containers::ItemManager;
use common_base::containers::Pool;
use common_base::containers::TtlHashMap;
use common_base::future::TimingFutureExt;
use common_base::runtime::Runtime;
use common_base::runtime::TrySpawn;
use common_base::runtime::UnlimitedFuture;
use common_base::GLOBAL_TASK;
use common_grpc::ConnectionFactory;
use common_grpc::GrpcConnectionError;
use common_grpc::RpcClientConf;
use common_grpc::RpcClientTlsConfig;
use common_meta_api::reply::reply_to_api_result;
use common_meta_kvapi::kvapi::ListKVReply;
use common_meta_types::anyerror::AnyError;
use common_meta_types::protobuf as pb;
use common_meta_types::protobuf::meta_service_client::MetaServiceClient;
use common_meta_types::protobuf::ClientInfo;
use common_meta_types::protobuf::ClusterStatus;
use common_meta_types::protobuf::Empty;
use common_meta_types::protobuf::ExportedChunk;
use common_meta_types::protobuf::HandshakeRequest;
use common_meta_types::protobuf::MemberListReply;
use common_meta_types::protobuf::MemberListRequest;
use common_meta_types::protobuf::RaftRequest;
use common_meta_types::protobuf::WatchRequest;
use common_meta_types::protobuf::WatchResponse;
use common_meta_types::ConnectionError;
use common_meta_types::GrpcConfig;
use common_meta_types::MetaClientError;
use common_meta_types::MetaError;
use common_meta_types::MetaHandshakeError;
use common_meta_types::MetaNetworkError;
use common_meta_types::TxnReply;
use common_meta_types::TxnRequest;
use futures::stream::StreamExt;
use log::as_debug;
use log::as_display;
use log::debug;
use log::error;
use log::info;
use log::warn;
use minitrace::full_name;
use minitrace::future::FutureExt;
use minitrace::Span;
use once_cell::sync::OnceCell;
use parking_lot::Mutex;
use prost::Message;
use semver::Version;
use serde::de::DeserializeOwned;
use tonic::async_trait;
use tonic::client::GrpcService;
use tonic::codegen::BoxStream;
use tonic::codegen::InterceptedService;
use tonic::metadata::MetadataValue;
use tonic::service::Interceptor;
use tonic::transport::Channel;
use tonic::Code;
use tonic::Request;
use tonic::Status;

use crate::from_digit_ver;
use crate::grpc_action::RequestFor;
use crate::grpc_metrics;
use crate::message;
use crate::to_digit_ver;
use crate::MetaGrpcReadReq;
use crate::MetaGrpcReq;
use crate::METACLI_COMMIT_SEMVER;
use crate::MIN_METASRV_SEMVER;

const RPC_RETRIES: usize = 2;
const AUTH_TOKEN_KEY: &str = "auth-token-bin";

pub(crate) type RealClient = MetaServiceClient<InterceptedService<Channel, AuthInterceptor>>;

#[derive(Debug)]
struct MetaChannelManager {
    timeout: Option<Duration>,
    conf: Option<RpcClientTlsConfig>,
}

impl MetaChannelManager {
    async fn build_channel(&self, addr: &String) -> Result<Channel, MetaNetworkError> {
        let ch = ConnectionFactory::create_rpc_channel(addr, self.timeout, self.conf.clone())
            .await
            .map_err(|e| match e {
                GrpcConnectionError::InvalidUri { .. } => MetaNetworkError::BadAddressFormat(
                    AnyError::new(&e).add_context(|| "while creating rpc channel"),
                ),
                GrpcConnectionError::TLSConfigError { .. } => MetaNetworkError::TLSConfigError(
                    AnyError::new(&e).add_context(|| "while creating rpc channel"),
                ),
                GrpcConnectionError::CannotConnect { .. } => MetaNetworkError::ConnectionError(
                    ConnectionError::new(e, "while creating rpc channel"),
                ),
            })?;
        Ok(ch)
    }
}

#[async_trait]
impl ItemManager for MetaChannelManager {
    type Key = String;
    type Item = Channel;
    type Error = MetaNetworkError;

    #[logcall::logcall(err = "debug")]
    #[minitrace::trace]
    async fn build(&self, addr: &Self::Key) -> Result<Self::Item, Self::Error> {
        self.build_channel(addr).await
    }

    #[logcall::logcall(err = "debug")]
    #[minitrace::trace]
    async fn check(&self, mut ch: Self::Item) -> Result<Self::Item, Self::Error> {
        futures::future::poll_fn(|cx| ch.poll_ready(cx))
            .await
            .map_err(|e| {
                MetaNetworkError::ConnectionError(ConnectionError::new(e, "while check item"))
            })?;
        Ok(ch)
    }
}

/// A handle to access meta-client worker.
/// The worker will be actually running in a dedicated runtime: `MetaGrpcClient.rt`.
pub struct ClientHandle {
    /// For sending request to meta-client worker.
    pub(crate) req_tx: Sender<message::ClientWorkerRequest>,
    /// Notify auto sync to stop.
    /// `oneshot::Receiver` impl `Drop` by sending a closed notification to the `Sender` half.
    #[allow(dead_code)]
    cancel_auto_sync_rx: OneRecv<()>,
}

impl ClientHandle {
    /// Send a request to the internal worker task, which may be running in another runtime.
    #[minitrace::trace]
    pub async fn request<Req, E>(&self, req: Req) -> Result<Req::Reply, E>
    where
        Req: RequestFor,
        Req: Into<message::Request>,
        Result<Req::Reply, E>: TryFrom<message::Response>,
        <Result<Req::Reply, E> as TryFrom<message::Response>>::Error: std::fmt::Display,
        E: From<MetaClientError> + Debug,
    {
        static META_REQUEST_ID: AtomicU64 = AtomicU64::new(1);

        let request_future = async move {
            let (tx, rx) = oneshot::channel();
            let req = message::ClientWorkerRequest {
                request_id: META_REQUEST_ID.fetch_add(1, Ordering::Relaxed),
                resp_tx: tx,
                req: req.into(),
                span: Span::enter_with_local_parent(std::any::type_name::<
                    message::ClientWorkerRequest,
                >()),
            };

            debug!(
                request = as_debug!(&req);
                "Meta ClientHandle send request to meta client worker"
            );

            grpc_metrics::incr_meta_grpc_client_request_inflight(1);

            let res = self.req_tx.send(req).await.map_err(|e| {
                let cli_err = MetaClientError::ClientRuntimeError(
                    AnyError::new(&e).add_context(|| "when sending req to MetaGrpcClient worker"),
                );
                cli_err.into()
            });

            if let Err(err) = res {
                grpc_metrics::incr_meta_grpc_client_request_inflight(-1);

                error!(
                    error = as_debug!(&err);
                    "Meta ClientHandle send request to meta client worker failed"
                );

                return Err(err);
            }

            let res = rx.await.map_err(|e| {
                grpc_metrics::incr_meta_grpc_client_request_inflight(-1);

                error!(
                    error = as_debug!(&e);
                    "Meta ClientHandle recv response from meta client worker failed"
                );

                MetaClientError::ClientRuntimeError(
                    AnyError::new(&e).add_context(|| "when recv resp from MetaGrpcClient worker"),
                )
            })?;

            grpc_metrics::incr_meta_grpc_client_request_inflight(-1);
            let res: Result<Req::Reply, E> = res
                .try_into()
                .map_err(|e| {
                    format!(
                        "expect: {}, got: {}",
                        std::any::type_name::<Req::Reply>(),
                        e
                    )
                })
                .unwrap();

            res
        };

        UnlimitedFuture::create(request_future).await
    }

    pub async fn get_cluster_status(&self) -> Result<ClusterStatus, MetaError> {
        self.request(message::GetClusterStatus {}).await
    }

    pub async fn get_client_info(&self) -> Result<ClientInfo, MetaError> {
        self.request(message::GetClientInfo {}).await
    }

    pub async fn make_client(&self) -> Result<(RealClient, u64), MetaClientError> {
        self.request(message::MakeClient {}).await
    }

    /// Return the endpoints list cached on this client.
    pub async fn get_cached_endpoints(&self) -> Result<Vec<String>, MetaError> {
        self.request(message::GetEndpoints {}).await
    }
}

// TODO: maybe it just needs a runtime, not a MetaGrpcClientWorker.
//
/// Meta grpc client has a internal worker task that deals with all traffic to remote meta service.
///
/// We expect meta-client should be cloneable.
/// But the underlying hyper client has a worker that runs in its creating tokio-runtime.
/// Thus a cloned meta client may try to talk to a destroyed hyper worker if the creating tokio-runtime is dropped.
/// Thus we have to guarantee that as long as there is a meta-client, the hyper worker runtime must not be dropped.
/// Thus a meta client creates a runtime then spawn a MetaGrpcClientWorker.
pub struct MetaGrpcClient {
    conn_pool: Pool<MetaChannelManager>,
    endpoints: Mutex<Vec<String>>,
    username: String,
    password: String,
    current_endpoint: Arc<Mutex<Option<String>>>,
    unhealthy_endpoints: Mutex<TtlHashMap<String, ()>>,
    auto_sync_interval: Option<Duration>,

    /// Dedicated runtime to support meta client background tasks.
    ///
    /// In order not to let a blocking operation(such as calling the new PipelinePullingExecutor) in a tokio runtime block meta-client background tasks.
    /// If a background task is blocked, no meta-client will be able to proceed if meta-client is reused.
    ///
    /// Note that a thread_pool tokio runtime does not help: a scheduled tokio-task resides in `filo_slot` won't be stolen by other tokio-workers.
    #[allow(dead_code)]
    rt: Arc<Runtime>,
}

impl Debug for MetaGrpcClient {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut de = f.debug_struct("MetaGrpcClient");
        de.field("endpoints", &self.endpoints);
        de.field("current_endpoints", &self.current_endpoint);
        de.field("unhealthy_endpoints", &self.unhealthy_endpoints);
        de.field("auto_sync_interval", &self.auto_sync_interval);
        de.finish()
    }
}

impl MetaGrpcClient {
    /// Create a new client of metasrv.
    ///
    /// It creates a new `Runtime` and spawn a background worker task in it that do all the RPC job.
    /// A client-handle is returned to communicate with the worker.
    ///
    /// Thus the real work is done in the dedicated runtime to avoid the client spawning tasks in the caller's runtime, which potentially leads to a deadlock if the caller has blocking calls to other components
    /// Because `tower` and `hyper` will spawn tasks when handling RPCs.
    ///
    /// The worker is a singleton and the returned handle is cheap to clone.
    /// When all handles are dropped the worker will quit, then the runtime will be destroyed.
    pub fn try_new(conf: &RpcClientConf) -> Result<Arc<ClientHandle>, MetaClientError> {
        Self::try_create(
            conf.get_endpoints(),
            &conf.username,
            &conf.password,
            conf.timeout,
            conf.auto_sync_interval,
            conf.unhealthy_endpoint_evict_time,
            conf.tls_conf.clone(),
        )
    }

    #[minitrace::trace]
    pub fn try_create(
        endpoints: Vec<String>,
        username: &str,
        password: &str,
        timeout: Option<Duration>,
        auto_sync_interval: Option<Duration>,
        unhealthy_endpoint_evict_time: Duration,
        conf: Option<RpcClientTlsConfig>,
    ) -> Result<Arc<ClientHandle>, MetaClientError> {
        Self::endpoints_non_empty(&endpoints)?;

        let mgr = MetaChannelManager { timeout, conf };

        let rt =
            Runtime::with_worker_threads(1, Some("meta-client-rt".to_string())).map_err(|e| {
                MetaClientError::ClientRuntimeError(
                    AnyError::new(&e).add_context(|| "when creating meta-client"),
                )
            })?;
        let rt = Arc::new(rt);

        // Build the handle-worker pair

        let (tx, rx) = mpsc::channel(256);
        let (one_tx, one_rx) = oneshot::channel::<()>();

        let handle = Arc::new(ClientHandle {
            req_tx: tx,
            cancel_auto_sync_rx: one_rx,
        });

        let worker = Arc::new(Self {
            conn_pool: Pool::new(mgr, Duration::from_millis(50)),
            endpoints: Mutex::new(endpoints),
            current_endpoint: Arc::new(Mutex::new(None)),
            unhealthy_endpoints: Mutex::new(TtlHashMap::new(unhealthy_endpoint_evict_time)),
            auto_sync_interval,
            username: username.to_string(),
            password: password.to_string(),
            rt: rt.clone(),
        });

        rt.spawn(
            GLOBAL_TASK,
            UnlimitedFuture::create(Self::worker_loop(worker.clone(), rx)),
        );
        rt.spawn(
            GLOBAL_TASK,
            UnlimitedFuture::create(Self::auto_sync_endpoints(worker, one_tx)),
        );

        Ok(handle)
    }

    /// A worker runs a receiving-loop to accept user-request to metasrv and deals with request in the dedicated runtime.
    #[minitrace::trace]
    async fn worker_loop(self: Arc<Self>, mut req_rx: Receiver<message::ClientWorkerRequest>) {
        info!("MetaGrpcClient::worker spawned");

        loop {
            let t = req_rx.recv().await;
            let req = match t {
                None => {
                    info!("MetaGrpcClient handle closed. worker quit");
                    return;
                }
                Some(x) => x,
            };

            let span = Span::enter_with_parent(full_name!(), &req.span);

            if req.resp_tx.is_closed() {
                debug!(
                    req = as_debug!(&req);
                    "MetaGrpcClient request.resp_tx is closed, cancel handling this request"
                );
                continue;
            }

            let request_id = req.request_id;
            let resp_tx = req.resp_tx;
            let req = req.req;
            let req_name = req.name();
            let req_str = format!("{:?}", req);

            // Deal with non-RPC request
            #[allow(clippy::single_match)]
            match req {
                message::Request::GetEndpoints(_) => {
                    let endpoints = self.get_cached_endpoints();
                    let resp = message::Response::GetEndpoints(Ok(endpoints));
                    Self::send_response(resp_tx, request_id, resp);
                    continue;
                }
                _ => {}
            }

            async {
                debug!(req = as_debug!(&req); "MetaGrpcClient recv request");

                // Deal with remote RPC request

                let start = Instant::now();
                let resp = match req {
                    message::Request::Get(r) => {
                        let resp = self
                            .kv_api(r)
                            .timed_ge(threshold(), info_spent("MetaGrpcClient::kv_api"))
                            .await;
                        message::Response::Get(resp)
                    }
                    message::Request::StreamGet(r) => {
                        let strm = self
                            .kv_read_v1(MetaGrpcReadReq::GetKV(r.into_inner()))
                            .timed_ge(threshold(), info_spent("MetaGrpcClient::kv_read_v1(GetKV)"))
                            .await;
                        message::Response::StreamGet(strm)
                    }
                    message::Request::MGet(r) => {
                        let resp = self
                            .kv_api(r)
                            .timed_ge(threshold(), info_spent("MetaGrpcClient::kv_api"))
                            .await;
                        message::Response::MGet(resp)
                    }
                    message::Request::StreamMGet(r) => {
                        let strm = self
                            .kv_read_v1(MetaGrpcReadReq::MGetKV(r.into_inner()))
                            .timed_ge(
                                threshold(),
                                info_spent("MetaGrpcClient::kv_read_v1(MGetKV)"),
                            )
                            .await;
                        message::Response::StreamMGet(strm)
                    }
                    message::Request::List(r) => {
                        let resp = self
                            .kv_api(r)
                            .timed_ge(threshold(), info_spent("MetaGrpcClient::kv_api"))
                            .await;
                        message::Response::List(resp)
                    }
                    message::Request::StreamList(r) => {
                        let strm = self
                            .kv_read_v1(MetaGrpcReadReq::ListKV(r.into_inner()))
                            .timed_ge(
                                threshold(),
                                info_spent("MetaGrpcClient::kv_read_v1(ListKV)"),
                            )
                            .await;
                        message::Response::StreamMGet(strm)
                    }
                    message::Request::Upsert(r) => {
                        let resp = self
                            .kv_api(r)
                            .timed_ge(threshold(), info_spent("MetaGrpcClient::kv_api"))
                            .await;
                        message::Response::Upsert(resp)
                    }
                    message::Request::Txn(r) => {
                        let resp = self
                            .transaction(r)
                            .timed_ge(threshold(), info_spent("MetaGrpcClient::transaction"))
                            .await;
                        message::Response::Txn(resp)
                    }
                    message::Request::Watch(r) => {
                        let resp = self.watch(r).await;
                        message::Response::Watch(resp)
                    }
                    message::Request::Export(r) => {
                        let resp = self.export(r).await;
                        message::Response::Export(resp)
                    }
                    message::Request::MakeClient(_) => {
                        let resp = self.make_client().await;
                        message::Response::MakeClient(resp)
                    }
                    message::Request::GetEndpoints(_) => {
                        unreachable!("handled above");
                    }
                    message::Request::GetClusterStatus(_) => {
                        let resp = self.get_cluster_status().await;
                        message::Response::GetClusterStatus(resp)
                    }
                    message::Request::GetClientInfo(_) => {
                        let resp = self.get_client_info().await;
                        message::Response::GetClientInfo(resp)
                    }
                };

                self.update_rpc_metrics(req_name, &req_str, request_id, start, resp.err());

                Self::send_response(resp_tx, request_id, resp);
            }
            .in_span(span)
            .await
        }
    }

    fn send_response(tx: OneSend<message::Response>, request_id: u64, resp: message::Response) {
        debug!(
            request_id = as_debug!(&request_id),
            resp = as_debug!(&resp);
            "MetaGrpcClient send response to the handle"
        );

        let send_res = tx.send(resp);
        if let Err(err) = send_res {
            error!(
                request_id = as_display!(request_id),
                err = as_debug!(&err);
                "MetaGrpcClient failed to send response to the handle. recv-end closed"
            );
        }
    }

    fn update_rpc_metrics(
        &self,
        req_name: &'static str,
        req_str: &str,
        request_id: u64,
        start: Instant,
        resp_err: Option<&(dyn std::error::Error + 'static)>,
    ) {
        let current_endpoint = self.get_current_endpoint();

        let Some(endpoint) = current_endpoint else {
            return;
        };

        // Duration metrics
        {
            let elapsed = start.elapsed().as_millis() as f64;
            grpc_metrics::record_meta_grpc_client_request_duration_ms(&endpoint, req_name, elapsed);

            if elapsed > 1000_f64 {
                warn!(
                    request_id = as_display!(request_id);
                    "MetaGrpcClient slow request {} to {} takes {} ms: {}",
                    req_name,
                    endpoint,
                    elapsed,
                    req_str,
                );
            }
        }

        // Error metrics
        if let Some(err) = resp_err {
            grpc_metrics::incr_meta_grpc_client_request_failed(&endpoint, req_name, err);
            error!(
                request_id = as_display!(request_id);
                "MetaGrpcClient error: {:?}", err
            );
        } else {
            grpc_metrics::incr_meta_grpc_client_request_success(&endpoint, req_name);
        }
    }

    /// Return a client for communication, and a server version in form of `{major:03}.{minor:03}.{patch:03}`.
    #[minitrace::trace]
    pub async fn make_client(&self) -> Result<(RealClient, u64), MetaClientError> {
        let all_endpoints = self.get_cached_endpoints();
        debug!("meta-service all endpoints: {:?}", all_endpoints);
        debug_assert!(!all_endpoints.is_empty());

        // Filter out unhealthy endpoints
        let endpoints = {
            let mut endpoints = all_endpoints.clone();

            let unhealthy = self.unhealthy_endpoints.lock();
            endpoints.retain(|e| !unhealthy.contains_key(e));
            endpoints
        };

        debug!("healthy endpoints: {:?}", &endpoints);

        let endpoints = if endpoints.is_empty() {
            warn!(
                "meta-service has no healthy endpoints, force using all(healthy or not) endpoints: {:?}",
                all_endpoints
            );
            all_endpoints.clone()
        } else {
            debug!("meta-service healthy endpoints: {:?}", endpoints);
            endpoints
        };

        let mut last_err = None;

        for addr in endpoints.iter() {
            self.set_current_endpoint(addr);

            let chan_res = self.make_channel(addr).await;
            let chan = match chan_res {
                Ok(chan) => chan,
                Err(net_err) => {
                    warn!("{} when make_channel to {}", net_err, addr);
                    self.mark_current_endpoint_unhealthy();

                    let cli_err = MetaClientError::NetworkError(net_err);
                    last_err = Some(cli_err);
                    continue;
                }
            };

            let (mut client, once) = Self::new_real_client(chan);

            let handshake_res = Self::handshake(
                &mut client,
                &METACLI_COMMIT_SEMVER,
                &MIN_METASRV_SEMVER,
                &self.username,
                &self.password,
            )
            .await;

            let (token, server_version) = match handshake_res {
                Ok(x) => x,
                Err(handshake_err) => {
                    warn!("handshake error when make client: {:?}", handshake_err);
                    self.mark_current_endpoint_unhealthy();

                    let cli_err = MetaClientError::HandshakeError(handshake_err);
                    last_err = Some(cli_err);
                    continue;
                }
            };

            // Update the token for the client interceptor.
            // Safe unwrap(): it is the first time setting it.
            once.set(token).unwrap();

            return Ok((client, server_version));
        }

        if let Some(e) = last_err {
            return Err(e);
        }

        let conn_err = ConnectionError::new(
            AnyError::error(format!(
                "healthy endpoints: {:?}; all endpoints: {:?}",
                endpoints, all_endpoints
            )),
            "no endpoints to connect",
        );

        Err(MetaClientError::NetworkError(
            MetaNetworkError::ConnectionError(conn_err),
        ))
    }

    #[minitrace::trace]
    async fn make_channel(&self, addr: &String) -> Result<Channel, MetaNetworkError> {
        debug!("make_channel to {}", addr);

        let ch = self.conn_pool.get(addr).await;

        if let Err(ref e) = ch {
            warn!(
                "grpc_client create channel with {} failed, err: {:?}",
                addr, e
            );
            grpc_metrics::incr_meta_grpc_make_client_fail(addr);
        }

        ch
    }

    pub fn endpoints_non_empty(endpoints: &[String]) -> Result<(), MetaClientError> {
        if endpoints.is_empty() {
            return Err(MetaClientError::ConfigError(AnyError::error(
                "endpoints is empty",
            )));
        }
        Ok(())
    }

    fn get_cached_endpoints(&self) -> Vec<String> {
        let eps = self.endpoints.lock();
        (*eps).clone()
    }

    #[minitrace::trace]
    pub async fn set_endpoints(&self, endpoints: Vec<String>) -> Result<(), MetaError> {
        Self::endpoints_non_empty(&endpoints)?;

        // Older meta nodes may not store endpoint information and need to be filtered out.
        let distinct_cnt = endpoints.iter().filter(|n| !(*n).is_empty()).count();

        // If the fetched endpoints are less than the majority of the current cluster, no replacement should occur.
        if distinct_cnt < endpoints.len() / 2 + 1 {
            warn!(
                "distinct endpoints small than majority of meta cluster nodes {}<{}, endpoints: {:?}",
                distinct_cnt,
                endpoints.len(),
                endpoints
            );
            return Ok(());
        }

        let mut eps = self.endpoints.lock();
        *eps = endpoints;
        Ok(())
    }

    #[minitrace::trace]
    pub async fn sync_endpoints(&self) -> Result<(), MetaError> {
        let (mut client, _sver) = self.make_client().await?;
        let result = client
            .member_list(Request::new(MemberListRequest {
                data: "".to_string(),
            }))
            .await;
        let endpoints: Result<MemberListReply, Status> = match result {
            Ok(r) => Ok(r.into_inner()),
            Err(s) => {
                if status_is_retryable(&s) {
                    self.mark_current_endpoint_unhealthy();
                    let (mut client, _sver) = self.make_client().await?;
                    let req = Request::new(MemberListRequest {
                        data: "".to_string(),
                    });
                    Ok(client.member_list(req).await?.into_inner())
                } else {
                    Err(s)
                }
            }
        };
        let result: Vec<String> = endpoints?.data;
        debug!("received meta endpoints: {:?}", result);

        self.set_endpoints(result).await?;
        Ok(())
    }

    async fn auto_sync_endpoints(self: Arc<Self>, mut cancel_tx: OneSend<()>) {
        info!(
            "start auto sync endpoints: interval: {:?}",
            self.auto_sync_interval
        );
        if let Some(interval) = self.auto_sync_interval {
            loop {
                select! {
                    _ = cancel_tx.closed() => {
                        return;
                    }
                    _ = sleep(interval) => {
                        let r = self.sync_endpoints().await;
                        if let Err(e) = r {
                            warn!("auto sync endpoints failed: {:?}", e);
                        }
                    }
                }
            }
        }
    }

    /// Create a MetaServiceClient with authentication interceptor
    ///
    /// The returned `OnceCell` is used to fill in a token for the interceptor.
    pub fn new_real_client(chan: Channel) -> (RealClient, Arc<OnceCell<Vec<u8>>>) {
        let once = Arc::new(OnceCell::new());

        let interceptor = AuthInterceptor {
            token: once.clone(),
        };

        let client = MetaServiceClient::with_interceptor(chan, interceptor)
            .max_decoding_message_size(GrpcConfig::MAX_DECODING_SIZE)
            .max_encoding_message_size(GrpcConfig::MAX_ENCODING_SIZE);

        (client, once)
    }

    /// Handshake with metasrv.
    ///
    /// - Check whether the versions of this client(`C`) and the remote metasrv(`S`) are compatible.
    /// - Authorize this client.
    ///
    /// ## Check compatibility
    ///
    /// Both client `C` and  server `S` maintains two semantic-version:
    /// - `C` maintains the its own semver(`C.ver`) and the minimal compatible `S` semver(`C.min_srv_ver`).
    /// - `S` maintains the its own semver(`S.ver`) and the minimal compatible `S` semver(`S.min_cli_ver`).
    ///
    /// When handshaking:
    /// - `C` sends its ver `C.ver` to `S`,
    /// - When `S` receives handshake request, `S` asserts that `C.ver >= S.min_cli_ver`.
    /// - Then `S` replies handshake-reply with its `S.ver`.
    /// - When `C` receives the reply, `C` asserts that `S.ver >= C.min_srv_ver`.
    ///
    /// Handshake succeeds if both of these two assertions hold.
    ///
    /// E.g.:
    /// - `S: (ver=3, min_cli_ver=1)` is compatible with `C: (ver=3, min_srv_ver=2)`.
    /// - `S: (ver=4, min_cli_ver=4)` is **NOT** compatible with `C: (ver=3, min_srv_ver=2)`.
    ///   Because although `S.ver(4) >= C.min_srv_ver(3)` holds,
    ///   but `C.ver(3) >= S.min_cli_ver(4)` does not hold.
    ///
    /// ```text
    /// C.ver:    1             3      4
    /// C --------+-------------+------+------------>
    ///           ^      .------'      ^
    ///           |      |             |
    ///           '-------------.      |
    ///                  |      |      |
    ///                  v      |      |
    /// S ---------------+------+------+------------>
    /// S.ver:           2      3      4
    /// ```
    #[minitrace::trace]
    pub async fn handshake(
        client: &mut RealClient,
        client_ver: &Version,
        min_metasrv_ver: &Version,
        username: &str,
        password: &str,
    ) -> Result<(Vec<u8>, u64), MetaHandshakeError> {
        debug!(
            client_ver = as_display!(client_ver),
            min_metasrv_ver = as_display!(min_metasrv_ver);
            "client version"
        );

        let auth = BasicAuth {
            username: username.to_string(),
            password: password.to_string(),
        };
        let mut payload = vec![];

        // TODO: return MetaNetworkError
        auth.encode(&mut payload)
            .map_err(|e| MetaHandshakeError::new("encode auth payload", &e))?;

        let my_ver = to_digit_ver(client_ver);
        let req = Request::new(futures::stream::once(async move {
            HandshakeRequest {
                protocol_version: my_ver,
                payload,
            }
        }));

        // TODO: return MetaNetworkError
        let rx = client
            .handshake(req)
            .await
            .map_err(|e| MetaHandshakeError::new("when sending handshake rpc", &e))?;
        let mut rx = rx.into_inner();

        // TODO: return MetaNetworkError
        let res = rx.next().await.ok_or_else(|| {
            MetaHandshakeError::new(
                "when recv from handshake stream",
                &AnyError::error("handshake returns nothing"),
            )
        })?;

        let resp =
            res.map_err(|status| MetaHandshakeError::new("handshake is refused", &status))?;

        assert!(
            resp.protocol_version > 0,
            "talking to a very old databend-meta: upgrade databend-meta to at least 0.8"
        );

        let min_compatible = to_digit_ver(min_metasrv_ver);
        if resp.protocol_version < min_compatible {
            let invalid_err = AnyError::error(format!(
                "metasrv protocol_version({}) < meta-client min-compatible({})",
                from_digit_ver(resp.protocol_version),
                min_metasrv_ver,
            ));
            return Err(MetaHandshakeError::new(
                "incompatible protocol version",
                &invalid_err,
            ));
        }

        let token = resp.payload;
        let server_version = resp.protocol_version;

        Ok((token, server_version))
    }

    /// Create a watching stream that receives KV change events.
    #[minitrace::trace]
    pub(crate) async fn watch(
        &self,
        watch_request: WatchRequest,
    ) -> Result<tonic::codec::Streaming<WatchResponse>, MetaError> {
        debug!(
            watch_request = as_debug!(&watch_request);
            "MetaGrpcClient worker: handle watch request"
        );

        let (mut client, _sver) = self.make_client().await?;
        let res = client.watch(watch_request).await?;
        Ok(res.into_inner())
    }

    /// Export all data in json from metasrv.
    #[minitrace::trace]
    pub(crate) async fn export(
        &self,
        export_request: message::ExportReq,
    ) -> Result<tonic::codec::Streaming<ExportedChunk>, MetaError> {
        debug!(
            export_request = as_debug!(&export_request);
            "MetaGrpcClient worker: handle export request"
        );

        let (mut client, _sver) = self.make_client().await?;
        let res = client.export(Empty {}).await?;
        Ok(res.into_inner())
    }

    /// Get cluster status
    #[minitrace::trace]
    pub(crate) async fn get_cluster_status(&self) -> Result<ClusterStatus, MetaError> {
        debug!("MetaGrpcClient::get_cluster_status");

        let (mut client, _sver) = self.make_client().await?;
        let res = client.get_cluster_status(Empty {}).await?;
        Ok(res.into_inner())
    }

    /// Export all data in json from metasrv.
    #[minitrace::trace]
    pub(crate) async fn get_client_info(&self) -> Result<ClientInfo, MetaError> {
        debug!("MetaGrpcClient::get_client_info");

        let (mut client, _sver) = self.make_client().await?;
        let res = client.get_client_info(Empty {}).await?;
        Ok(res.into_inner())
    }

    #[minitrace::trace]
    pub(crate) async fn kv_api<T>(&self, v: T) -> Result<T::Reply, MetaError>
    where
        T: RequestFor,
        T: Into<MetaGrpcReq>,
        T::Reply: DeserializeOwned,
    {
        let grpc_req: MetaGrpcReq = v.into();

        debug!(
            req = as_debug!(&grpc_req);
            "MetaGrpcClient::kv_api request"
        );

        let raft_req: RaftRequest = grpc_req.into();

        let mut failures = vec![];

        for i in 0..RPC_RETRIES {
            let (mut client, _server_version) = self
                .make_client()
                .timed_ge(threshold(), info_spent("MetaGrpcClient::make_client"))
                .await?;

            let req = traced_req(raft_req.clone());

            let result = client
                .kv_api(req)
                .timed_ge(threshold(), info_spent("client::kv_api"))
                .await;

            debug!(
                result = as_debug!(&result);
                "MetaGrpcClient::kv_api result, {}-th try", i
            );

            if let Err(ref e) = result {
                if status_is_retryable(e) {
                    self.mark_current_endpoint_unhealthy();
                    failures.push(e.clone());
                    continue;
                }
            }

            let raft_reply = result?.into_inner();

            let resp: T::Reply = reply_to_api_result(raft_reply)?;
            return Ok(resp);
        }

        let net_err = MetaNetworkError::ConnectionError(ConnectionError::new(
            AnyError::error(format_args!(
                "failed after {} retries: {:?}",
                RPC_RETRIES, failures
            )),
            "failed to connect to meta-service",
        ));

        Err(net_err.into())
    }

    #[minitrace::trace]
    pub(crate) async fn kv_read_v1(
        &self,
        grpc_req: MetaGrpcReadReq,
    ) -> Result<BoxStream<pb::StreamItem>, MetaError> {
        debug!(
            req = as_debug!(&grpc_req);
            "MetaGrpcClient::kv_read_v1 request"
        );

        let mut failures = vec![];

        for i in 0..RPC_RETRIES {
            let (mut client, server_version) = self
                .make_client()
                .timed_ge(threshold(), info_spent("MetaGrpcClient::make_client"))
                .await?;

            // TODO: remove this fallback when MIN_METASRV_SEMVER is bumped to at least 1.2.163

            // 1.2.163
            // in 1.2.163, kv_read_v1() API is added
            let kv_read_v1_ver = 1002163;
            if server_version < kv_read_v1_ver {
                if let MetaGrpcReadReq::ListKV(list_req) = &grpc_req {
                    // Fallback to call non-stream API

                    debug!(
                        "meta-service version({} < 0.2.163) is too old, fallback to call non-stream API",
                        server_version
                    );

                    let grpc_req = MetaGrpcReq::ListKV(list_req.clone());
                    let raft_req: RaftRequest = grpc_req.into();

                    let req = traced_req(raft_req.clone());

                    let result = client
                        .kv_api(req)
                        .timed_ge(threshold(), info_spent("client::kv_read_v1"))
                        .await;

                    debug!(
                        result = as_debug!(&result);
                        "MetaGrpcClient::kv_read_v1 result, {}-th try", i
                    );

                    if let Err(ref e) = result {
                        if status_is_retryable(e) {
                            self.mark_current_endpoint_unhealthy();
                            failures.push(e.clone());
                            continue;
                        }
                    }

                    let raft_reply = result?.into_inner();
                    let list_reply: ListKVReply = reply_to_api_result(raft_reply)?;
                    let strm = futures::stream::iter(
                        list_reply.into_iter().map(|x| Ok(pb::StreamItem::from(x))),
                    );

                    return Ok(strm.boxed());
                }
            }

            let raft_req: RaftRequest = grpc_req.clone().into();
            let req = traced_req(raft_req.clone());

            let result = client
                .kv_read_v1(req)
                .timed_ge(threshold(), info_spent("client::kv_read_v1"))
                .await;

            debug!(
                result = as_debug!(&result);
                "MetaGrpcClient::kv_read_v1 result, {}-th try", i
            );

            if let Err(ref e) = result {
                if status_is_retryable(e) {
                    self.mark_current_endpoint_unhealthy();
                    failures.push(e.clone());
                    continue;
                }
            }

            let strm = result?.into_inner();

            return Ok(strm.boxed());
        }

        let net_err = MetaNetworkError::ConnectionError(ConnectionError::new(
            AnyError::error(format_args!(
                "failed after {} retries: {:?}",
                RPC_RETRIES, failures
            )),
            "failed to connect to meta-service",
        ));

        Err(net_err.into())
    }

    #[minitrace::trace]
    pub(crate) async fn transaction(&self, req: TxnRequest) -> Result<TxnReply, MetaError> {
        let txn: TxnRequest = req;

        debug!(
            req = as_display!(&txn);
            "MetaGrpcClient::transaction request"
        );

        let req: Request<TxnRequest> = Request::new(txn.clone());
        let req = common_tracing::inject_span_to_tonic_request(req);

        let (mut client, _sver) = self.make_client().await?;
        let result = client.transaction(req).await;

        let result: Result<TxnReply, Status> = match result {
            Ok(r) => return Ok(r.into_inner()),
            Err(s) => {
                if status_is_retryable(&s) {
                    self.mark_current_endpoint_unhealthy();
                    let (mut client, _sver) = self.make_client().await?;
                    let req: Request<TxnRequest> = Request::new(txn);
                    let req = common_tracing::inject_span_to_tonic_request(req);
                    let ret = client.transaction(req).await?.into_inner();
                    return Ok(ret);
                } else {
                    Err(s)
                }
            }
        };

        let reply = result?;

        debug!(
            reply = as_display!(&reply);
            "MetaGrpcClient::transaction reply"
        );

        Ok(reply)
    }

    fn set_current_endpoint(&self, addr: impl ToString) {
        let mut ce = self.current_endpoint.lock();
        *ce = Some(addr.to_string());
    }

    fn get_current_endpoint(&self) -> Option<String> {
        let ce = self.current_endpoint.lock();
        ce.as_ref().cloned()
    }

    fn mark_current_endpoint_unhealthy(&self) {
        let endpoint = self.get_current_endpoint().unwrap();

        let mut ue = self.unhealthy_endpoints.lock();
        ue.insert(endpoint, ());
    }
}

/// Inject span into a tonic request, so that on the remote peer the tracing context can be restored.
fn traced_req<T>(t: T) -> Request<T> {
    let req = Request::new(t);
    common_tracing::inject_span_to_tonic_request(req)
}

fn status_is_retryable(status: &Status) -> bool {
    matches!(
        status.code(),
        Code::Unauthenticated | Code::Unavailable | Code::Internal
    )
}

/// Fill in auth token into request metadata.
///
/// The token is stored in a `OnceCell`, which is fill in when handshake is done.
#[derive(Clone)]
pub struct AuthInterceptor {
    pub token: Arc<OnceCell<Vec<u8>>>,
}

impl Interceptor for AuthInterceptor {
    fn call(&mut self, mut req: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
        let metadata = req.metadata_mut();

        // The handshake does not need token.
        // When the handshake is done, token is filled.
        let Some(token) = self.token.get() else {
            return Ok(req);
        };

        let meta_value = MetadataValue::from_bytes(token.as_ref());
        metadata.insert_bin(AUTH_TOKEN_KEY, meta_value);
        Ok(req)
    }
}

fn threshold() -> Duration {
    Duration::from_millis(300)
}

fn info_spent(msg: impl Display) -> impl Fn(Duration, Duration) {
    move |total, busy| {
        info!(
            total = as_debug!(&total),
            busy = as_debug!(&busy);
            "{} spent", msg
        );
    }
}
