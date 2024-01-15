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

use std::fmt::Display;

use chrono::Utc;
use common_meta_app::app_error::AppError;
use common_meta_app::app_error::BackgroundJobAlreadyExists;
use common_meta_app::app_error::UnknownBackgroundJob;
use common_meta_app::background::BackgroundJobId;
use common_meta_app::background::BackgroundJobIdent;
use common_meta_app::background::BackgroundJobInfo;
use common_meta_app::background::BackgroundTaskIdent;
use common_meta_app::background::BackgroundTaskInfo;
use common_meta_app::background::CreateBackgroundJobReply;
use common_meta_app::background::CreateBackgroundJobReq;
use common_meta_app::background::DeleteBackgroundJobReply;
use common_meta_app::background::DeleteBackgroundJobReq;
use common_meta_app::background::GetBackgroundJobReply;
use common_meta_app::background::GetBackgroundJobReq;
use common_meta_app::background::GetBackgroundTaskReply;
use common_meta_app::background::GetBackgroundTaskReq;
use common_meta_app::background::ListBackgroundJobsReq;
use common_meta_app::background::ListBackgroundTasksReq;
use common_meta_app::background::UpdateBackgroundJobParamsReq;
use common_meta_app::background::UpdateBackgroundJobReply;
use common_meta_app::background::UpdateBackgroundJobStatusReq;
use common_meta_app::background::UpdateBackgroundTaskReply;
use common_meta_app::background::UpdateBackgroundTaskReq;
use common_meta_kvapi::kvapi;
use common_meta_kvapi::kvapi::Key;
use common_meta_kvapi::kvapi::UpsertKVReq;
use common_meta_types::ConditionResult::Eq;
use common_meta_types::InvalidReply;
use common_meta_types::KVMeta;
use common_meta_types::MatchSeq;
use common_meta_types::MatchSeq::Any;
use common_meta_types::MetaError;
use common_meta_types::Operation;
use common_meta_types::TxnRequest;
use log::as_debug;
use log::debug;
use minitrace::func_name;

use crate::background_api::BackgroundApi;
use crate::deserialize_struct;
use crate::fetch_id;
use crate::get_pb_value;
use crate::get_u64_value;
use crate::id_generator::IdGenerator;
use crate::kv_app_error::KVAppError;
use crate::send_txn;
use crate::serialize_struct;
use crate::serialize_u64;
use crate::txn_cond_seq;
use crate::txn_op_put;
use crate::util::deserialize_u64;
use crate::util::txn_trials;

/// BackgroundApi is implemented upon kvapi::KVApi.
/// Thus every type that impl kvapi::KVApi impls BackgroundApi.
#[tonic::async_trait]
impl<KV: kvapi::KVApi<Error = MetaError>> BackgroundApi for KV {
    #[minitrace::trace]
    async fn create_background_job(
        &self,
        req: CreateBackgroundJobReq,
    ) -> Result<CreateBackgroundJobReply, KVAppError> {
        debug!(req = as_debug!(&req); "BackgroundApi: {}", func_name!());

        let name_key = &req.job_name;

        let ctx = &func_name!();
        let mut trials = txn_trials(None, ctx);
        let id = loop {
            trials.next().unwrap()?;
            // Get db mask by name to ensure absence
            let (seq, id) = get_u64_value(self, name_key).await?;
            debug!(seq = seq, id = id, name_key = as_debug!(name_key); "create_background_job");

            if seq > 0 {
                return if req.if_not_exists {
                    Ok(CreateBackgroundJobReply { id })
                } else {
                    Err(KVAppError::AppError(AppError::BackgroundJobAlreadyExists(
                        BackgroundJobAlreadyExists::new(
                            &name_key.name,
                            format!("create background job: {:?}", req.job_name),
                        ),
                    )))
                };
            }

            let id = fetch_id(self, IdGenerator::background_job_id()).await?;
            let id_key = BackgroundJobId { id };

            debug!(
                id = as_debug!(&id_key),
                name_key = as_debug!(name_key);
                "new backgroundjob id"
            );

            {
                let meta: BackgroundJobInfo = req.job_info.clone();
                let condition = vec![txn_cond_seq(name_key, Eq, 0)];
                let if_then = vec![
                    txn_op_put(name_key, serialize_u64(id)?), // name -> background_job_id
                    txn_op_put(&id_key, serialize_struct(&meta)?), // id -> meta
                ];

                let txn_req = TxnRequest {
                    condition,
                    if_then,
                    else_then: vec![],
                };

                let (succ, _responses) = send_txn(self, txn_req).await?;

                debug!(
                    name = as_debug!(name_key),
                    id = as_debug!(&id_key),
                    succ = succ;
                    "create_background_job"
                );

                if succ {
                    break id;
                }
            }
        };

        Ok(CreateBackgroundJobReply { id })
    }

    // TODO(zhihanz): needs to drop both background job and related background tasks, also needs to gracefully shutdown running queries
    #[minitrace::trace]
    async fn drop_background_job(
        &self,
        _req: DeleteBackgroundJobReq,
    ) -> Result<DeleteBackgroundJobReply, KVAppError> {
        todo!()
    }

    #[minitrace::trace]
    async fn update_background_job_status(
        &self,
        req: UpdateBackgroundJobStatusReq,
    ) -> Result<UpdateBackgroundJobReply, KVAppError> {
        let name = &req.job_name;

        update_background_job(self, name, |info| {
            if info.job_status.as_ref() == Some(&req.status) {
                return false;
            }
            info.job_status = Some(req.status);
            info.last_updated = Some(Utc::now());
            true
        })
        .await
    }

    #[minitrace::trace]
    async fn update_background_job_params(
        &self,
        req: UpdateBackgroundJobParamsReq,
    ) -> Result<UpdateBackgroundJobReply, KVAppError> {
        let name = &req.job_name;
        update_background_job(self, name, |info| {
            if info.job_params.as_ref() == Some(&req.params) {
                return false;
            }
            info.job_params = Some(req.params);
            info.last_updated = Some(Utc::now());
            true
        })
        .await
    }

    #[minitrace::trace]
    async fn get_background_job(
        &self,
        req: GetBackgroundJobReq,
    ) -> Result<GetBackgroundJobReply, KVAppError> {
        debug!(req = as_debug!(&req); "BackgroundApi: {}", func_name!());

        let name_key = &req.name;

        let (id, _, job) =
            get_background_job_or_error(self, name_key, format!("get_: {:?}", name_key)).await?;

        Ok(GetBackgroundJobReply { id, info: job })
    }

    #[minitrace::trace]
    async fn list_background_jobs(
        &self,
        req: ListBackgroundJobsReq,
    ) -> Result<Vec<(u64, String, BackgroundJobInfo)>, KVAppError> {
        let prefix = format!("{}/{}", BackgroundJobIdent::PREFIX, req.tenant);
        let reply = self.prefix_list_kv(&prefix).await?;
        let mut res = vec![];
        for (k, v) in reply {
            let ident = BackgroundJobIdent::from_str_key(k.as_str()).map_err(|e| {
                KVAppError::MetaError(MetaError::from(InvalidReply::new(
                    "list_background_jobs",
                    &e,
                )))
            })?;
            let job_id = deserialize_u64(&v.data)?;
            let r = get_background_job_by_id(self, &BackgroundJobId { id: job_id.0 }).await?;
            // filter none and get the task info
            if let Some(task_info) = r.1 {
                res.push((r.0, ident.name, task_info));
            }
        }
        Ok(res)
    }

    #[minitrace::trace]
    async fn update_background_task(
        &self,
        req: UpdateBackgroundTaskReq,
    ) -> Result<UpdateBackgroundTaskReply, KVAppError> {
        debug!(req = as_debug!(&req); "BackgroundApi: {}", func_name!());
        let name_key = &req.task_name;
        debug!(name_key = as_debug!(name_key); "update_background_task");

        let meta = req.task_info.clone();

        let resp = self
            .upsert_kv(UpsertKVReq::new(
                name_key.to_string_key().as_str(),
                Any,
                Operation::Update(serialize_struct(&meta)?),
                Some(KVMeta {
                    expire_at: Some(req.expire_at),
                }),
            ))
            .await?;
        // confirm a successful update
        assert!(resp.is_changed());
        Ok(UpdateBackgroundTaskReply {
            last_updated: Utc::now(),
            expire_at: req.expire_at,
        })
    }

    #[minitrace::trace]
    async fn list_background_tasks(
        &self,
        req: ListBackgroundTasksReq,
    ) -> Result<Vec<(u64, String, BackgroundTaskInfo)>, KVAppError> {
        let prefix = format!("{}/{}", BackgroundTaskIdent::PREFIX, req.tenant);
        let reply = self.prefix_list_kv(&prefix).await?;
        let mut res = vec![];
        for (k, v) in reply {
            let ident = BackgroundTaskIdent::from_str_key(k.as_str()).map_err(|e| {
                KVAppError::MetaError(MetaError::from(InvalidReply::new(
                    "list_background_tasks",
                    &e,
                )))
            })?;
            let val: BackgroundTaskInfo = deserialize_struct(&v.data)?;
            res.push((v.seq, ident.task_id, val));
        }
        Ok(res)
    }

    #[minitrace::trace]
    async fn get_background_task(
        &self,
        req: GetBackgroundTaskReq,
    ) -> Result<GetBackgroundTaskReply, KVAppError> {
        debug!(
            req = as_debug!(&req);
            "BackgroundTaskApi: {}",
            func_name!()
        );
        let name = &req.name;
        let (_, resp) = get_background_task_by_name(self, name).await?;
        Ok(GetBackgroundTaskReply { task_info: resp })
    }
}

async fn get_background_job_id(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    name_key: &BackgroundJobIdent,
) -> Result<BackgroundJobId, KVAppError> {
    let (id_seq, id) = get_u64_value(kv_api, name_key).await?;
    background_job_has_to_exist(id_seq, name_key)?;
    Ok(BackgroundJobId { id })
}

async fn get_background_job_or_error(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    name_key: &BackgroundJobIdent,
    _msg: impl Display,
) -> Result<(u64, u64, BackgroundJobInfo), KVAppError> {
    let id_key = get_background_job_id(kv_api, name_key).await?;
    let (id_seq, job_info) = get_pb_value(kv_api, &id_key).await?;
    background_job_has_to_exist(id_seq, name_key)?;

    Ok((
        id_key.id,
        id_seq,
        // Safe unwrap(): background_job_seq > 0 implies background_job is not None.
        job_info.unwrap(),
    ))
}

/// Return OK if a db_id or db_meta exists by checking the seq.
///
/// Otherwise returns UnknownBackgroundJob error
pub fn background_job_has_to_exist(
    seq: u64,
    name_ident: &BackgroundJobIdent,
) -> Result<(), KVAppError> {
    if seq == 0 {
        debug!(seq = seq, name_ident = as_debug!(name_ident); "background job does not exist");
        Err(KVAppError::AppError(AppError::UnknownBackgroundJob(
            UnknownBackgroundJob::new(&name_ident.name, format!("{:?}", name_ident)),
        )))
    } else {
        Ok(())
    }
}

async fn get_background_job_by_id(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    id: &BackgroundJobId,
) -> Result<(u64, Option<BackgroundJobInfo>), KVAppError> {
    let (seq, res) = get_pb_value(kv_api, id).await?;
    Ok((seq, res))
}

async fn get_background_task_by_name(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    id: &BackgroundTaskIdent,
) -> Result<(u64, Option<BackgroundTaskInfo>), KVAppError> {
    let (seq, res) = get_pb_value(kv_api, id).await?;
    Ok((seq, res))
}

async fn update_background_job<F: FnOnce(&mut BackgroundJobInfo) -> bool>(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    name: &BackgroundJobIdent,
    mutation: F,
) -> Result<UpdateBackgroundJobReply, KVAppError> {
    debug!(req = as_debug!(name); "BackgroundApi: {}", func_name!());
    let (id, id_val_seq, mut info) =
        get_background_job_or_error(kv_api, name, "update_background_job").await?;
    let should_update = mutation(&mut info);
    if !should_update {
        return Ok(UpdateBackgroundJobReply { id });
    }
    let resp = kv_api
        .upsert_kv(UpsertKVReq::new(
            BackgroundJobId { id }.to_string_key().as_str(),
            MatchSeq::Exact(id_val_seq),
            Operation::Update(serialize_struct(&info)?),
            None,
        ))
        .await?;
    assert!(resp.is_changed());
    Ok(UpdateBackgroundJobReply { id })
}
