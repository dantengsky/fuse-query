// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::convert::TryInto;
use std::io::Cursor;

use common_arrow::arrow_flight::Action;
use prost::Message;
use tonic::Request;

use crate::impls::kv_api_impl::DeleteKVReq;
use crate::impls::kv_api_impl::GetKVAction;
use crate::impls::kv_api_impl::MGetKVAction;
use crate::impls::kv_api_impl::PrefixListReq;
use crate::impls::storage_api_impl::ReadPlanAction;
use crate::protobuf::FlightStoreRequest;
use crate::CreateDatabaseAction;
use crate::CreateTableAction;
use crate::DropDatabaseAction;
use crate::DropTableAction;
use crate::GetDatabaseAction;
use crate::GetTableAction;
use crate::UpdateKVReq;
use crate::UpsertKVAction;

pub trait RequestFor {
    type Reply;
}

// TODO move this to somewhere else (impls/mod.rs?)
#[macro_export]
macro_rules! action_declare {
    ($req:ident, $reply:ident, $enum_ctor:expr) => {
        impl RequestFor for $req {
            type Reply = $reply;
        }

        impl From<$req> for StoreDoAction {
            fn from(act: $req) -> Self {
                $enum_ctor(act)
            }
        }
    };
}

// === partition: read_plan===

// Action wrapper for do_action.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub enum StoreDoAction {
    // meta-database
    CreateDatabase(CreateDatabaseAction),
    GetDatabase(GetDatabaseAction),
    DropDatabase(DropDatabaseAction),
    // meta-table
    CreateTable(CreateTableAction),
    DropTable(DropTableAction),
    // storage
    ReadPlan(ReadPlanAction),
    GetTable(GetTableAction),
    // general purpose kv
    UpsertKV(UpsertKVAction),
    GetKV(GetKVAction),
    MGetKV(MGetKVAction),
    PrefixListKV(PrefixListReq),
    DeleteKV(DeleteKVReq),
    UpdateKV(UpdateKVReq),
}

/// Try convert tonic::Request<Action> to DoActionAction.
impl TryInto<StoreDoAction> for Request<Action> {
    type Error = tonic::Status;

    fn try_into(self) -> Result<StoreDoAction, Self::Error> {
        let action = self.into_inner();
        let mut buf = Cursor::new(&action.body);

        // Decode FlightRequest from buffer.
        let request: FlightStoreRequest = FlightStoreRequest::decode(&mut buf)
            .map_err(|e| tonic::Status::internal(e.to_string()))?;

        // Decode DoActionAction from flight request body.
        let json_str = request.body.as_str();
        let action = serde_json::from_str::<StoreDoAction>(json_str)
            .map_err(|e| tonic::Status::internal(e.to_string()))?;
        Ok(action)
    }
}

/// Try convert DoActionAction to tonic::Request<Action>.
impl TryInto<Request<Action>> for &StoreDoAction {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<Request<Action>, Self::Error> {
        let flight_request = FlightStoreRequest {
            body: serde_json::to_string(&self)?,
        };
        let mut buf = vec![];
        flight_request.encode(&mut buf)?;
        let request = tonic::Request::new(Action {
            r#type: "".to_string(),
            body: buf,
        });
        Ok(request)
    }
}
