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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use log::info;
use serde::Deserialize;
use serde::Serialize;

use crate::servers::flight::v1::exchange::DataExchangeManager;

pub static FINISH_QUERY: &str = "/actions/finish_query";

/// Sent by the coordinator when a distributed query fails before it starts executing,
/// so that nodes which already accepted the query env or fragments release them at once
/// instead of waiting for the leaked-query sweeper.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FinishQueryPacket {
    pub query_id: String,
    pub cause: String,
}

pub async fn finish_query(packet: FinishQueryPacket) -> Result<()> {
    info!(
        "Finishing query {} on coordinator request, cause: {}",
        packet.query_id, packet.cause
    );
    DataExchangeManager::instance().on_finished_query(
        &packet.query_id,
        Some(ErrorCode::AbortedQuery(packet.cause)),
    );
    Ok(())
}
