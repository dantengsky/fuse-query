//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

use std::sync::mpsc::channel;

use common_dal::DataAccessor;
use common_exception::ErrorCode;
use common_exception::Result;
use futures::AsyncReadExt;

use crate::datasources::fuse_table::types::table_snapshot::TableSnapshot;
use crate::sessions::DatafuseQueryContextRef;

pub fn read_table_snapshot(
    da: impl DataAccessor + Send + 'static,
    ctx: &DatafuseQueryContextRef,
    loc: &str,
    len: u64,
) -> Result<TableSnapshot> {
    let (tx, rx) = channel();
    let location = loc.to_string();
    ctx.execute_task(async move {
        let input_stream = da.get_input_stream(&location, Some(len));
        match input_stream.await {
            Ok(mut input) => {
                let mut buffer = vec![];
                // TODO send this error
                input.read_to_end(&mut buffer).await?;
                let _ = tx.send(Ok(buffer));
            }
            Err(e) => {
                let _ = tx.send(Err(e));
            }
        }
        Ok::<(), ErrorCode>(())
    })?;

    let res = rx.recv().map_err(ErrorCode::from_std_error)?;
    let snapshot = serde_json::from_slice::<TableSnapshot>(&res?)?;
    Ok(snapshot)
}
