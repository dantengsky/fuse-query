// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use futures::StreamExt;

use common_exception::ErrorCodes;
use common_exception::Result;
use common_flights::ReadAction;
use common_planners::PlanNode;
use common_planners::ReadDataSourcePlan;
use common_streams::SendableDataBlockStream;

use crate::datasources::remote::remote_table::RemoteTable;
use crate::sessions::FuseQueryContextRef;

impl RemoteTable {
    #[inline]
    pub(super) async fn do_read(
        &self,
        ctx: FuseQueryContextRef,
    ) -> Result<SendableDataBlockStream> {
        let client = self.store_client_provider.try_get_client().await?;
        let schema = self.schema.clone();
        let db = self.db.to_string();
        let tbl = self.name.to_string();

        let iter = std::iter::from_fn(move || match ctx.try_get_partitions(1) {
            Err(_) => None,
            Ok(parts) if parts.is_empty() => None,
            Ok(parts) => Some(ReadAction {
                partition: parts,
                push_down: PlanNode::ReadSource(ReadDataSourcePlan {
                    db: db.clone(),
                    table: tbl.clone(),
                    schema: schema.clone(),
                    ..ReadDataSourcePlan::empty()
                }),
            }),
        });
        let parts = futures::stream::iter(iter);
        let streams = parts.then(move |parts| {
            let mut client = client.clone();
            async move {
                let r = client.get_partition(&parts).await;
                r.unwrap_or_else(|e| {
                    Box::pin(futures::stream::once(async move {
                        Err(ErrorCodes::CannotReadFile(format!(
                            "get partition failure. partition [{:?}], error {:?}",
                            &parts, e
                        )))
                    }))
                })
            }
        });

        let flatten = streams.flatten();
        Ok(Box::pin(flatten))
    }
}
