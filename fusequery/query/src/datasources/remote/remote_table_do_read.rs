// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::sync::Arc;

use common_datavalues::DataSchemaRef;
use common_datavalues::DataSchemaRefExt;
use common_exception::ErrorCodes;
use common_exception::Result;
use common_flights::ReadAction;
use common_flights::ScanPartitionResult;
use common_planners::EmptyPlan;
use common_planners::InsertIntoPlan;
use common_planners::Partition;
use common_planners::PlanNode;
use common_planners::ReadDataSourcePlan;
use common_planners::ScanPlan;
use common_planners::Statistics;
use common_planners::TableOptions;
use common_streams::SendableDataBlockStream;
use futures::StreamExt;

use crate::datasources::remote::remote_table::RemoteTable;
use crate::datasources::remote::store_client_provider::StoreClientProvider;
use crate::datasources::remote::test_fun;
use crate::datasources::ITable;
use crate::sessions::FuseQueryContextRef;

impl RemoteTable {
    #[inline]
    pub(super) async fn do_read(
        &self,
        ctx: FuseQueryContextRef,
    ) -> Result<SendableDataBlockStream> {
        let client = self.store_client_provider.try_get_client().await?;
        let schema = self.schema.clone();

        //        let (tx, rx) = tokio::sync::mpsc::channel(10);
        // A stream of partitions
        let iter = std::iter::from_fn(move || {
            let partitions = ctx.try_get_partitions(1).unwrap();

            if partitions.is_empty() {
                None
            } else {
                Some(ReadAction {
                    partition: partitions,
                    push_down: PlanNode::Empty(EmptyPlan {
                        schema: schema.clone(),
                    }),
                })
            }
        });
        let parts = futures::stream::iter(iter);
        let streams = parts.then(move |parts| {
            let mut client = client.clone();
            async move {
                let r = client.get_partition(&parts).await;
                if let Err(ref e) = r {
                    log::info!("get partition error {:?}", e);
                }
                r.unwrap()
            }
        });

        let flatten = streams.flatten();
        Ok(Box::pin(flatten))
    }
}
