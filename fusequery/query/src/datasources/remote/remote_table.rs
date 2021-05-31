// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;

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
use futures::TryFutureExt;

//use tokio_stream::StreamExt;
use crate::datasources::remote::store_client_provider::StoreClientProvider;
use crate::datasources::remote::test_fun;
use crate::datasources::ITable;
use crate::sessions::FuseQueryContextRef;

#[allow(dead_code)]
pub struct RemoteTable {
    pub(crate) db: String,
    name: String,
    schema: DataSchemaRef,
    store_client_provider: StoreClientProvider,
}

impl RemoteTable {
    #[allow(dead_code)]
    pub fn try_create(
        db: String,
        name: String,
        schema: DataSchemaRef,
        store_client_provider: StoreClientProvider,
        _options: TableOptions,
    ) -> Result<Box<dyn ITable>> {
        let table = Self {
            db,
            name,
            schema,
            store_client_provider,
        };
        Ok(Box::new(table))
    }
}

#[async_trait::async_trait]
impl ITable for RemoteTable {
    fn name(&self) -> &str {
        &self.name
    }

    fn engine(&self) -> &str {
        "remote"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Result<DataSchemaRef> {
        Ok(self.schema.clone())
    }

    fn is_local(&self) -> bool {
        false
    }

    fn read_plan(&self, ctx: FuseQueryContextRef, scan: &ScanPlan) -> Result<ReadDataSourcePlan> {
        ctx.block_on(async {
            let mut client = self.store_client_provider.try_get_client().await?;
            let res = client
                .scan_partition(self.db.clone(), self.name.clone(), scan)
                .await?;
            Ok(self.partitions_to_plan(res))
        })
    }

    async fn read(&self, ctx: FuseQueryContextRef) -> Result<SendableDataBlockStream> {
        use std::sync::Arc;
        let client = Arc::new(self.store_client_provider.try_get_client().await?);
        let num = 2;
        let iter = std::iter::from_fn(move || {
            let partitions = ctx.clone().try_get_partitions(num).unwrap();
            if partitions.is_empty() {
                None
            } else {
                Some(ReadAction {
                    partition: partitions,
                    push_down: PlanNode::Empty(EmptyPlan {
                        schema: DataSchemaRefExt::create(vec![])
                    })
                })
            }
        });
        let parts = futures::stream::iter(iter);
        let clone = client.clone();
        let streams = parts.then(move |parts| {
            //let mut client = test_fun().await.unwrap();
            let client = client.clone();
            async move { client.clone().get_partition(&parts).await.unwrap() }
        });

        let flatten = streams.flatten();
        Ok(Box::pin(flatten))
        //todo!()
    }

    async fn append_data(&self, _ctx: FuseQueryContextRef, plan: InsertIntoPlan) -> Result<()> {
        // goes like this
        let opt_stream = {
            let mut inner = plan.input_stream.lock().unwrap();
            (*inner).take()
        };

        {
            let block_stream =
                opt_stream.ok_or_else(|| ErrorCodes::EmptyData("input stream consumed"))?;
            let mut client = self.store_client_provider.try_get_client().await?;
            client
                .append_data(
                    plan.db_name.clone(),
                    plan.tbl_name.clone(),
                    (&plan).schema().clone(),
                    block_stream,
                )
                .await?;
        }

        Ok(())
    }
}

impl RemoteTable {
    fn partitions_to_plan(&self, res: ScanPartitionResult) -> ReadDataSourcePlan {
        let mut partitions = vec![];
        let mut statistics = Statistics {
            read_rows: 0,
            read_bytes: 0
        };

        if let Some(parts) = res {
            for part in parts {
                partitions.push(Partition {
                    name: part.partition.name,
                    version: 0
                });
                statistics.read_rows += part.stats.read_rows;
                statistics.read_bytes += part.stats.read_bytes;
            }
        }

        ReadDataSourcePlan {
            db: self.db.clone(),
            table: self.name.clone(),
            schema: self.schema.clone(),
            partitions,
            statistics,
            description: "".to_string()
        }
    }
}
