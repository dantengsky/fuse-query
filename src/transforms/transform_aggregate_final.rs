// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::sync::Arc;

use async_trait::async_trait;
use tokio::stream::StreamExt;

use crate::datablocks::DataBlock;
use crate::datastreams::{DataBlockStream, SendableDataBlockStream};
use crate::datavalues::{DataSchemaRef, DataValue};
use crate::error::FuseQueryResult;
use crate::functions::Function;
use crate::planners::ExpressionPlan;
use crate::processors::{EmptyProcessor, IProcessor};

pub struct AggregateFinalTransform {
    funcs: Vec<Function>,
    schema: DataSchemaRef,
    input: Arc<dyn IProcessor>,
}

impl AggregateFinalTransform {
    pub fn try_create(schema: DataSchemaRef, exprs: Vec<ExpressionPlan>) -> FuseQueryResult<Self> {
        let mut funcs = Vec::with_capacity(exprs.len());
        for expr in &exprs {
            funcs.push(expr.to_function()?);
        }

        Ok(AggregateFinalTransform {
            funcs,
            schema,
            input: Arc::new(EmptyProcessor::create()),
        })
    }
}

#[async_trait]
impl IProcessor for AggregateFinalTransform {
    fn name(&self) -> &str {
        "AggregateFinalTransform"
    }

    fn connect_to(&mut self, input: Arc<dyn IProcessor>) -> FuseQueryResult<()> {
        self.input = input;
        Ok(())
    }

    async fn execute(&self) -> FuseQueryResult<SendableDataBlockStream> {
        let mut funcs = self.funcs.clone();
        let mut stream = self.input.execute().await?;
        while let Some(block) = stream.next().await {
            let block = block?;

            for (i, func) in funcs.iter_mut().enumerate() {
                if let DataValue::String(Some(serialized)) =
                    DataValue::try_from_array(block.column(0), i)?
                {
                    let deserialized: DataValue = serde_json::from_str(&serialized)?;
                    if let DataValue::Struct(states) = deserialized {
                        func.merge_state(&states)?;
                    }
                }
            }
        }

        let mut arrays = Vec::with_capacity(funcs.len());
        for func in &funcs {
            arrays.push(func.merge_result()?.to_array(1)?);
        }
        let block = DataBlock::create(self.schema.clone(), arrays);
        Ok(Box::pin(DataBlockStream::create(
            self.schema.clone(),
            None,
            vec![block],
        )))
    }
}
