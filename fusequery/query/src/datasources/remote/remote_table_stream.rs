// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

//use std::mem;
//use std::mem::ManuallyDrop;
//use std::ptr::NonNull;
//use std::sync::Arc;
//use std::task::Context;
//use std::task::Poll;
//use std::usize;
//
//use common_arrow::arrow::array::ArrayData;
//use common_arrow::arrow::buffer::Buffer;
//use common_arrow::arrow::datatypes::DataType;
//use common_datablocks::DataBlock;
//use common_datavalues::DataSchemaRef;
//use common_datavalues::UInt64Array;
//use common_exception::Result;
//use common_flights::StoreClient;
//use common_streams::ProgressStream;
//use futures::stream::Stream;
//
//use crate::sessions::FuseQueryContextRef;
//use std::pin::Pin;
//use common_arrow::arrow_flight::FlightData;
//use futures::{StreamExt, FutureExt};
//
//#[derive(Debug, Clone)]
//struct BlockRange {
//    begin: u64,
//    end: u64
//}
//
//pub struct RemoteTableBlockStream {
//    ctx: FuseQueryContextRef,
//    schema: DataSchemaRef,
//    block_index: usize,
//    blocks: Vec<BlockRange>
//}
//
//impl RemoteTableBlockStream {
//    pub fn try_create(ctx: FuseQueryContextRef, schema: DataSchemaRef) -> Result<ProgressStream> {
//        let stream = Box::pin(RemoteTableBlockStream {
//            ctx: ctx.clone(),
//            schema,
//            block_index: 0,
//            blocks: vec![]
//        });
//        ProgressStream::try_create(stream, ctx.progress_callback()?)
//    }
//
//    fn try_get_one_block(&mut self) -> Result<Option<DataBlock>> {
//        let partitions = self.ctx.try_get_partitions(1)?;
//        if partitions.is_empty() {
//            return Ok(None);
//        }
//
//        let block_size = self.ctx.get_max_block_size()?;
//        let mut blocks = Vec::with_capacity(partitions.len());
//
//
//        for part in partitions {
//
//            let mut stream: Pin<Box<dyn Stream<Item = Result<FlightData, tonic::Status>> + Send + Sync + 'static>>;
//            let next = stream.next();
//            next.poll_unpin()
//
//        }
//        todo!()
//    }
//
//        impl Stream for RemoteTableBlockStream {
//            type Item = Result<DataBlock>;
//
//    fn poll_next(
//        mut self: std::pin::Pin<&mut Self>,
//        _: &mut Context<'_>
//    ) -> Poll<Opion<Self::Item>> {
//        let block = self.try_get_one_block()?;
//
//        Poll::Ready(block.map(Ok))
//    }
//}
//
