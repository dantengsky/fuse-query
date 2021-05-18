// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_datavalues::DataSchemaRef;
use common_streams::SendableDataBlockStream;

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct InsertIntoPlan {
    pub db_name: String,
    pub tbl_name: String,

    /// This is a simple(silly) data structure for POC, please feel free to change it.
    ///
    /// - The Arc<_> stuff here is for Clone
    /// - Semantic of Clone/Ser-De could be problematic
    ///
    ///    Maybe we should change this to something like AddressableStream, so that it could be
    ///    lightly cloned, or even be sent across process boundaries.
    /// - Lifetime
    ///
    ///    Unless the stream could be fully "materialized" in memory (which is hardly realistic),
    ///    lifetime would be tricky, since logically, input_stream should reference the VALUES,
    ///    or the output stream of a Sub-Query, etc.
    ///
    #[serde(skip, default = "InsertIntoPlan::empty_stream")]
    pub input_stream: Arc<SendableDataBlockStream>
}

impl InsertIntoPlan {
    pub fn new() -> Self {
        todo!()
    }
    pub fn empty_stream() -> Arc<SendableDataBlockStream> {
        todo!()
    }
}

impl InsertIntoPlan {
    pub fn schema(&self) -> DataSchemaRef {
        todo!()
    }
}
