// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_datablocks::DataBlock;
use common_exception::Result;

// TODO is it safe to remove `Sync` ?
pub type SendableDataBlockStream =
    std::pin::Pin<Box<dyn futures::stream::Stream<Item = Result<DataBlock>> + Send>>;
