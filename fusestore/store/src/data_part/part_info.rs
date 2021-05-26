// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
//

use common_planners::Partition;
use common_planners::Statistics;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct DataPartInfo {
    pub partition: Partition,
    pub stats: Statistics
}
