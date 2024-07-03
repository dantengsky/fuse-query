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

use std::io::Read;

use databend_common_exception::Result;
use databend_common_expression::TableSchema;

use crate::meta::load_json;
use crate::meta::SnapshotVersion;
use crate::meta::TableSnapshot;
use crate::meta::TableSnapshotV2;
use crate::meta::TableSnapshotV3;
use crate::meta::TableSnapshotV4;
use crate::readers::VersionedReader;

impl VersionedReader<TableSnapshot> for SnapshotVersion {
    type TargetType = TableSnapshot;
    fn read<R>(&self, reader: R) -> Result<TableSnapshot>
    where R: Read + Unpin + Send {
        let r = match self {
            SnapshotVersion::V5(_) => TableSnapshot::from_read(reader)?,
            SnapshotVersion::V4(_) => TableSnapshotV4::from_read(reader)?.into(),
            SnapshotVersion::V3(_) => TableSnapshotV3::from_reader(reader)?.into(),
            SnapshotVersion::V2(v) => {
                let mut ts: TableSnapshotV2 = load_json(reader, v)?;
                ts.schema = TableSchema::init_if_need(ts.schema);
                ts.into()
            }
            SnapshotVersion::V1(v) => {
                let ts = load_json(reader, v)?;
                TableSnapshotV2::from(ts).into()
            }
            SnapshotVersion::V0(v) => {
                let ts = load_json(reader, v)?;
                TableSnapshotV2::from(ts).into()
            }
        };
        Ok(r)
    }
}
