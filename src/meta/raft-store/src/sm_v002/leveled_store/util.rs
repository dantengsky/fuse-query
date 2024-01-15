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

use std::fmt;
use std::io;

use crate::sm_v002::leveled_store::map_api::MapKV;
use crate::sm_v002::leveled_store::map_api::MapKey;
use crate::sm_v002::marked::Marked;

/// Sort by key and internal_seq.
/// Return `true` if `a` should be placed before `b`, e.g., `a` is smaller.
pub(in crate::sm_v002) fn by_key_seq<K>(
    r1: &Result<MapKV<K>, io::Error>,
    r2: &Result<MapKV<K>, io::Error>,
) -> bool
where
    K: MapKey + Ord + fmt::Debug,
{
    // TODO test Result
    match (r1, r2) {
        (Ok((k1, v1)), Ok((k2, v2))) => {
            assert_ne!((k1, v1.internal_seq()), (k2, v2.internal_seq()));

            // Put entries with the same key together, smaller internal-seq first
            // Tombstone is always greater.
            (k1, v1.internal_seq()) <= (k2, v2.internal_seq())
        }
        // If there is an error, just yield them in order.
        // It's the caller's responsibility to handle the error.
        _ => true,
    }
}

/// Result type of a key-value pair and io Error used in a map.
type KVResult<K> = Result<MapKV<K>, io::Error>;

/// Return a Ok(combined) to merge two consecutive values,
/// otherwise return Err((x,y)) to not to merge.
#[allow(clippy::type_complexity)]
pub(in crate::sm_v002) fn choose_greater<K>(
    r1: KVResult<K>,
    r2: KVResult<K>,
) -> Result<KVResult<K>, (KVResult<K>, KVResult<K>)>
where
    K: MapKey + Ord,
{
    // TODO test Result
    match (r1, r2) {
        (Ok((k1, v1)), Ok((k2, v2))) if k1 == k2 => Ok(Ok((k1, Marked::max(v1, v2)))),
        // If there is an error,
        // or k1 != k2
        // just yield them without change.
        (r1, r2) => Err((r1, r2)),
    }
}
