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

use std::collections::HashMap;
use std::collections::HashSet;

use databend_storages_common_table_meta::meta::Location;

/// Represents the difference between two sets of segments and provides functionality to apply these differences.
///
/// `SegmentsDiff` is primarily used in the transaction retry mechanism to resolve conflicts. When a transaction
/// fails due to conflicts with concurrent changes, this struct helps calculate the difference between:
/// - The base segments (from the beginning of the transaction)
/// - The new segments (the changes the transaction wants to make)
///
/// This diff can then be applied to the latest table state to create a merged result that incorporates
/// both the transaction's changes and any concurrent changes made to the table.
///
/// If the diff cannot be applied (e.g., because segments that need to be replaced no longer exist),
/// it indicates an unresolvable conflict that requires the transaction to be aborted.
pub struct SegmentsDiff {
    appended: Vec<Location>,
    replaced: HashMap<Location, Vec<Location>>,
}

impl SegmentsDiff {
    /// Creates a new `SegmentsDiff` by comparing base segments with new segments.
    ///
    /// This method calculates the differences between two sets of segments:
    /// - Identifies which segments are replaced
    /// - Determines which new segments are newly appended
    ///
    /// ## Logic Summary
    /// 1. If base segments are empty, all new segments are considered appended
    /// 2. Otherwise, finds common segments by matching segment locations
    /// 3. Handles segments before the first common segment as appended
    /// 4. For segments between common elements, determines replacements
    /// 5. Handles any remaining base segments after the last common element by replacing them with any remaining new segments
    /// 6. Returns a diff structure with replaced and appended segments
    ///
    /// ## Example
    /// ```text
    /// Base segments: [A] [B] [C] [D] [E] [F]
    ///                     |       |
    ///                     |       |
    ///                     v       v
    /// New segments:  [X] [B] [Y] [D] [Z]
    ///
    /// Result:
    /// - Common segments: B and D (matched by ID)
    /// - Appended: [X] (before first common segment)
    /// - Replaced:
    ///   - A -> [] (removed)
    ///   - C -> [Y] (replaced with Y)
    ///   - E, F -> [Z] (replaced with Z)
    /// ```
    ///
    /// For more examples, please refer to the unit test cases.
    ///
    /// ## Validations
    ///   - No duplicate segments exist in either input array (will panic if found)
    ///   - Common segments maintain the same relative order in both arrays
    ///   - Invariants of diff:
    ///     - Appended segments are not present in the base segments
    ///     - Replacement segments are a subset of new segments
    ///     - Keys in replaced should be a subset of base segments
    ///
    /// # Arguments
    /// * `base_segments` - The original set of segments, typically it is from the table snapshot at the beginning of a transaction
    /// * `new_segments` - The target set of segments, typically it is from the latest table snapshot found during transaction retry
    ///
    /// # Returns
    /// A `SegmentsDiff` containing the necessary changes to transform base segments to new segments
    ///
    /// # Panics
    /// This function will panic if:
    /// - Duplicate segments are found in either input array
    /// - The invariants about appended and replaced segments are violated
    /// - Common segments don't maintain the same relative order in both arrays
    #[allow(clippy::needless_range_loop)]
    pub fn new(base_segments: &[Location], new_segments: &[Location]) -> Self {
        // Defensive check 1: Ensure no duplicate segments in input arrays
        let new_segments_set: HashSet<_> = new_segments.iter().collect();
        let base_segments_set: HashSet<_> = base_segments.iter().collect();
        {
            // Check that no duplicate segments in either vector
            assert_eq!(new_segments_set.len(), new_segments.len());
            assert_eq!(base_segments_set.len(), base_segments.len());
        }

        // base_segments is empty
        if base_segments.is_empty() {
            return SegmentsDiff {
                appended: new_segments.to_vec(),
                replaced: HashMap::new(),
            };
        }

        let mut common_indices = Vec::new();
        let new_segment_map: HashMap<_, _> = new_segments
            .iter()
            .enumerate()
            .map(|(idx, loc)| (&loc.0, idx))
            .collect();

        for (i, base) in base_segments.iter().enumerate() {
            if let Some(&j) = new_segment_map.get(&base.0) {
                common_indices.push((i, j));
            }
        }

        // Defensive check 2: Verify common segments maintain the same relative order in both arrays
        {
            if common_indices.len() >= 2 {
                Self::validate_common_segment_order(&common_indices);
            }
        }

        let mut replaced = HashMap::new();
        let mut appended = Vec::new();
        let mut prev_base_idx = 0;
        let mut prev_new_idx = 0;

        // first common element is the first element of base_segments
        if let Some(&(base_idx, new_idx)) = common_indices.first() {
            if base_idx == 0 {
                appended.extend(new_segments[..new_idx].iter().cloned());
            }
        }

        // process the elements between common elements
        for &(base_idx, new_idx) in &common_indices {
            for i in prev_base_idx..base_idx {
                let mut replacements = Vec::new();

                if i == prev_base_idx && prev_new_idx < new_idx {
                    for j in prev_new_idx..new_idx {
                        replacements.push(new_segments[j].clone());
                    }
                }

                replaced.insert(base_segments[i].clone(), replacements);
            }

            prev_base_idx = base_idx + 1;
            prev_new_idx = new_idx + 1;
        }

        // Process the remaining elements after the last common element
        for i in prev_base_idx..base_segments.len() {
            let mut replacements = Vec::new();

            if i == prev_base_idx && prev_new_idx < new_segments.len() {
                for j in prev_new_idx..new_segments.len() {
                    replacements.push(new_segments[j].clone());
                }
            }

            replaced.insert(base_segments[i].clone(), replacements);
        }

        // Defensive check 3: Verify invariants of the resulting diff
        let diff = SegmentsDiff { replaced, appended };
        diff.validate_diff_invariants(&base_segments_set, &new_segments_set);

        diff
    }

    /// Applies the diff to a target set of segments, transforming it according to the calculated differences.
    ///
    /// This method takes a target set of segments and applies both the replacements and additions
    /// defined in this diff. It performs the following operations:
    ///
    /// 1. Verifies that all segments to be replaced exist in the target set
    /// 2. Adds all appended segments to the result
    /// 3. For each segment in the target:
    ///    - If it should be replaced, adds the replacement segments instead
    ///    - Otherwise, keeps the original segment
    ///
    /// ## Example
    /// ```text
    /// Target segments: [A] [B] [C] [D]
    /// Diff:
    ///   - Appended: [X]
    ///   - Replaced: {B -> [Y, Z], D -> []}
    ///
    /// Result: [X] [A] [Y] [Z] [C]
    /// ```
    ///
    /// # Arguments
    /// * `self` - Consumes the diff as it's applied
    /// * `target` - The set of segments to apply the diff to, typically it is from a snapshot being committed
    ///
    /// # Returns
    /// * `Some(Vec<Location>)` - The transformed set of segments if all segments to be replaced exist in the target
    /// * `None` - If any segment to be replaced doesn't exist in the target, indicating the diff cannot be applied
    pub fn apply(self, target: Vec<Location>) -> Option<Vec<Location>> {
        let target_segments = target.iter().collect::<HashSet<_>>();
        // Defensive check 1: Ensure no duplicate segments in target
        {
            assert_eq!(target_segments.len(), target.len());
        }
        for base in self.replaced.keys() {
            if !target_segments.contains(base) {
                return None;
            }
        }

        let Self {
            appended,
            mut replaced,
        } = self;

        // Defensive check 2:
        // If a segment is in both appended and target, it must also be in replaced.
        {
            let appended_set: HashSet<&Location> = appended.iter().collect();
            for segment in &target {
                assert!(
                    !appended_set.contains(segment),
                    "Segment {:?} should not appear in both appended and target collections",
                    segment
                );
            }
        }

        let mut new_segments = appended;
        for segment in target.into_iter() {
            match replaced.remove(&segment) {
                Some(replacements) => {
                    new_segments.extend(replacements);
                }
                None => {
                    new_segments.push(segment);
                }
            }
        }
        Some(new_segments)
    }

    /// Validates that the diff invariants are maintained:
    /// 1. Appended segments should not be present in base segments
    /// 2. Replacement segments should be a subset of new segments
    /// 3. Keys in replaced should be a subset of base segments
    ///
    /// # Panics
    /// This function will panic if any of the invariants are violated.
    fn validate_diff_invariants(
        &self,
        base_segments_set: &HashSet<&Location>,
        new_segments_set: &HashSet<&Location>,
    ) {
        // Invariant 1: Segments in `appended` should not be present in base_segments
        let appended_set: HashSet<&Location> = self.appended.iter().collect();
        let is_disjoint = appended_set.is_disjoint(base_segments_set);
        if !is_disjoint {
            // Find the overlapping elements for better error reporting
            let overlap: Vec<_> = appended_set.intersection(base_segments_set).collect();
            log::error!(
                "Invariant violation: appended segments found in base_segments: {:?}",
                overlap
            );
            assert!(
                is_disjoint,
                "Appended segments must not be present in base_segments"
            );
        }

        // Invariant 2: Replacement segments should be a subset of new_segments
        let replaced_set: HashSet<&Location> = self.replaced.values().flatten().collect();
        let is_subset = replaced_set.is_subset(new_segments_set);
        if !is_subset {
            // Find the elements that are in replaced_set but not in new_segments_set
            let diff: Vec<_> = replaced_set.difference(new_segments_set).collect();
            log::error!(
                "Invariant violation: replacement segments not found in new_segments: {:?}",
                diff
            );
            assert!(
                is_subset,
                "Replacement segments must be present in new_segments"
            );
        }

        // Invariant 3: Keys in replaced should be a subset of base_segments
        let replaced_keys: HashSet<&Location> = self.replaced.keys().collect();
        let keys_are_subset = replaced_keys.is_subset(base_segments_set);
        if !keys_are_subset {
            // Find the elements that are in replaced_keys but not in base_segments_set
            let diff: Vec<_> = replaced_keys.difference(base_segments_set).collect();
            log::error!(
                "Invariant violation: replaced keys not found in base_segments: {:?}",
                diff
            );
            assert!(
                keys_are_subset,
                "Keys in replaced must be present in base_segments"
            );
        }
    }
    /// Validates that common segments maintain the same relative order in both arrays.
    ///
    /// # Panics
    /// This function will panic if common segments do not maintain the same relative order.
    fn validate_common_segment_order(common_indices: &[(usize, usize)]) {
        // Sort by base index to ensure we check in order
        let mut sorted_indices = common_indices.to_owned();
        sorted_indices.sort_by_key(|&(base_idx, _)| base_idx);

        // Check if the new indices are also in ascending order
        let new_indices: Vec<_> = sorted_indices.iter().map(|&(_, new_idx)| new_idx).collect();
        let mut is_ordered = true;

        for i in 1..new_indices.len() {
            if new_indices[i] < new_indices[i - 1] {
                is_ordered = false;
                log::error!(
                        "Order violation: Segment at base_index {} (new_index {}) comes after base_index {} (new_index {}), but new_index order is reversed",
                        sorted_indices[i].0,
                        new_indices[i],
                        sorted_indices[i-1].0,
                        new_indices[i-1]
                    );
            }
        }

        assert!(
            is_ordered,
            "Common segments must maintain the same relative order in both arrays"
        );
    }
}

#[cfg(test)]
mod tests {
    use databend_storages_common_table_meta::meta::TableSnapshot;

    use super::*;

    fn snapshot_from_segments(segments: Vec<&str>) -> TableSnapshot {
        let mut snapshot = TableSnapshot::new_empty_snapshot(Default::default(), None);
        snapshot.segments = segments.iter().map(|s| (s.to_string(), 0)).collect();
        snapshot
    }

    #[test]
    fn test_segments_edition() {
        {
            let base_snapshot = snapshot_from_segments(vec!["a", "b", "c", "d", "e", "f", "g"]);
            let new_snapshot = snapshot_from_segments(vec!["x", "y", "b", "m", "n", "f", "p"]);
            let segments_edition =
                SegmentsDiff::new(&base_snapshot.segments, &new_snapshot.segments);
            let mut replaced = segments_edition
                .replaced
                .iter()
                .map(|(l, o)| {
                    (
                        l.0.as_str(),
                        o.iter().map(|l| l.0.as_str()).collect::<Vec<_>>(),
                    )
                })
                .collect::<Vec<_>>();
            replaced.sort_by_key(|(k, _)| k.to_string());
            let appended: Vec<&str> = segments_edition
                .appended
                .iter()
                .map(|l| l.0.as_str())
                .collect::<Vec<_>>();
            assert_eq!(replaced, vec![
                ("a", vec!["x", "y"]),
                ("c", vec!["m", "n"]),
                ("d", vec![]),
                ("e", vec![]),
                ("g", vec!["p"])
            ]);
            assert!(appended.is_empty());
        }

        {
            {
                let base_snapshot = snapshot_from_segments(vec!["a", "b", "c", "d", "e", "f", "g"]);
                let new_snapshot = snapshot_from_segments(vec![]);
                let segments_edition =
                    SegmentsDiff::new(&base_snapshot.segments, &new_snapshot.segments);
                let mut replaced = segments_edition
                    .replaced
                    .iter()
                    .map(|(l, o)| {
                        (
                            l.0.as_str(),
                            o.iter().map(|l| l.0.as_str()).collect::<Vec<_>>(),
                        )
                    })
                    .collect::<Vec<_>>();
                replaced.sort_by_key(|(k, _)| k.to_string());
                let appended = segments_edition
                    .appended
                    .iter()
                    .map(|l| l.0.as_str())
                    .collect::<Vec<_>>();
                assert_eq!(replaced, vec![
                    ("a", vec![]),
                    ("b", vec![]),
                    ("c", vec![]),
                    ("d", vec![]),
                    ("e", vec![]),
                    ("f", vec![]),
                    ("g", vec![]),
                ]);
                assert!(appended.is_empty());
            }
        }

        {
            {
                let base_snapshot = snapshot_from_segments(vec!["a", "b", "c", "d", "e", "f", "g"]);
                let new_snapshot = snapshot_from_segments(vec!["z"]);
                let segments_edition =
                    SegmentsDiff::new(&base_snapshot.segments, &new_snapshot.segments);
                let mut replaced = segments_edition
                    .replaced
                    .iter()
                    .map(|(l, o)| {
                        (
                            l.0.as_str(),
                            o.iter().map(|l| l.0.as_str()).collect::<Vec<_>>(),
                        )
                    })
                    .collect::<Vec<_>>();
                replaced.sort_by_key(|(k, _)| k.to_string());
                let appended = segments_edition
                    .appended
                    .iter()
                    .map(|l| l.0.as_str())
                    .collect::<Vec<_>>();
                assert_eq!(replaced, vec![
                    ("a", vec!["z"]),
                    ("b", vec![]),
                    ("c", vec![]),
                    ("d", vec![]),
                    ("e", vec![]),
                    ("f", vec![]),
                    ("g", vec![]),
                ]);
                assert!(appended.is_empty());
            }
        }

        {
            {
                let base_snapshot = snapshot_from_segments(vec!["a"]);
                let new_snapshot = snapshot_from_segments(vec!["x", "y", "z", "a"]);
                let segments_edition =
                    SegmentsDiff::new(&base_snapshot.segments, &new_snapshot.segments);
                let replaced = segments_edition
                    .replaced
                    .iter()
                    .map(|(l, o)| {
                        (
                            l.0.as_str(),
                            o.iter().map(|l| l.0.as_str()).collect::<Vec<_>>(),
                        )
                    })
                    .collect::<Vec<_>>();
                let appended = segments_edition
                    .appended
                    .iter()
                    .map(|l| l.0.as_str())
                    .collect::<Vec<_>>();
                assert_eq!(appended, vec!["x", "y", "z"]);
                assert!(replaced.is_empty());
            }
        }

        {
            {
                let base_snapshot = snapshot_from_segments(vec![]);
                let new_snapshot = snapshot_from_segments(vec!["x", "y", "z"]);
                let segments_edition =
                    SegmentsDiff::new(&base_snapshot.segments, &new_snapshot.segments);
                let replaced = segments_edition
                    .replaced
                    .iter()
                    .map(|(l, o)| {
                        (
                            l.0.as_str(),
                            o.iter().map(|l| l.0.as_str()).collect::<Vec<_>>(),
                        )
                    })
                    .collect::<Vec<_>>();
                let appended = segments_edition
                    .appended
                    .iter()
                    .map(|l| l.0.as_str())
                    .collect::<Vec<_>>();
                assert_eq!(appended, vec!["x", "y", "z"]);
                assert!(replaced.is_empty());
            }
        }
    }

    #[test]
    fn test_validate_segment_order_valid() {
        // Valid case - segments maintain the same order
        let common_indices = vec![(0, 1), (2, 3), (4, 6)];
        // This should not panic
        SegmentsDiff::validate_common_segment_order(&common_indices);
    }

    #[test]
    #[should_panic(expected = "Common segments must maintain the same relative order")]
    fn test_validate_segment_order_invalid() {
        // Invalid case - segments have different relative order
        let common_indices = vec![(0, 3), (2, 1), (4, 5)];
        // This should panic because the new indices (3, 1, 5) are not in ascending order
        SegmentsDiff::validate_common_segment_order(&common_indices);
    }
}
