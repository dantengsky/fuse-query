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
use std::sync::Arc;
use std::time::Instant;

use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_catalog::plan::PrewhereInfo;
use databend_common_catalog::plan::Projection;
use databend_common_catalog::runtime_filter_info::RuntimeBloomFilter;
use databend_common_catalog::runtime_filter_info::RuntimeFilterStats;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_expression::Evaluator;
use databend_common_expression::Expr;
use databend_common_expression::FieldIndex;
use databend_common_expression::FilterExecutor;
use databend_common_expression::FunctionContext;
use databend_common_expression::SelectExprBuilder;
use databend_common_expression::filter_helper::FilterHelpers;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::MutableBitmap;
use databend_common_functions::BUILTIN_FUNCTIONS;

use crate::fuse_part::FuseBlockPartInfo;
use crate::io::BlockReader;
use crate::io::DataItem;
use crate::io::RowSelection;
use crate::pruning::ExprBloomFilter;

#[derive(Clone)]
pub struct BloomRuntimeFilterRef {
    pub column_index: FieldIndex,
    pub filter: RuntimeBloomFilter,
    pub stats: Arc<RuntimeFilterStats>,
    pub sample_rows: u64,
    pub adaptive: bool,
}

pub(super) const MIN_RUNTIME_FILTER_REDUCTION_PERCENT: u64 = 5;

pub(super) fn runtime_filter_sample_rows(build_rows: usize) -> u64 {
    const MIN_SAMPLE_ROWS: u64 = 1_000_000;
    const MAX_SAMPLE_ROWS: u64 = 8_000_000;

    (build_rows as u64)
        .saturating_mul(2)
        .clamp(MIN_SAMPLE_ROWS, MAX_SAMPLE_ROWS)
}

pub struct ReadState {
    pub pre_reader: Arc<BlockReader>,
    pub remain_reader: Arc<BlockReader>,
    pub filters: Option<Expr>,
    filter_executor: Option<FilterExecutor>,
    pub runtime_filters: Vec<BloomRuntimeFilterRef>,
    pub pre_column_ids: HashSet<ColumnId>,
    pub remain_column_ids: HashSet<ColumnId>,
    pub func_ctx: FunctionContext,
    pub output_schema: DataSchema,
    pub prewhere_selectivity_threshold: u64,
    pub use_single_prewhere_reader: bool,
}

impl ReadState {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        scan_id: usize,
        prewhere_info: Option<&PrewhereInfo>,
        block_reader: Arc<BlockReader>,
    ) -> Result<Self> {
        let prewhere_selectivity_threshold =
            ctx.get_settings().get_prewhere_selectivity_threshold()?;
        let use_single_prewhere_reader =
            prewhere_info.is_some() && prewhere_selectivity_threshold == 0;
        let original_schema = block_reader.original_schema.as_ref();

        let runtime_filter_entries = ctx.get_runtime_filters(scan_id);
        let runtime_filter_column_names: Vec<_> = runtime_filter_entries
            .iter()
            .filter_map(|entry| {
                let column_name = entry.bloom.as_ref().map(|bloom| &bloom.column_name)?;
                if original_schema.index_of(column_name).is_ok() {
                    Some(column_name)
                } else {
                    None
                }
            })
            .collect();

        let mut preread_projection =
            Projection::from_column_names(original_schema, &runtime_filter_column_names)?;
        if let Some(prewhere_info) = prewhere_info {
            Projection::merge(&mut preread_projection, &prewhere_info.prewhere_columns);
        }

        let remain_projection = if use_single_prewhere_reader {
            Projection::Columns(vec![])
        } else {
            block_reader.projection.difference(&preread_projection)
        };
        let prewhere_reader = if use_single_prewhere_reader {
            block_reader.clone()
        } else {
            block_reader.change_projection(preread_projection)?
        };
        let remain_reader = block_reader.change_projection(remain_projection)?;
        let pre_column_ids = prewhere_reader.schema().to_leaf_column_id_set();
        let remain_column_ids = remain_reader.schema().to_leaf_column_id_set();

        let prewhere_schema: DataSchema = (prewhere_reader.schema().as_ref()).into();

        let runtime_filters: Vec<BloomRuntimeFilterRef> = runtime_filter_entries
            .into_iter()
            .filter_map(|entry| {
                let bloom = entry.bloom?;
                let column_index = prewhere_schema.index_of(bloom.column_name.as_str()).ok()?;
                Some(BloomRuntimeFilterRef {
                    column_index,
                    filter: bloom.filter,
                    stats: entry.stats,
                    sample_rows: runtime_filter_sample_rows(entry.build_rows),
                    adaptive: entry.adaptive_bloom,
                })
            })
            .collect();

        let prewhere_filter = if let Some(prewhere_info) = prewhere_info {
            let filter = prewhere_info
                .filter
                .as_expr(&BUILTIN_FUNCTIONS)
                .project_column_ref(|name| Ok(prewhere_schema.column_with_name(name).unwrap().0))?;
            Some(filter)
        } else {
            None
        };

        let func_ctx = ctx.get_function_context()?;
        let max_block_size = (ctx.get_settings().get_max_block_size()? as usize).max(1);
        let filter_executor = prewhere_filter.as_ref().and_then(|filter| {
            if !func_ctx.enable_selector_executor {
                return None;
            }

            // Keep bitmap evaluation for predicates that are already cheap to vectorize. Direct
            // string-scalar equality has a specialized selector path that avoids that overhead.
            let (select_expr, _) = SelectExprBuilder::new(&BUILTIN_FUNCTIONS)
                .build(filter)
                .into();
            select_expr.contains_string_column_eq_constant().then(|| {
                FilterExecutor::new(
                    filter.clone(),
                    func_ctx.clone(),
                    max_block_size,
                    None,
                    &BUILTIN_FUNCTIONS,
                    false,
                )
            })
        });

        Ok(Self {
            pre_reader: prewhere_reader,
            remain_reader,
            filters: prewhere_filter,
            filter_executor,
            runtime_filters,
            pre_column_ids,
            remain_column_ids,
            func_ctx,
            output_schema: block_reader.data_schema(),
            prewhere_selectivity_threshold,
            use_single_prewhere_reader,
        })
    }

    pub fn filter(&mut self, block: &DataBlock, num_rows: usize) -> Result<Option<MutableBitmap>> {
        if let Some(ref f) = self.filters {
            if let Some(filter_executor) = self.filter_executor.as_mut() {
                return Ok(Some(filter_with_selector(
                    filter_executor,
                    block,
                    num_rows,
                )?));
            }

            let evaluator = Evaluator::new(block, &self.func_ctx, &BUILTIN_FUNCTIONS);
            let filter_result = evaluator.run(f)?.try_downcast::<BooleanType>().unwrap();
            Ok(Some(FilterHelpers::filter_to_bitmap(
                filter_result,
                num_rows,
            )))
        } else {
            Ok(None)
        }
    }

    pub fn runtime_filter(
        &mut self,
        block: &DataBlock,
        _num_rows: usize,
    ) -> Result<Option<MutableBitmap>> {
        let bloom_start = Instant::now();

        let mut bitmaps = vec![];
        for runtime_filter in &self.runtime_filters {
            if runtime_filter.adaptive && runtime_filter.stats.bloom_disabled() {
                continue;
            }

            let filter_start = Instant::now();
            let probe_column = block.get_by_offset(runtime_filter.column_index).to_column();
            let bitmap = ExprBloomFilter::new(&runtime_filter.filter).apply(probe_column)?;
            if runtime_filter.adaptive {
                runtime_filter.stats.record_adaptive_bloom(
                    filter_start.elapsed().as_nanos() as u64,
                    bitmap.null_count() as u64,
                    bitmap.len() as u64,
                );
                runtime_filter.stats.disable_bloom_if_unselective(
                    runtime_filter.sample_rows,
                    MIN_RUNTIME_FILTER_REDUCTION_PERCENT,
                );
            } else {
                runtime_filter.stats.record_bloom(
                    filter_start.elapsed().as_nanos() as u64,
                    bitmap.null_count() as u64,
                );
            }
            bitmaps.push(bitmap);
        }

        let result_bitmap = bitmaps.into_iter().reduce(|acc, b| {
            let rhs: Bitmap = b.into();
            acc & &rhs
        });

        let bloom_duration = bloom_start.elapsed();
        Profile::record_usize_profile(
            ProfileStatisticsName::RuntimeFilterBloomTime,
            bloom_duration.as_nanos() as usize,
        );

        Ok(result_bitmap)
    }

    pub fn deserialize_and_filter(
        &mut self,
        columns_chunks: HashMap<ColumnId, DataItem>,
        part: &FuseBlockPartInfo,
    ) -> Result<(DataBlock, Option<RowSelection>, Option<Bitmap>)> {
        let pre_columns_chunks = Self::filter_column_chunks(&columns_chunks, &self.pre_column_ids)?;
        let mut preread_block = self
            .pre_reader
            .deserialize_part(part, pre_columns_chunks, None)?;

        let filter_bitmap = self.filter(&preread_block, part.nums_rows)?;
        let runtime_filter_bitmap = self.runtime_filter(&preread_block, part.nums_rows)?;

        let bitmap_selection: Option<Bitmap> = match (filter_bitmap, runtime_filter_bitmap) {
            (Some(filter_bitmap), Some(runtime_filter_bitmap)) => {
                let rhs: Bitmap = runtime_filter_bitmap.into();
                Some((filter_bitmap & &rhs).into())
            }
            (Some(filter_bitmap), None) => Some(filter_bitmap.into()),
            (None, Some(runtime_filter_bitmap)) => Some(runtime_filter_bitmap.into()),
            (None, None) => None,
        };

        let row_selection = bitmap_selection.as_ref().map(RowSelection::from);

        if let Some(ref bitmap) = bitmap_selection {
            preread_block = preread_block.filter_with_bitmap(bitmap)?;
        }

        if self.use_single_prewhere_reader {
            return Ok((preread_block, row_selection, bitmap_selection));
        }

        let remain_columns_chunks =
            Self::filter_column_chunks(&columns_chunks, &self.remain_column_ids)?;
        let push_down_row_selection = row_selection.as_ref().is_some_and(|row_selection| {
            should_push_down_row_selection(row_selection, self.prewhere_selectivity_threshold)
        });

        let mut remain_block = self.remain_reader.deserialize_part(
            part,
            remain_columns_chunks,
            push_down_row_selection
                .then_some(row_selection.as_ref())
                .flatten(),
        )?;
        if !push_down_row_selection {
            if let Some(bitmap) = bitmap_selection.as_ref() {
                remain_block = remain_block.filter_with_bitmap(bitmap)?;
            }
        }

        let mut merged_fields = self.pre_reader.data_fields();
        merged_fields.extend(self.remain_reader.data_fields());
        let merged_schema = DataSchema::new(merged_fields);

        preread_block.merge_block(remain_block);

        let data_block = preread_block.resort(&merged_schema, &self.output_schema)?;

        Ok((data_block, row_selection, bitmap_selection))
    }

    fn filter_column_chunks<'a>(
        columns_chunks: &'a HashMap<ColumnId, DataItem<'a>>,
        column_ids: &'a HashSet<ColumnId>,
    ) -> Result<HashMap<ColumnId, DataItem<'a>>> {
        let mut filtered_columns_chunks = HashMap::new();
        for (column_id, data_item) in columns_chunks {
            if column_ids.contains(column_id) {
                filtered_columns_chunks.insert(*column_id, data_item.clone());
            }
        }
        Ok(filtered_columns_chunks)
    }
}

fn should_push_down_row_selection(row_selection: &RowSelection, threshold: u64) -> bool {
    let total_rows = row_selection.bitmap.len();
    if threshold == 0 || total_rows == 0 {
        return false;
    }

    (row_selection.selected_rows as u128) * 100 < (total_rows as u128) * (threshold as u128)
}

fn filter_with_selector(
    filter_executor: &mut FilterExecutor,
    block: &DataBlock,
    num_rows: usize,
) -> Result<MutableBitmap> {
    debug_assert_eq!(block.num_rows(), num_rows);
    let mut bitmap = MutableBitmap::from_len_zeroed(num_rows);
    let chunk_size = filter_executor.max_block_size().max(1);

    if num_rows <= chunk_size {
        let selected = filter_executor.select(block)?;
        apply_selection(
            &mut bitmap,
            &filter_executor.true_selection()[..selected],
            0,
        );
        return Ok(bitmap);
    }

    for offset in (0..num_rows).step_by(chunk_size) {
        let end = (offset + chunk_size).min(num_rows);
        let block = block.slice(offset..end);
        let selected = filter_executor.select(&block)?;
        apply_selection(
            &mut bitmap,
            &filter_executor.true_selection()[..selected],
            offset,
        );
    }
    Ok(bitmap)
}

fn apply_selection(bitmap: &mut MutableBitmap, selection: &[u32], offset: usize) {
    for index in selection {
        bitmap.set(offset + *index as usize, true);
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::FromData;
    use databend_common_expression::RawExpr;
    use databend_common_expression::Scalar;
    use databend_common_expression::type_check;
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::MutableBitmap;
    use databend_common_expression::types::StringType;

    use super::*;

    #[test]
    fn test_should_push_down_row_selection() {
        let mut sparse_bitmap = MutableBitmap::from_len_zeroed(5);
        sparse_bitmap.set(2, true);
        let sparse_bitmap: Bitmap = sparse_bitmap.into();
        let sparse_selection = RowSelection::from(&sparse_bitmap);

        assert!(should_push_down_row_selection(&sparse_selection, 50));
        assert!(!should_push_down_row_selection(&sparse_selection, 20));
        assert!(!should_push_down_row_selection(&sparse_selection, 0));

        let dense_bitmap: Bitmap = MutableBitmap::from_len_set(5).into();
        let dense_selection = RowSelection::from(&dense_bitmap);
        assert!(!should_push_down_row_selection(&dense_selection, 100));
    }

    #[test]
    fn test_threshold_zero_disables_row_selection_pushdown() {
        let mut sparse_bitmap = MutableBitmap::from_len_zeroed(5);
        sparse_bitmap.set(2, true);
        let sparse_bitmap: Bitmap = sparse_bitmap.into();
        let sparse_selection = RowSelection::from(&sparse_bitmap);

        assert!(!should_push_down_row_selection(&sparse_selection, 0));
    }

    #[test]
    fn test_apply_selection_accepts_unordered_indices_and_offset() {
        let mut bitmap = MutableBitmap::from_len_zeroed(8);
        apply_selection(&mut bitmap, &[4, 1, 3], 2);
        assert_eq!(bitmap.iter().collect::<Vec<_>>(), vec![
            false, false, false, true, false, true, true, false
        ]);
    }

    #[test]
    fn test_chunked_selector_bitmap_matches_evaluator() {
        let raw_expr = RawExpr::FunctionCall {
            span: None,
            name: "eq".to_string(),
            params: vec![],
            args: vec![
                RawExpr::ColumnRef {
                    span: None,
                    id: 0,
                    data_type: DataType::String,
                    display_name: "value".to_string(),
                },
                RawExpr::Constant {
                    span: None,
                    scalar: Scalar::String("match".to_string()),
                    data_type: Some(DataType::String),
                },
            ],
        };
        let expr = type_check::check(&raw_expr, &BUILTIN_FUNCTIONS).unwrap();
        let (select_expr, _) = SelectExprBuilder::new(&BUILTIN_FUNCTIONS)
            .build(&expr)
            .into();
        assert!(select_expr.contains_string_column_eq_constant());
        let block = DataBlock::new_from_columns(vec![StringType::from_data(vec![
            "other", "match", "other", "match", "match", "other", "match",
        ])]);
        let func_ctx = FunctionContext::default();
        let mut filter_executor = FilterExecutor::new(
            expr.clone(),
            func_ctx.clone(),
            3,
            None,
            &BUILTIN_FUNCTIONS,
            false,
        );

        let actual = filter_with_selector(&mut filter_executor, &block, block.num_rows()).unwrap();
        let expected = Evaluator::new(&block, &func_ctx, &BUILTIN_FUNCTIONS)
            .run(&expr)
            .unwrap()
            .try_downcast::<BooleanType>()
            .unwrap();
        let expected = FilterHelpers::filter_to_bitmap(expected, block.num_rows());

        assert_eq!(
            actual.iter().collect::<Vec<_>>(),
            expected.iter().collect::<Vec<_>>()
        );
    }
}
