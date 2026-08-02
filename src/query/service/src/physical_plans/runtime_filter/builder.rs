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
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::RemoteExpr;
use databend_common_expression::types::DataType;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_sql::ColumnEntry;
use databend_common_sql::IndexType;
use databend_common_sql::MetadataRef;
use databend_common_sql::Symbol;
use databend_common_sql::TypeCheck;
use databend_common_sql::optimizer::ir::SExpr;
use databend_common_sql::plans::Aggregate;
use databend_common_sql::plans::Exchange;
use databend_common_sql::plans::Join;
use databend_common_sql::plans::JoinEquiCondition;
use databend_common_sql::plans::JoinType;
use databend_common_sql::plans::RelOperator;
use databend_common_sql::plans::ScalarExpr;

use super::types::PhysicalRuntimeFilter;
use super::types::PhysicalRuntimeFilters;
use super::types::SpatialRuntimeFilterMode;
use crate::sessions::TableContext;

/// Type alias for probe keys with runtime filter information
/// Contains: (RemoteExpr, scan_id, table_index, column_idx, is_null_equal)
type ProbeKeysWithRuntimeFilter = Vec<Option<(RemoteExpr<String>, usize, usize, Symbol, bool)>>;

/// Check if a data type is supported for bloom filter
///
/// Currently supports: numbers and strings
pub fn is_type_supported_for_bloom_filter(data_type: &DataType) -> bool {
    data_type.is_number() || data_type.is_string()
}

/// Check if a data type is supported for min-max filter
///
/// Currently supports: numbers, dates, and strings
pub fn is_type_supported_for_min_max_filter(data_type: &DataType) -> bool {
    data_type.is_number() || data_type.is_date() || data_type.is_string()
}

/// Check if the join type is supported for runtime filter
///
/// Runtime filters are only applicable to certain join types where
/// filtering the probe side can reduce processing
pub fn supported_join_type_for_runtime_filter(join_type: &JoinType) -> bool {
    matches!(
        join_type,
        JoinType::Inner
            | JoinType::LeftSemi
            | JoinType::Right
            | JoinType::RightSemi
            | JoinType::RightAnti
            | JoinType::LeftMark
    )
}

/// Resolve the table statistics used to decide whether a bloom runtime filter is selective.
///
/// Most build keys directly reference a base-table column. A grouped `MIN` or `MAX`, however,
/// produces a derived column even though every value still comes from one base-table column. If
/// the aggregate reads exactly one table, use that table as the statistics source as well. This
/// keeps the runtime selectivity check available for the common "join to latest row" pattern
/// without enabling bloom filters for arbitrary derived expressions or multi-table aggregates.
pub(in crate::physical_plans) fn resolve_runtime_filter_build_table_index(
    metadata: &MetadataRef,
    build_side: &SExpr,
    build_column: Symbol,
) -> Option<IndexType> {
    if let ColumnEntry::BaseTableColumn(column) = metadata.read().column(build_column) {
        return Some(column.table_index);
    }

    resolve_min_max_aggregate_table_index(metadata, build_side, build_column)
}

fn resolve_min_max_aggregate_table_index(
    metadata: &MetadataRef,
    s_expr: &SExpr,
    output_column: Symbol,
) -> Option<IndexType> {
    match s_expr.plan() {
        RelOperator::Aggregate(aggregate) => {
            if let Some(table_index) = min_max_aggregate_table_index(
                metadata,
                aggregate,
                s_expr.unary_child(),
                output_column,
            ) {
                return Some(table_index);
            }
            None
        }
        RelOperator::EvalScalar(eval_scalar) => {
            if let Some(item) = eval_scalar
                .items
                .iter()
                .find(|item| item.index == output_column)
            {
                let ScalarExpr::BoundColumnRef(column) = &item.scalar else {
                    return None;
                };
                return resolve_min_max_aggregate_table_index(
                    metadata,
                    s_expr.unary_child(),
                    column.column.index,
                );
            }
            resolve_min_max_aggregate_table_index(metadata, s_expr.unary_child(), output_column)
        }
        RelOperator::Filter(_)
        | RelOperator::Sort(_)
        | RelOperator::Limit(_)
        | RelOperator::Exchange(_) => {
            resolve_min_max_aggregate_table_index(metadata, s_expr.unary_child(), output_column)
        }
        _ => None,
    }
}

fn min_max_aggregate_table_index(
    metadata: &MetadataRef,
    aggregate: &Aggregate,
    input: &SExpr,
    output_column: Symbol,
) -> Option<IndexType> {
    let item = aggregate
        .aggregate_functions
        .iter()
        .find(|item| item.index == output_column)?;
    let ScalarExpr::AggregateFunction(function) = &item.scalar else {
        return None;
    };
    if !matches!(function.func_name.as_str(), "min" | "max")
        || function.distinct
        || function.args.len() != 1
    {
        return None;
    }

    let ScalarExpr::BoundColumnRef(source_column) = &function.args[0] else {
        return None;
    };
    let table_index = match metadata.read().column(source_column.column.index) {
        ColumnEntry::BaseTableColumn(column) => column.table_index,
        _ => return None,
    };

    (single_scan_table_index(input) == Some(table_index)).then_some(table_index)
}

fn single_scan_table_index(s_expr: &SExpr) -> Option<IndexType> {
    match s_expr.plan() {
        RelOperator::Scan(scan) => Some(scan.table_index),
        RelOperator::Aggregate(_)
        | RelOperator::EvalScalar(_)
        | RelOperator::Filter(_)
        | RelOperator::Sort(_)
        | RelOperator::Limit(_)
        | RelOperator::Exchange(_) => single_scan_table_index(s_expr.unary_child()),
        _ => None,
    }
}

/// Build runtime filters for a join operation
///
/// This is the legacy method that creates one runtime filter per probe key.
/// For equivalence class propagation, use the enhanced version in physical_hash_join.rs
///
/// # Arguments
/// * `ctx` - Table context
/// * `metadata` - Metadata reference
/// * `join` - Join plan
/// * `s_expr` - SExpr for the join
/// * `build_keys` - Build side keys
/// * `probe_keys` - Probe side keys with scan_id, table_index, and column_idx
///
/// # Returns
/// Collection of runtime filters to be applied
pub async fn build_runtime_filter(
    ctx: Arc<dyn TableContext>,
    metadata: &MetadataRef,
    join: &Join,
    s_expr: &SExpr,
    build_keys: &[RemoteExpr],
    probe_keys: ProbeKeysWithRuntimeFilter,
    build_table_indexes: Vec<Option<IndexType>>,
    spatial_modes: Vec<Option<SpatialRuntimeFilterMode>>,
) -> Result<PhysicalRuntimeFilters> {
    if !ctx.get_settings().get_enable_join_runtime_filter()? {
        return Ok(Default::default());
    }

    if !supported_join_type_for_runtime_filter(&join.join_type) {
        return Ok(Default::default());
    }

    let build_side = s_expr.build_side_child();
    let build_side_data_distribution = build_side.get_data_distribution()?;
    if build_side_data_distribution.as_ref().is_some_and(|e| {
        !matches!(
            e,
            Exchange::Broadcast
                | Exchange::NodeToNodeHash(_)
                | Exchange::GlobalHash(_)
                | Exchange::Merge
        )
    }) {
        return Ok(Default::default());
    }

    let mut filters = Vec::new();

    let probe_side = s_expr.probe_side_child();

    // Process each probe key that has runtime filter information
    for (
        build_key,
        probe_key,
        scan_id,
        probe_table_index,
        column_idx,
        is_null_equal,
        build_table_index,
        spatial_mode,
    ) in build_keys
        .iter()
        .zip(probe_keys.into_iter())
        .zip(build_table_indexes.into_iter())
        .zip(spatial_modes.into_iter())
        .filter_map(|(((b, p), table_idx), spatial_mode)| {
            p.map(|(p, scan_id, table_index, column_idx, is_null_equal)| {
                (
                    b,
                    p,
                    scan_id,
                    table_index,
                    column_idx,
                    is_null_equal,
                    table_idx,
                    spatial_mode,
                )
            })
        })
    {
        if !supported_probe_key_for_runtime_filter(&probe_key) {
            continue;
        }

        let probe_targets =
            find_probe_targets(metadata, probe_side, &probe_key, scan_id, column_idx)?;

        let build_table_rows =
            get_build_table_rows(ctx.clone(), metadata, build_table_index).await?;
        let probe_table_rows =
            get_build_table_rows(ctx.clone(), metadata, Some(probe_table_index)).await?;

        let data_type = build_key
            .as_expr(&BUILTIN_FUNCTIONS)
            .data_type()
            .remove_nullable();
        let id = metadata.write().next_runtime_filter_id();

        let enable_bloom_runtime_filter =
            !is_null_equal && is_type_supported_for_bloom_filter(&data_type);

        let enable_min_max_runtime_filter =
            !is_null_equal && is_type_supported_for_min_max_filter(&data_type);

        let enable_inlist_runtime_filter = !is_null_equal && spatial_mode.is_none();

        // Create and add the runtime filter
        let runtime_filter = PhysicalRuntimeFilter {
            id,
            build_key: build_key.clone(),
            probe_targets,
            build_table_rows,
            probe_table_rows,
            enable_bloom_runtime_filter,
            enable_inlist_runtime_filter,
            enable_min_max_runtime_filter,
            spatial_mode,
        };
        filters.push(runtime_filter);
    }

    Ok(PhysicalRuntimeFilters { filters })
}

async fn get_build_table_rows(
    ctx: Arc<dyn TableContext>,
    metadata: &MetadataRef,
    build_table_index: Option<IndexType>,
) -> Result<Option<u64>> {
    if let Some(table_index) = build_table_index {
        let table = {
            let metadata_read = metadata.read();
            metadata_read.table(table_index).table().clone()
        };

        let table_stats = table.table_statistics(ctx, false, None).await?;
        return Ok(table_stats.and_then(|s| s.num_rows));
    }

    Ok(None)
}

fn find_probe_targets(
    metadata: &MetadataRef,
    s_expr: &SExpr,
    probe_key: &RemoteExpr<String>,
    probe_scan_id: usize,
    probe_key_col_idx: Symbol,
) -> Result<Vec<(RemoteExpr<String>, usize)>> {
    let mut uf = UnionFind::new();
    let mut column_to_remote: HashMap<Symbol, (RemoteExpr<String>, usize)> = HashMap::new();
    column_to_remote.insert(probe_key_col_idx, (probe_key.clone(), probe_scan_id));

    let equi_conditions = collect_equi_conditions(s_expr)?;
    for cond in equi_conditions {
        if let (
            Some((left_remote, left_scan_id, left_idx)),
            Some((right_remote, right_scan_id, right_idx)),
        ) = (
            scalar_to_remote_expr(metadata, &cond.left)?,
            scalar_to_remote_expr(metadata, &cond.right)?,
        ) {
            uf.union(left_idx, right_idx);
            column_to_remote.insert(left_idx, (left_remote, left_scan_id));
            column_to_remote.insert(right_idx, (right_remote, right_scan_id));
        }
    }

    let equiv_class = uf.get_equivalence_class(probe_key_col_idx);

    let mut result = Vec::new();
    for idx in equiv_class {
        if let Some((remote_expr, scan_id)) = column_to_remote.get(&idx) {
            result.push((remote_expr.clone(), *scan_id));
        }
    }

    Ok(result)
}

fn collect_equi_conditions(s_expr: &SExpr) -> Result<Vec<JoinEquiCondition>> {
    let mut conditions = Vec::new();

    if let RelOperator::Join(join) = s_expr.plan() {
        if matches!(join.join_type, JoinType::Inner) {
            conditions.extend(join.equi_conditions.clone());
        }
    }

    for child in s_expr.children() {
        conditions.extend(collect_equi_conditions(child)?);
    }

    Ok(conditions)
}

fn scalar_to_remote_expr(
    metadata: &MetadataRef,
    scalar: &ScalarExpr,
) -> Result<Option<(RemoteExpr<String>, usize, Symbol)>> {
    let used_columns = scalar.used_columns();
    if used_columns.len() != 1 {
        return Ok(None);
    }

    let column_idx = *used_columns.iter().next().unwrap();
    if !matches!(
        metadata.read().column(column_idx),
        ColumnEntry::BaseTableColumn(_)
    ) {
        return Ok(None);
    }

    let Some(scan_id) = metadata.read().base_column_scan_id(column_idx) else {
        return Ok(None);
    };

    let remote_expr = {
        let md = metadata.read();
        scalar
            .as_raw_expr()
            .type_check(&*md)?
            .project_column_ref(|col| {
                let entry = md.column(col.index);
                if let ColumnEntry::BaseTableColumn(base_col) = entry {
                    if base_col.path_indices.is_none() {
                        let table = md.table(base_col.table_index);
                        let schema = table.table().schema_with_stream();
                        if let Ok(field) = schema.field_of_column_id(base_col.column_id) {
                            return Ok(field.name().clone());
                        }
                    }
                    return Ok(base_col.column_name.clone());
                }
                Ok(col.column_name.clone())
            })?
            .as_remote_expr()
    };

    if supported_probe_key_for_runtime_filter(&remote_expr) {
        return Ok(Some((remote_expr, scan_id, column_idx)));
    }

    Ok(None)
}

fn supported_probe_key_for_runtime_filter(probe_key: &RemoteExpr<String>) -> bool {
    match probe_key {
        RemoteExpr::ColumnRef { .. } => true,
        // Support simple cast that only changes nullability, e.g. CAST(col AS Nullable(T)).
        RemoteExpr::Cast {
            expr, dest_type, ..
        } => matches!(
            expr.as_ref(),
            RemoteExpr::ColumnRef { data_type, .. } if &dest_type.remove_nullable() == data_type
        ),
        _ => false,
    }
}

struct UnionFind {
    parent: HashMap<Symbol, Symbol>,
}

impl UnionFind {
    fn new() -> Self {
        Self {
            parent: HashMap::new(),
        }
    }

    fn find(&mut self, x: Symbol) -> Symbol {
        if !self.parent.contains_key(&x) {
            self.parent.insert(x, x);
            return x;
        }

        let parent = *self.parent.get(&x).unwrap();
        if parent != x {
            let root = self.find(parent);
            self.parent.insert(x, root);
        }
        *self.parent.get(&x).unwrap()
    }

    fn union(&mut self, x: Symbol, y: Symbol) {
        let root_x = self.find(x);
        let root_y = self.find(y);
        if root_x != root_y {
            self.parent.insert(root_x, root_y);
        }
    }

    fn get_equivalence_class(&mut self, x: Symbol) -> Vec<Symbol> {
        let root = self.find(x);
        let all_keys: Vec<_> = self.parent.keys().copied().collect();
        all_keys
            .into_iter()
            .filter(|&k| self.find(k) == root)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use databend_common_expression::TableDataType;
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::NumberDataType;
    use databend_common_sql::ColumnBindingBuilder;
    use databend_common_sql::ColumnSet;
    use databend_common_sql::MetadataRef;
    use databend_common_sql::Visibility;
    use databend_common_sql::optimizer::ir::SExpr;
    use databend_common_sql::plans::Aggregate;
    use databend_common_sql::plans::AggregateFunction;
    use databend_common_sql::plans::AggregateMode;
    use databend_common_sql::plans::BoundColumnRef;
    use databend_common_sql::plans::Exchange;
    use databend_common_sql::plans::RelOperator;
    use databend_common_sql::plans::ScalarExpr;
    use databend_common_sql::plans::ScalarItem;
    use databend_common_sql::plans::Scan;

    use super::resolve_runtime_filter_build_table_index;

    fn aggregate_build_side(
        function_name: &str,
        scan_table_index: usize,
    ) -> (MetadataRef, SExpr, databend_common_sql::Symbol) {
        let metadata = MetadataRef::default();
        let data_type = DataType::Number(NumberDataType::UInt64);
        let source_column = metadata.write().add_base_table_column(
            "id".to_string(),
            TableDataType::Number(NumberDataType::UInt64),
            scan_table_index,
            None,
            0,
            None,
            None,
        );
        let aggregate_column = metadata
            .write()
            .add_derived_column(format!("{function_name}(id)"), data_type.clone());

        let source_expr = ScalarExpr::BoundColumnRef(BoundColumnRef {
            span: None,
            column: ColumnBindingBuilder::new(
                "id".to_string(),
                source_column,
                Box::new(data_type.clone()),
                Visibility::Visible,
            )
            .table_index(Some(scan_table_index))
            .build(),
        });
        let aggregate = Aggregate {
            mode: AggregateMode::Final,
            group_items: vec![],
            aggregate_functions: vec![ScalarItem {
                index: aggregate_column,
                scalar: ScalarExpr::AggregateFunction(AggregateFunction {
                    span: None,
                    func_name: function_name.to_string(),
                    distinct: false,
                    params: vec![],
                    args: vec![source_expr],
                    return_type: Box::new(data_type),
                    sort_descs: vec![],
                    display_name: format!("{function_name}(id)"),
                }),
            }],
            from_distinct: false,
            rank_limit: None,
            grouping_sets: None,
        };
        let scan = Scan {
            table_index: scan_table_index,
            columns: ColumnSet::from([source_column]),
            ..Default::default()
        };
        let partial = SExpr::create_unary(
            Arc::new(RelOperator::Aggregate(Aggregate {
                mode: AggregateMode::Partial,
                ..aggregate.clone()
            })),
            Arc::new(SExpr::create_leaf(Arc::new(RelOperator::Scan(scan)))),
        );
        let exchange = SExpr::create_unary(
            Arc::new(RelOperator::Exchange(Exchange::Merge)),
            Arc::new(partial),
        );
        let build_side = SExpr::create_unary(
            Arc::new(RelOperator::Aggregate(aggregate)),
            Arc::new(exchange),
        );

        (metadata, build_side, aggregate_column)
    }

    #[test]
    fn test_resolve_min_max_aggregate_build_table() {
        for function_name in ["min", "max"] {
            let (metadata, build_side, aggregate_column) = aggregate_build_side(function_name, 7);
            assert_eq!(
                resolve_runtime_filter_build_table_index(&metadata, &build_side, aggregate_column),
                Some(7)
            );
        }
    }

    #[test]
    fn test_do_not_resolve_arbitrary_aggregate_build_table() {
        let (metadata, build_side, aggregate_column) = aggregate_build_side("sum", 7);
        assert_eq!(
            resolve_runtime_filter_build_table_index(&metadata, &build_side, aggregate_column),
            None
        );
    }
}
