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

use common_exception::Result;
use common_expression::type_check::check_function;
use common_expression::Expr;
use common_expression::RemoteExpr;
use common_functions::BUILTIN_FUNCTIONS;
use common_sql::executor::physical_plans::HashJoin;
use parking_lot::RwLock;

use crate::sql::plans::JoinType;

pub const MARKER_KIND_TRUE: u8 = 0;
pub const MARKER_KIND_FALSE: u8 = 1;
pub const MARKER_KIND_NULL: u8 = 2;

pub struct MarkJoinDesc {
    // pub(crate) marker_index: Option<IndexType>,
    pub(crate) has_null: RwLock<bool>,
}

pub struct HashJoinDesc {
    pub(crate) build_keys: Vec<Expr>,
    pub(crate) probe_keys: Vec<Expr>,
    pub(crate) join_type: JoinType,
    pub(crate) other_predicate: Option<Expr>,
    pub(crate) marker_join_desc: MarkJoinDesc,
    /// Whether the Join are derived from correlated subquery.
    pub(crate) from_correlated_subquery: bool,
}

impl HashJoinDesc {
    pub fn create(join: &HashJoin) -> Result<HashJoinDesc> {
        let other_predicate = Self::join_predicate(&join.non_equi_conditions)?;

        let build_keys: Vec<Expr> = join
            .build_keys
            .iter()
            .map(|k| k.as_expr(&BUILTIN_FUNCTIONS))
            .collect();
        let probe_keys: Vec<Expr> = join
            .probe_keys
            .iter()
            .map(|k| k.as_expr(&BUILTIN_FUNCTIONS))
            .collect();

        Ok(HashJoinDesc {
            join_type: join.join_type.clone(),
            build_keys,
            probe_keys,
            other_predicate,
            marker_join_desc: MarkJoinDesc {
                has_null: RwLock::new(false),
                // marker_index: join.marker_index,
            },
            from_correlated_subquery: join.from_correlated_subquery,
        })
    }

    fn join_predicate(non_equi_conditions: &[RemoteExpr]) -> Result<Option<Expr>> {
        non_equi_conditions
            .iter()
            .map(|expr| expr.as_expr(&BUILTIN_FUNCTIONS))
            .try_reduce(|lhs, rhs| {
                check_function(None, "and_filters", &[], &[lhs, rhs], &BUILTIN_FUNCTIONS)
            })
    }
}
