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

use common_ast::ast::Expr as AExpr;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::DataType;
use common_expression::types::NumberDataType;
use common_expression::types::NumberScalar;
use common_expression::BlockEntry;
use common_expression::DataBlock;
use common_expression::DataSchemaRef;
use common_expression::Scalar;
use common_expression::Value;
use common_pipeline_transforms::processors::Transform;
use indexmap::IndexMap;

use crate::binder::wrap_cast_scalar;
use crate::evaluator::BlockOperator;
use crate::evaluator::CompoundBlockOperator;
use crate::BindContext;
use crate::MetadataRef;
use crate::NameResolutionContext;
use crate::ScalarBinder;

impl BindContext {
    pub async fn exprs_to_scalar(
        &mut self,
        exprs: Vec<AExpr>,
        schema: &DataSchemaRef,
        ctx: Arc<dyn TableContext>,
        name_resolution_ctx: &NameResolutionContext,
        metadata: MetadataRef,
    ) -> Result<Vec<Scalar>> {
        let schema_fields_len = schema.fields().len();
        if exprs.len() != schema_fields_len {
            return Err(ErrorCode::TableSchemaMismatch(format!(
                "Table columns count is not match, expect {schema_fields_len}, input: {}, expr: {:?}",
                exprs.len(),
                exprs
            )));
        }
        let mut scalar_binder = ScalarBinder::new(
            self,
            ctx.clone(),
            name_resolution_ctx,
            metadata.clone(),
            &[],
            HashMap::new(),
            Box::new(IndexMap::new()),
        );

        let mut map_exprs = Vec::with_capacity(exprs.len());
        for (i, expr) in exprs.iter().enumerate() {
            // `DEFAULT` in insert values will be parsed as `Expr::ColumnRef`.
            if let AExpr::ColumnRef { column, .. } = expr {
                if column.name().eq_ignore_ascii_case("default") {
                    let field = schema.field(i);
                    map_exprs.push(scalar_binder.get_default_value(field, schema).await?);
                    continue;
                }
            }

            let (scalar, data_type) = scalar_binder.bind(expr).await?;
            let target_type = schema.field(i).data_type();
            let scalar = wrap_cast_scalar(&scalar, &data_type, target_type)?;
            let expr = scalar
                .as_expr()?
                .project_column_ref(|col| schema.index_of(&col.index.to_string()).unwrap());
            map_exprs.push(expr);
        }

        let operators = vec![BlockOperator::Map {
            exprs: map_exprs,
            projections: None,
        }];

        let one_row_chunk = DataBlock::new(
            vec![BlockEntry::new(
                DataType::Number(NumberDataType::UInt8),
                Value::Scalar(Scalar::Number(NumberScalar::UInt8(1))),
            )],
            1,
        );
        let func_ctx = ctx.get_function_context()?;
        let mut expression_transform = CompoundBlockOperator {
            operators,
            ctx: func_ctx,
        };
        let res = expression_transform.transform(one_row_chunk)?;
        let scalars: Vec<Scalar> = res
            .columns()
            .iter()
            .skip(1)
            .map(|col| unsafe { col.value.as_ref().index_unchecked(0).to_owned() })
            .collect();
        Ok(scalars)
    }
}
