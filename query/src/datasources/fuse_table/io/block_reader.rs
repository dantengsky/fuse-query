//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//
use std::collections::HashMap;
use std::sync::Arc;

use common_arrow::arrow::io::parquet::read::get_page_stream;
use common_arrow::parquet::read::read_metadata_async;
use common_dal::DataAccessor;
use common_datavalues::DataSchema;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::datasources::fuse_table::FuseTable;

impl<T> FuseTable<T>
where T: DataAccessor + Clone
{
    pub(crate) async fn read_partition1(
        &self,
        part_loc: &str,
        projection: &DataSchema,
    ) -> Result<()> {
        let mut input_stream = self.data_accessor.get_input_stream(part_loc, None).await?;

        let metadata = read_metadata_async(&mut input_stream)
            .await
            .map_err(|e| ErrorCode::from_std_error(e))?;

        // TODO
        // we should prune by field-id, instead of by name
        let col_map = metadata.schema_descr.columns().iter().enumerate().fold(
            HashMap::new(),
            |mut v, (i, item)| {
                v.insert(item.base_type().name().to_string(), i);
                v
            },
        );

        let mut proj_idx = vec![];

        for col in projection.fields() {
            let name = col.name();
            if let Some(idx) = col_map.get(col.name()) {
                proj_idx.push(idx)
            } else {
                return Err(ErrorCode::IllegalSchema(format!(
                    "column not exist {}",
                    name
                )));
            }
        }

        // For each parquet file, we arrange EXACTLY ONE row group and ONE page in it,
        // the "motivation" behind this is :...
        //let row_group = 0;
        //let cols = proj_idx
        //    .iter()
        //    .map(|idx| metadata.row_groups[row_group].column(**idx));
        //let col = 0;
        //let meta_ref = &metadata;
        //let mut streams = vec![];
        //for col in proj_idx {
        //    let mut reader = self.data_accessor.get_input_stream(part_loc, None).await?;
        //    let stream = get_page_stream(meta_ref, row_group, *col, &mut reader, None, vec![])
        //        .await
        //        .map_err(|pe| ErrorCode::DALTransportError(pe.to_string()))?;
        //    streams.push(stream);
        //}
        //;        let v = proj_idx.iter().map(|col| {
        //;            let da = self.data_accessor.clone();
        //;            //            let meta = metadata.clone();
        //;            async move {
        //;                let da = self.data_accessor.clone();
        //;                let mut reader = da.get_input_stream(part_loc, None).await?;
        //;                get_page_stream(
        //;                    meta_ref,
        //;                    row_group,
        //;                    **col,
        //;                    &mut reader,
        //;                    None,
        //;                    vec![],
        //;                )
        //;                .await
        //;                .map_err(|pe| ErrorCode::DALTransportError(pe.to_string()))
        //;            }
        //;        });
        //;
        //        let page_streams = futures::stream::iter(proj_idx.iter().map(|col| {
        //            let da = self.data_accessor.clone();
        //            //            let meta = metadata.clone();
        //            async {
        //                let mut reader = da.get_input_stream(part_loc, None).await?;
        //                common_arrow::arrow::io::parquet::read::get_page_stream(
        //                    &metadata,
        //                    row_group,
        //                    **col,
        //                    &mut reader,
        //                    None,
        //                    vec![],
        //                )
        //                .await
        //                .map_err(|pe| ErrorCode::DALTransportError(pe.to_string()))
        //            }
        //        }));

        //let column = 0;
        //let column_metadata = metadata.row_groups[row_group].column(column);

        //        let col_metas = projection

        //        // For simplicity, we do the conversion in-memory, to be optimized later
        //        // TODO consider using `parquet_table` and `stream_parquet`
        //        let write_opt = IpcWriteOptions::default();
        //        let flights =
        //            reader
        //                .into_iter()
        //                .map(|batch| {
        //                    batch.map(
        //                        |b| flight_data_from_arrow_batch(&b, &write_opt).1, /*dictionary ignored*/
        //                    ).map_err(|arrow_err| Status::internal(arrow_err.to_string()))
        //                })
        //                .collect::<Vec<_>>();
        //        let stream = futures::stream::iter(flights);
        //        Ok(Box::pin(stream))
        todo!()
    }
}
