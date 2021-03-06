// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn transform_source_test() -> crate::error::FuseQueryResult<()> {
    use futures::TryStreamExt;
    use std::sync::Arc;

    use crate::processors::*;
    use crate::testdata;

    let testdata = testdata::NumberTestData::create();
    let mut pipeline = Pipeline::create();

    let a = testdata.number_source_transform_for_test(8)?;
    pipeline.add_source(Arc::new(a))?;

    let b = testdata.number_source_transform_for_test(8)?;
    pipeline.add_source(Arc::new(b))?;
    pipeline.merge_processor()?;

    let stream = pipeline.execute().await?;
    let blocks = stream.try_collect::<Vec<_>>().await?;
    let rows: usize = blocks.iter().map(|block| block.num_rows()).sum();
    assert_eq!(16, rows);
    Ok(())
}
