// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
//

use std::collections::HashMap;

use async_trait::async_trait;
use common_exception::ErrorCode;
use common_exception::Result;
use common_store_api::kv_api::MGetKVActionResult;
use common_store_api::kv_api::PrefixListReply;
use common_store_api::GetKVActionResult;
use common_store_api::KVApi;
use common_store_api::UpsertKVActionResult;
use mockall::predicate::*;
use mockall::*;
use sha2::Digest;

use crate::user::user_info::NewUser;
use crate::user::user_info::UserInfo;
use crate::user::user_mgr::USER_API_KEY_PREFIX;
use crate::user::UserMgr;

// and mock!
mock! {
    pub KV {}
    #[async_trait]
    impl KVApi for KV {
        async fn upsert_kv(
            &mut self,
            key: &str,
            seq: Option<u64>,
            value: Vec<u8>,
        ) -> common_exception::Result<UpsertKVActionResult>;
    async fn delete_kv(&mut self, key: &str, seq: Option<u64>) -> common_exception::Result<()>;
    async fn update_kv(
        &mut self,
        key: &str,
        seq: Option<u64>,
        value: Vec<u8>,
    ) -> common_exception::Result<()>;

    async fn get_kv(&mut self, key: &str) -> common_exception::Result<GetKVActionResult>;

    // TODO 'static in introduced by mock, let's figure how to avoid it later.
    async fn mget_kv<T: 'static + AsRef<str> + Sync>(
        &mut self,
        key: &[T],
    ) -> common_exception::Result<MGetKVActionResult>;

    async fn prefix_list_kv(&mut self, prefix: &str) -> common_exception::Result<PrefixListReply>;
    }
}

#[tokio::test]
async fn test_add_user() -> common_exception::Result<()> {
    let mut v = MockKV::new();
    let test_key = USER_API_KEY_PREFIX.to_string() + "test";

    v.expect_delete_kv()
        //.with(predicate::eq(key), predicate::eq(None))
        .with(
            // seems we must use a move closure here
            // predicate:eq(&test_key) will cause lifetime error
            predicate::function(move |v| v == test_key.as_str()),
            predicate::eq(None),
        )
        .times(1)
        .returning(|_k, _seq| Ok(()));
    let mut user_mgr = UserMgr::new(v);
    let res = user_mgr.drop_user("test", None).await;
    assert!(res.is_ok());

    Ok(())
}

//#[tokio::test]
//async fn test_add_user() -> common_exception::Result<()> {
//    let mock = TK::new();
//    let mut user_mgr = UserMgr::new(mock);
//    let r = user_mgr.add_user("usename", "password", "1").await?;
//    Ok(())
//}

//#[tokio::test]
//async fn test_drop_user() -> common_exception::Result<()> {
//    let mock = MockKVApi::new();
//    let mut user_mgr = UserMgr::new(mock);
//    let r = user_mgr.add_user("usename", "password", "1").await?;
//    Ok(())
//}
//
//#[tokio::test]
//async fn test_update_user() -> common_exception::Result<()> {
//    let mock = MockKV::new();
//    let mut user_mgr = UserMgr::new(mock);
//    let r = user_mgr.add_user("usename", "password", "1").await?;
//    Ok(())
//}
//
