// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
//

use async_trait::async_trait;
use common_exception::ErrorCode;
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
#[test]
fn test_user_info_converter() {
    let name = "name";
    let pass = "pass";
    let salt = "salt";
    let user = NewUser::new(name, pass, salt);
    let user_info = UserInfo::from(&user);
    assert_eq!(name, &user_info.name);
    let digest: [u8; 32] = sha2::Sha256::digest(pass.as_bytes()).into();
    assert_eq!(digest, user_info.password_sha256);
    let digest: [u8; 32] = sha2::Sha256::digest(salt.as_bytes()).into();
    assert_eq!(digest, user_info.salt_sha256);
}

mod add {
    use super::*;

    #[tokio::test]
    async fn test_add_user() -> common_exception::Result<()> {
        let test_user_name = "test_user";
        let test_password = "test_password";
        let test_salt = "test_salt";
        let new_user = NewUser::new(test_user_name, test_password, test_salt);
        let user_info = UserInfo::from(new_user);
        let value = serde_json::to_vec(&user_info)?;

        let test_key = USER_API_KEY_PREFIX.to_string() + test_user_name;
        let test_seq = Some(0);

        // normal
        {
            let test_key = test_key.clone();
            let mut api = MockKV::new();
            api.expect_upsert_kv()
                .with(
                    predicate::function(move |v| v == test_key.as_str()),
                    predicate::eq(test_seq),
                    predicate::eq(value.clone()),
                )
                .times(1)
                .returning(|_u, _s, _salt| {
                    Ok(UpsertKVActionResult {
                        prev: None,
                        result: None,
                    })
                });
            let mut user_mgr = UserMgr::new(api);
            let res = user_mgr
                .add_user(test_user_name, test_password, test_salt)
                .await;

            assert_eq!(
                res.unwrap_err().code(),
                ErrorCode::UnknownException("").code()
            );
        }

        // already exists
        {
            let test_key = test_key.clone();
            let mut api = MockKV::new();
            api.expect_upsert_kv()
                .with(
                    predicate::function(move |v| v == test_key.as_str()),
                    predicate::eq(test_seq),
                    predicate::eq(value.clone()),
                )
                .times(1)
                .returning(|_u, _s, _salt| {
                    Ok(UpsertKVActionResult {
                        prev: Some((1, vec![])),
                        result: None,
                    })
                });
            let mut user_mgr = UserMgr::new(api);
            let res = user_mgr
                .add_user(test_user_name, test_password, test_salt)
                .await;

            assert_eq!(
                res.unwrap_err().code(),
                ErrorCode::UserAlreadyExists("").code()
            );
        }

        // unknown exception
        {
            let mut api = MockKV::new();
            api.expect_upsert_kv()
                .with(
                    predicate::function(move |v| v == test_key.as_str()),
                    predicate::eq(test_seq),
                    predicate::eq(value.clone()),
                )
                .times(1)
                .returning(|_u, _s, _salt| {
                    Ok(UpsertKVActionResult {
                        prev: None,
                        result: None,
                    })
                });
            let mut user_mgr = UserMgr::new(api);
            let res = user_mgr
                .add_user(test_user_name, test_password, test_salt)
                .await;

            assert_eq!(
                res.unwrap_err().code(),
                ErrorCode::UnknownException("").code()
            );
        }
        Ok(())
    }
}

mod get {
    use super::*;

    #[tokio::test]
    async fn test_get_user_normal() -> common_exception::Result<()> {
        let test_name = "test";
        let test_key = USER_API_KEY_PREFIX.to_string() + test_name;

        let user = NewUser::new(test_name, "pass", "salt");
        let user_info = UserInfo::from(user);
        let value = serde_json::to_vec(&user_info)?;

        let mut kv = MockKV::new();
        kv.expect_get_kv()
            .with(predicate::function(move |v| v == test_key.as_str()))
            .times(1)
            .return_once(move |_k| {
                Ok(GetKVActionResult {
                    result: Some((1, value)),
                })
            });
        let mut user_mgr = UserMgr::new(kv);
        let res = user_mgr.get_user(test_name, None).await;
        assert!(res.is_ok());

        Ok(())
    }

    #[tokio::test]
    async fn test_get_user_not_exist() -> common_exception::Result<()> {
        let test_name = "test";
        let test_key = USER_API_KEY_PREFIX.to_string() + test_name;

        let mut kv = MockKV::new();
        kv.expect_get_kv()
            .with(predicate::function(move |v| v == test_key.as_str()))
            .times(1)
            .return_once(move |_k| Ok(GetKVActionResult { result: None }));
        let mut user_mgr = UserMgr::new(kv);
        let res = user_mgr.get_user(test_name, None).await;
        assert!(res.unwrap().is_none());
        Ok(())
    }

    #[tokio::test]
    async fn test_get_user_invalid_user_info_encoding() -> common_exception::Result<()> {
        let test_name = "test";
        let test_key = USER_API_KEY_PREFIX.to_string() + test_name;

        let mut kv = MockKV::new();
        kv.expect_get_kv()
            .with(predicate::function(move |v| v == test_key.as_str()))
            .times(1)
            .return_once(move |_k| {
                Ok(GetKVActionResult {
                    result: Some((1, vec![1])),
                })
            });
        let mut user_mgr = UserMgr::new(kv);
        let res = user_mgr.get_user(test_name, None).await;
        assert_eq!(
            res.unwrap_err().code(),
            ErrorCode::IllegalUserInfoFormat("").code()
        );

        Ok(())
    }
}

mod get_users {
    use super::*;
    use crate::user::user_mgr::prepend;

    #[tokio::test]
    async fn test_get_users_normal() -> common_exception::Result<()> {
        let mut names = vec![];
        let mut keys = vec![];
        let mut res = vec![];
        let mut user_infos = vec![];
        for i in 0..9 {
            let name = format!("test_user_{}", i);
            names.push(name.clone());
            keys.push(prepend(&name));
            let new_user = NewUser::new(&name, "pass", "salt");
            let user_info = UserInfo::from(new_user);
            if i % 2 == 0 {
                res.push(Some((0, serde_json::to_vec(&user_info)?)));
                user_infos.push(Some(user_info));
            } else {
                res.push(None);
                user_infos.push(None);
            }
        }

        let mut kv = MockKV::new();
        {
            kv.expect_mget_kv()
                .with(predicate::function(move |ks: &[String]| ks == keys))
                .times(1)
                .return_once(move |_: &[String]| Ok(MGetKVActionResult { result: res }));
        }
        let mut user_mgr = UserMgr::new(kv);
        let res = user_mgr.get_users(&names).await?;
        assert_eq!(res, user_infos);

        Ok(())
    }

    #[tokio::test]
    async fn test_get_users_invalid_user_info_encoding() -> common_exception::Result<()> {
        let mut names = vec![];
        let mut keys = vec![];
        let mut res = vec![];
        for i in 0..9 {
            let name = format!("test_user_{}", i);
            names.push(name.clone());
            keys.push(prepend(&name));
            let new_user = NewUser::new(&name, "pass", "salt");
            let user_info = UserInfo::from(new_user);
            res.push(Some((0, serde_json::to_vec(&user_info)?)));
            if i == 8 {
                res.push(Some((0, "some arbitrary string".as_bytes().to_vec())));
            }
        }

        let mut kv = MockKV::new();
        {
            kv.expect_mget_kv()
                .with(predicate::function(move |ks: &[String]| ks == keys))
                .times(1)
                .return_once(move |_: &[String]| Ok(MGetKVActionResult { result: res }));
        }
        let mut user_mgr = UserMgr::new(kv);
        let res = user_mgr.get_users(&names).await;
        assert_eq!(
            res.unwrap_err().code(),
            ErrorCode::IllegalUserInfoFormat("").code()
        );
        Ok(())
    }
}

mod get_all_users {
    use super::*;
    use crate::user::user_mgr::prepend;

    #[tokio::test]
    async fn test_get_all_users_normal() -> common_exception::Result<()> {
        let mut names = vec![];
        let mut keys = vec![];
        let mut res = vec![];
        let mut user_infos = vec![];
        for i in 0..9 {
            let name = format!("test_user_{}", i);
            names.push(name.clone());
            keys.push(prepend(&name));
            let new_user = NewUser::new(&name, "pass", "salt");
            let user_info = UserInfo::from(new_user);
            res.push((0, serde_json::to_vec(&user_info)?));
            user_infos.push(user_info);
        }

        let mut kv = MockKV::new();
        {
            kv.expect_prefix_list_kv()
                .with(predicate::eq(USER_API_KEY_PREFIX))
                .times(1)
                .return_once(|_p| Ok(res));
        }
        let mut user_mgr = UserMgr::new(kv);
        let res = user_mgr.get_all_users().await?;
        assert_eq!(res, user_infos);

        Ok(())
    }

    #[tokio::test]
    async fn test_get_all_users_invalid_user_info_encoding() -> common_exception::Result<()> {
        let mut names = vec![];
        let mut keys = vec![];
        let mut res = vec![];
        for i in 0..9 {
            let name = format!("test_user_{}", i);
            names.push(name.clone());
            keys.push(prepend(&name));
            let new_user = NewUser::new(&name, "pass", "salt");
            let user_info = UserInfo::from(new_user);
            res.push((0, serde_json::to_vec(&user_info)?));
        }
        res.insert(8, (0, "some arbitrary str".as_bytes().to_vec()));

        let mut kv = MockKV::new();
        {
            kv.expect_prefix_list_kv()
                .with(predicate::eq(USER_API_KEY_PREFIX))
                .times(1)
                .return_once(|_p| Ok(res));
        }
        let mut user_mgr = UserMgr::new(kv);
        let res = user_mgr.get_all_users().await;
        assert_eq!(
            res.unwrap_err().code(),
            ErrorCode::IllegalUserInfoFormat("").code()
        );

        Ok(())
    }
}

mod del {
    use super::*;

    #[tokio::test]
    async fn test_del_user_normal_case() -> common_exception::Result<()> {
        let mut kv = MockKV::new();
        let test_key = USER_API_KEY_PREFIX.to_string() + "test";
        kv.expect_delete_kv()
            .with(
                // seems we must use a move closure here
                // predicate:eq(&test_key) will cause lifetime error
                predicate::function(move |v| v == test_key.as_str()),
                predicate::eq(None),
            )
            .times(1)
            .returning(|_k, _seq| Ok(()));
        let mut user_mgr = UserMgr::new(kv);
        let res = user_mgr.drop_user("test", None).await;
        assert!(res.is_ok());

        Ok(())
    }

    #[tokio::test]
    async fn test_del_user_unknown() -> common_exception::Result<()> {
        let mut v = MockKV::new();
        let test_key = USER_API_KEY_PREFIX.to_string() + "test";
        v.expect_delete_kv()
            .with(
                predicate::function(move |v| v == test_key.as_str()),
                predicate::eq(None),
            )
            .times(1)
            .returning(|_k, _seq| Err(ErrorCode::UnknownKey("")));
        let mut user_mgr = UserMgr::new(v);
        let res = user_mgr.drop_user("test", None).await;
        assert_eq!(res.unwrap_err().code(), ErrorCode::UnknownKey("").code());
        Ok(())
    }
}

mod update {
    use super::*;

    #[tokio::test]
    async fn test_update_user_normal_partial_update() -> common_exception::Result<()> {
        let test_name = "name";
        let test_key = USER_API_KEY_PREFIX.to_string() + test_name;
        let test_seq = None;

        let old_pass = "old_key";
        let old_salt = "old_salt";

        let user = NewUser::new(test_name, old_pass, old_salt);
        let user_info = UserInfo::from(user);
        let prev_value = serde_json::to_vec(&user_info)?;

        // get_kv should be called
        let mut kv = MockKV::new();
        {
            let test_key = test_key.clone();
            kv.expect_get_kv()
                .with(predicate::function(move |v| v == test_key.as_str()))
                .times(1)
                .return_once(move |_k| {
                    Ok(GetKVActionResult {
                        result: Some((0, prev_value)),
                    })
                });
        }

        // and then, update_kv should be called

        let new_pass = "new pass";
        let new_salt: Option<&str> = None;
        let new_user = NewUser::new(test_name, new_pass, old_salt);

        let new_user_info = UserInfo::from(new_user);
        let new_value_with_old_salt = serde_json::to_vec(&new_user_info)?;

        kv.expect_update_kv()
            .with(
                predicate::function(move |v| v == test_key.as_str()),
                predicate::eq(test_seq),
                predicate::eq(new_value_with_old_salt),
            )
            .times(1)
            .return_once(|_, _, _| Ok(()));

        let mut user_mgr = UserMgr::new(kv);

        let res = user_mgr
            .update_user(test_name, Some(new_pass), new_salt, test_seq)
            .await;
        assert!(res.is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn test_update_user_normal_full_update() -> common_exception::Result<()> {
        let test_name = "name";
        let test_key = USER_API_KEY_PREFIX.to_string() + test_name;
        let test_seq = None;

        // - get_kv should NOT be called
        // - update_kv should be called

        let new_pass = "new_pass";
        let new_salt = "new_salt";
        let new_user = NewUser::new(test_name, new_pass, new_salt);

        let new_user_info = UserInfo::from(new_user);
        let new_value = serde_json::to_vec(&new_user_info)?;

        let mut kv = MockKV::new();
        kv.expect_update_kv()
            .with(
                predicate::function(move |v| v == test_key.as_str()),
                predicate::eq(test_seq),
                predicate::eq(new_value),
            )
            .times(1)
            .return_once(|_, _, _| Ok(()));

        let mut user_mgr = UserMgr::new(kv);

        let res = user_mgr
            .update_user(test_name, Some(new_pass), Some(new_salt), test_seq)
            .await;
        assert!(res.is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn test_update_user_none_update() -> common_exception::Result<()> {
        // mock kv expects nothing

        let test_name = "name";
        let kv = MockKV::new();
        let mut user_mgr = UserMgr::new(kv);

        let new_password: Option<&str> = None;
        let new_salt: Option<&str> = None;
        let res = user_mgr
            .update_user(test_name, new_password, new_salt, None)
            .await;
        assert!(res.is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn test_update_user_partial_unknown() -> common_exception::Result<()> {
        let test_name = "name";
        let test_key = USER_API_KEY_PREFIX.to_string() + test_name;
        let test_seq = None;

        // if partial update, and get_kv returns None
        // update_kv should NOT be called
        let mut kv = MockKV::new();
        let test_key = test_key.clone();
        kv.expect_get_kv()
            .with(predicate::function(move |v| v == test_key.as_str()))
            .times(1)
            .return_once(move |_k| Ok(GetKVActionResult { result: None }));
        let mut user_mgr = UserMgr::new(kv);

        let new_salt: Option<&str> = None;
        let res = user_mgr
            .update_user(test_name, Some("new_pass"), new_salt, test_seq)
            .await;
        assert_eq!(res.unwrap_err().code(), ErrorCode::UnknownUser("").code());
        Ok(())
    }

    #[tokio::test]
    async fn test_update_user_full_unknown() -> common_exception::Result<()> {
        let test_name = "name";
        let test_key = USER_API_KEY_PREFIX.to_string() + test_name;
        let test_seq = None;

        // get_kv should not be called
        let mut kv = MockKV::new();
        let test_key = test_key.clone();

        // upsert should be called
        // and ErrorCode::UnknownKey should be translated to ErrorCode:UnknownUser
        kv.expect_update_kv()
            .with(
                predicate::function(move |v| v == test_key.as_str()),
                predicate::eq(test_seq),
                predicate::always(), // a little bit relax here, as we've covered it before
            )
            .times(1)
            .returning(|_u, _s, _salt| Err(ErrorCode::UnknownKey("")));

        let mut user_mgr = UserMgr::new(kv);

        let res = user_mgr
            .update_user(test_name, Some("new_pass"), Some("new_salt"), test_seq)
            .await;
        assert_eq!(res.unwrap_err().code(), ErrorCode::UnknownUser("").code());
        Ok(())
    }
}
