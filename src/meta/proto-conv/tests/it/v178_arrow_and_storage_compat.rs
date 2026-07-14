// Copyright 2026 Datafuse Labs.
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

use databend_common_meta_app::principal as mt;
use databend_common_meta_app::storage::*;
use fastrace::func_name;

use crate::common;

#[test]
fn test_v178_arrow_file_format_params() -> anyhow::Result<()> {
    let want = mt::ArrowFileFormatParams {
        missing_field_as: mt::NullAs::Error,
    };
    common::test_pb_from_to(func_name!(), want)?;
    Ok(())
}

#[test]
fn test_v178_file_format_params_arrow() -> anyhow::Result<()> {
    let want = mt::FileFormatParams::Arrow(mt::ArrowFileFormatParams {
        missing_field_as: mt::NullAs::Error,
    });
    common::test_pb_from_to(func_name!(), want)?;
    Ok(())
}

#[test]
fn test_v178_file_format_params_arrow_stream() -> anyhow::Result<()> {
    let want = mt::FileFormatParams::ArrowStream(mt::ArrowFileFormatParams {
        missing_field_as: mt::NullAs::Null,
    });
    common::test_pb_from_to(func_name!(), want)?;
    Ok(())
}

#[test]
fn test_v178_storage_params_azblob() -> anyhow::Result<()> {
    let want = StorageParams::Azblob(StorageAzblobConfig {
        endpoint_url: "https://myaccount.blob.core.windows.net".to_string(),
        container: "mycontainer".to_string(),
        account_name: "myaccount".to_string(),
        account_key: "mykey".to_string(),
        root: "/data".to_string(),
        network_config: None,
    });
    common::test_pb_from_to(func_name!(), want)?;
    Ok(())
}

#[test]
fn test_v178_storage_params_ftp() -> anyhow::Result<()> {
    let want = StorageParams::Ftp(StorageFtpConfig {
        endpoint: "ftp://example.com".to_string(),
        root: "/files".to_string(),
        username: "user".to_string(),
        password: "pass".to_string(),
        network_config: None,
    });
    common::test_pb_from_to(func_name!(), want)?;
    Ok(())
}

#[test]
fn test_v178_storage_params_http() -> anyhow::Result<()> {
    let want = StorageParams::Http(StorageHttpConfig {
        endpoint_url: "https://example.com".to_string(),
        paths: vec!["path/a.csv".to_string(), "path/b.csv".to_string()],
        network_config: None,
    });
    common::test_pb_from_to(func_name!(), want)?;
    Ok(())
}

#[test]
fn test_v178_storage_params_ipfs() -> anyhow::Result<()> {
    let want = StorageParams::Ipfs(StorageIpfsConfig {
        endpoint_url: "https://ipfs.example.com".to_string(),
        root: "/ipfs/QmRoot".to_string(),
        network_config: None,
    });
    common::test_pb_from_to(func_name!(), want)?;
    Ok(())
}

#[test]
fn test_v178_storage_params_memory() -> anyhow::Result<()> {
    let want = StorageParams::Memory;
    common::test_pb_from_to(func_name!(), want)?;
    Ok(())
}
