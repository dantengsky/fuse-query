use std::collections::BTreeMap;
use std::string::FromUtf8Error;

use databend_common_grpc::RpcClientConf;
use databend_common_meta_client::MetaGrpcClient;
use databend_common_meta_kvapi::kvapi::KVApi;
use databend_common_meta_types::NodeInfo;
use tokio;

pub static CLUSTER_API_KEY_PREFIX: &str = "__fd_clusters_v3";
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = RpcClientConf {
        endpoints: vec!["0.0.0.0:9191".to_owned()],
        username: "root".to_string(),
        password: "root".to_string(),
        tls_conf: None,
        timeout: None,
        auto_sync_interval: None,
        unhealthy_endpoint_evict_time: Default::default(),
    };

    let client = MetaGrpcClient::try_new(&config)?;

    let mut tenant_clusters = BTreeMap::new();
    let reply = client.prefix_list_kv(CLUSTER_API_KEY_PREFIX).await?;
    for (node_key, value) in reply {
        let node_key = unescape_for_key(&node_key[CLUSTER_API_KEY_PREFIX.len() + 1..])?;
        let elements = node_key.split('/').collect::<Vec<&str>>();
        let tenant = elements[0];
        let cluster_id = elements[1];
        let node_id = elements[3];
        let mut node_info = serde_json::from_slice::<NodeInfo>(&value.data)?;
        node_info.id = node_id.to_string();
        tenant_clusters
            .entry(tenant.to_string())
            .or_insert_with(BTreeMap::new)
            .entry(cluster_id.to_string())
            .or_insert_with(Vec::new)
            .push(node_info);
    }
    println!("{:#?}", tenant_clusters);
    Ok(())
}

// from src/common/base/src/base/string.rs
/// Function that escapes special characters in a string.
///
/// All characters except digit, alphabet and '_' are treated as special characters.
/// A special character will be converted into "%num" where num is the hexadecimal form of the character.
///
/// # Example
/// ```
/// let key = "data_bend!!";
/// let new_key = escape_for_key(&key);
/// assert_eq!(Ok("data_bend%21%21".to_string()), new_key);
/// ```
pub fn escape_for_key(key: &str) -> std::result::Result<String, FromUtf8Error> {
    let mut new_key = Vec::with_capacity(key.len());

    fn hex(num: u8) -> u8 {
        match num {
            0..=9 => b'0' + num,
            10..=15 => b'a' + (num - 10),
            unreachable => unreachable!("Unreachable branch num = {}", unreachable),
        }
    }

    for char in key.as_bytes() {
        match char {
            b'0'..=b'9' => new_key.push(*char),
            b'_' | b'a'..=b'z' | b'A'..=b'Z' => new_key.push(*char),
            _other => {
                new_key.push(b'%');
                new_key.push(hex(*char / 16));
                new_key.push(hex(*char % 16));
            }
        }
    }

    String::from_utf8(new_key)
}

// from src/common/base/src/base/string.rs
/// The reverse function of escape_for_key.
///
/// # Example
/// ```
/// let key = "data_bend%21%21";
/// let original_key = unescape_for_key(&key);
/// assert_eq!(Ok("data_bend!!".to_string()), original_key);
/// ```
pub fn unescape_for_key(key: &str) -> std::result::Result<String, FromUtf8Error> {
    let mut new_key = Vec::with_capacity(key.len());

    fn unhex(num: u8) -> u8 {
        match num {
            b'0'..=b'9' => num - b'0',
            b'a'..=b'f' => num - b'a' + 10,
            unreachable => unreachable!("Unreachable branch num = {}", unreachable),
        }
    }

    let bytes = key.as_bytes();

    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'%' => {
                // The last byte of the string won't be '%'
                let mut num = unhex(bytes[index + 1]) * 16;
                num += unhex(bytes[index + 2]);
                new_key.push(num);
                index += 3;
            }
            other => {
                new_key.push(other);
                index += 1;
            }
        }
    }

    String::from_utf8(new_key)
}
