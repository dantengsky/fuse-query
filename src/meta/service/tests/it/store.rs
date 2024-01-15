// Copyright 2021 Datafuse Labs.
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

use common_meta_raft_store::sm_v002::leveled_store::sys_data_api::SysDataApiRO;
use common_meta_raft_store::state_machine::testing::snapshot_logs;
use common_meta_sled_store::openraft::async_trait::async_trait;
use common_meta_sled_store::openraft::entry::RaftEntry;
use common_meta_sled_store::openraft::storage::Adaptor;
use common_meta_sled_store::openraft::storage::RaftLogReaderExt;
use common_meta_sled_store::openraft::testing::log_id;
use common_meta_sled_store::openraft::testing::StoreBuilder;
use common_meta_sled_store::openraft::RaftSnapshotBuilder;
use common_meta_sled_store::openraft::RaftStorage;
use common_meta_types::new_log_id;
use common_meta_types::Entry;
use common_meta_types::Membership;
use common_meta_types::StorageError;
use common_meta_types::StoredMembership;
use common_meta_types::TypeConfig;
use common_meta_types::Vote;
use databend_meta::meta_service::meta_node::LogStore;
use databend_meta::meta_service::meta_node::SMStore;
use databend_meta::store::RaftStore;
use databend_meta::Opened;
use log::debug;
use log::info;
use maplit::btreeset;
use minitrace::full_name;
use minitrace::prelude::*;
use pretty_assertions::assert_eq;
use test_harness::test;

use crate::testing::meta_service_test_harness;
use crate::testing::meta_service_test_harness_sync;
use crate::tests::service::MetaSrvTestContext;

struct MetaStoreBuilder {}

#[async_trait]
impl StoreBuilder<TypeConfig, LogStore, SMStore, MetaSrvTestContext> for MetaStoreBuilder {
    async fn build(&self) -> Result<(MetaSrvTestContext, LogStore, SMStore), StorageError> {
        let tc = MetaSrvTestContext::new(555);
        let sto = RaftStore::open_create(&tc.config.raft_config, None, Some(()))
            .await
            .expect("fail to create store");
        let (log_store, sm_store) = Adaptor::new(sto);
        Ok((tc, log_store, sm_store))
    }
}

#[test(harness = meta_service_test_harness_sync)]
#[minitrace::trace]
fn test_impl_raft_storage() -> anyhow::Result<()> {
    let root = Span::root(full_name!(), SpanContext::random());
    let _guard = root.set_local_parent();

    common_meta_sled_store::openraft::testing::Suite::test_all(MetaStoreBuilder {})?;

    Ok(())
}

#[test(harness = meta_service_test_harness)]
#[minitrace::trace]
async fn test_meta_store_restart() -> anyhow::Result<()> {
    // - Create a meta store
    // - Update meta store
    // - Close and reopen it
    // - Test state is restored: hard state, log, state machine

    let id = 3;
    let tc = MetaSrvTestContext::new(id);

    info!("--- new meta store");
    {
        let mut sto = RaftStore::open_create(&tc.config.raft_config, None, Some(())).await?;
        assert_eq!(id, sto.id);
        assert!(!sto.is_opened());
        assert_eq!(None, sto.read_vote().await?);

        info!("--- update metasrv");

        sto.save_vote(&Vote::new(10, 5)).await?;

        sto.append_to_log([Entry::new_blank(log_id(1, 2, 1))])
            .await?;

        sto.save_committed(Some(log_id(1, 2, 2))).await?;

        sto.apply_to_state_machine(&[Entry::new_blank(log_id(1, 2, 2))])
            .await?;
    }

    info!("--- reopen meta store");
    {
        let mut sto = RaftStore::open_create(&tc.config.raft_config, Some(()), None).await?;
        assert_eq!(id, sto.id);
        assert!(sto.is_opened());
        assert_eq!(Some(Vote::new(10, 5)), sto.read_vote().await?);

        assert_eq!(log_id(1, 2, 1), sto.get_log_id(1).await?);
        assert_eq!(Some(log_id(1, 2, 2)), sto.read_committed().await?);
        assert_eq!(
            None,
            sto.last_applied_state().await?.0,
            "state machine is not persisted"
        );
    }
    Ok(())
}

#[test(harness = meta_service_test_harness)]
#[minitrace::trace]
async fn test_meta_store_build_snapshot() -> anyhow::Result<()> {
    // - Create a metasrv
    // - Apply logs
    // - Create a snapshot check snapshot state

    let id = 3;
    let tc = MetaSrvTestContext::new(id);

    let mut sto = RaftStore::open_create(&tc.config.raft_config, None, Some(())).await?;

    info!("--- feed logs and state machine");

    let (logs, want) = snapshot_logs();

    sto.log.write().await.append(logs.clone()).await?;
    sto.state_machine.write().await.apply_entries(&logs).await?;

    let curr_snap = sto.build_snapshot().await?;
    assert_eq!(Some(new_log_id(1, 0, 9)), curr_snap.meta.last_log_id);

    info!("--- check snapshot");
    {
        let data = curr_snap.snapshot;
        let res = data.read_to_lines().await?;

        debug!("res: {:?}", res);

        assert_eq!(want, res);
    }

    info!("--- rebuild other 4 times, keeps only last 3");
    {
        sto.build_snapshot().await?;
        sto.build_snapshot().await?;
        sto.build_snapshot().await?;
        sto.build_snapshot().await?;

        let snapshot_store = sto.snapshot_store();
        let (snapshot_ids, _) = snapshot_store.load_snapshot_ids().await?;
        assert_eq!(3, snapshot_ids.len());
    }

    Ok(())
}

#[test(harness = meta_service_test_harness)]
#[minitrace::trace]
async fn test_meta_store_current_snapshot() -> anyhow::Result<()> {
    // - Create a metasrv
    // - Apply logs
    // - Create a snapshot check snapshot state

    let id = 3;
    let tc = MetaSrvTestContext::new(id);

    let mut sto = RaftStore::open_create(&tc.config.raft_config, None, Some(())).await?;

    info!("--- feed logs and state machine");

    let (logs, want) = snapshot_logs();

    sto.log.write().await.append(logs.clone()).await?;
    {
        let mut sm = sto.state_machine.write().await;
        sm.apply_entries(&logs).await?;
    }

    sto.build_snapshot().await?;

    info!("--- check get_current_snapshot");

    let curr_snap = sto.get_current_snapshot().await?.unwrap();
    assert_eq!(Some(new_log_id(1, 0, 9)), curr_snap.meta.last_log_id);

    info!("--- check snapshot");
    {
        let data = curr_snap.snapshot;
        let res = data.read_to_lines().await?;

        debug!("res: {:?}", res);

        assert_eq!(want, res);
    }

    Ok(())
}

#[test(harness = meta_service_test_harness)]
#[minitrace::trace]
async fn test_meta_store_install_snapshot() -> anyhow::Result<()> {
    // - Create a metasrv
    // - Feed logs
    // - Create a snapshot
    // - Create a new metasrv and restore it by install the snapshot

    let (logs, want) = snapshot_logs();

    let id = 3;
    let snap;
    {
        let tc = MetaSrvTestContext::new(id);

        let mut sto = RaftStore::open_create(&tc.config.raft_config, None, Some(())).await?;

        info!("--- feed logs and state machine");

        sto.log.write().await.append(logs.clone()).await?;
        sto.state_machine.write().await.apply_entries(&logs).await?;

        snap = sto.build_snapshot().await?;
    }

    let data = snap.snapshot;

    info!("--- reopen a new metasrv to install snapshot");
    {
        let tc = MetaSrvTestContext::new(id);

        let mut sto = RaftStore::open_create(&tc.config.raft_config, None, Some(())).await?;

        info!("--- install snapshot");
        {
            sto.do_install_snapshot(data).await?;
        }

        info!("--- check installed meta");
        {
            let mem = sto
                .state_machine
                .write()
                .await
                .sys_data_ref()
                .last_membership_ref()
                .clone();

            assert_eq!(
                StoredMembership::new(
                    Some(log_id(1, 0, 5)),
                    Membership::new(vec![btreeset! {4,5,6}], ())
                ),
                mem
            );

            let last_applied = *sto
                .state_machine
                .write()
                .await
                .sys_data_ref()
                .last_applied_ref();
            assert_eq!(Some(log_id(1, 0, 9)), last_applied);
        }

        info!("--- check snapshot");
        {
            let curr_snap = sto.build_snapshot().await?;
            let data = curr_snap.snapshot;
            let res = data.read_to_lines().await?;

            debug!("res: {:?}", res);

            assert_eq!(want, res);
        }
    }

    Ok(())
}
