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

use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use databend_common_base::base::WatchNotify;
use databend_common_catalog::runtime_filter_info::RuntimeFilterReady;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_pipeline::core::OutputPort;
use databend_common_sql::IndexType;

const RUNTIME_FILTER_WAIT_POLL_INTERVAL: Duration = Duration::from_millis(50);

pub async fn wait_runtime_filters(
    scan_id: IndexType,
    output: &Arc<OutputPort>,
    abort_notify: Arc<WatchNotify>,
    runtime_filter_ready: &[Arc<RuntimeFilterReady>],
    wait_timeout: Duration,
) -> Result<()> {
    if wait_timeout.is_zero() {
        return Ok(());
    }

    // Share one budget across all runtime filters for this scan. Waiting the full timeout for
    // each filter serially can otherwise multiply the scan stall when several joins target it.
    let deadline = Instant::now() + wait_timeout;
    for runtime_filter_ready in runtime_filter_ready {
        let mut rx = runtime_filter_ready.runtime_filter_watcher.subscribe();
        if (*rx.borrow()).is_some() {
            continue;
        }

        loop {
            if output.is_finished() {
                return Ok(());
            }

            let now = Instant::now();
            if now >= deadline {
                log::warn!(
                    "Runtime filter wait timeout after {:?} for scan_id: {}",
                    wait_timeout,
                    scan_id
                );
                return Ok(());
            }

            let wait_duration = (deadline - now).min(RUNTIME_FILTER_WAIT_POLL_INTERVAL);
            tokio::select! {
                changed = rx.changed() => {
                    match changed {
                        Ok(()) => break,
                        Err(_) => return Err(ErrorCode::TokioError("watcher's sender is dropped")),
                    }
                }
                _ = abort_notify.notified() => {
                    return Err(ErrorCode::AbortedQuery(
                        "query aborted while waiting for runtime filter",
                    ));
                }
                _ = tokio::time::sleep(wait_duration) => {}
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use databend_common_base::base::WatchNotify;
    use databend_common_base::runtime::spawn;
    use databend_common_pipeline::core::OutputPort;

    use super::*;

    #[tokio::test]
    async fn test_wait_runtime_filters_returns_when_output_finished() {
        let output = OutputPort::create();
        let ready = Arc::new(RuntimeFilterReady::default());
        let abort_notify = Arc::new(WatchNotify::new());

        let output_cloned = output.clone();
        spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            output_cloned.finish();
        });

        tokio::time::timeout(
            Duration::from_millis(300),
            wait_runtime_filters(0, &output, abort_notify, &[ready], Duration::from_secs(30)),
        )
        .await
        .expect("runtime filter wait should stop after branch finish")
        .expect("branch finish should not return an error");
    }

    #[tokio::test]
    async fn test_wait_runtime_filters_returns_when_query_aborted() {
        let output = OutputPort::create();
        let ready = Arc::new(RuntimeFilterReady::default());
        let abort_notify = Arc::new(WatchNotify::new());

        let abort_notify_cloned = abort_notify.clone();
        spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            abort_notify_cloned.notify_waiters();
        });

        let err = tokio::time::timeout(
            Duration::from_millis(300),
            wait_runtime_filters(0, &output, abort_notify, &[ready], Duration::from_secs(30)),
        )
        .await
        .expect("runtime filter wait should stop after query abort")
        .expect_err("query abort should propagate as an error");

        assert_eq!(err.name(), "AbortedQuery");
    }

    #[tokio::test]
    async fn test_wait_runtime_filters_returns_when_filter_notified() {
        let output = OutputPort::create();
        let ready = Arc::new(RuntimeFilterReady::default());
        let abort_notify = Arc::new(WatchNotify::new());

        let ready_cloned = ready.clone();
        spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            ready_cloned
                .runtime_filter_watcher
                .send(Some(()))
                .expect("watcher should stay open");
        });

        tokio::time::timeout(
            Duration::from_millis(300),
            wait_runtime_filters(0, &output, abort_notify, &[ready], Duration::from_secs(30)),
        )
        .await
        .expect("runtime filter wait should stop after filter notification")
        .expect("filter notification should not return an error");
    }

    #[tokio::test]
    async fn test_zero_timeout_does_not_block_scan() {
        let output = OutputPort::create();
        let ready = Arc::new(RuntimeFilterReady::default());
        let abort_notify = Arc::new(WatchNotify::new());

        tokio::time::timeout(
            Duration::from_millis(100),
            wait_runtime_filters(0, &output, abort_notify, &[ready], Duration::ZERO),
        )
        .await
        .expect("zero timeout should return immediately")
        .expect("zero timeout should not return an error");
    }

    #[tokio::test]
    async fn test_runtime_filters_share_one_wait_budget() {
        let output = OutputPort::create();
        let first = Arc::new(RuntimeFilterReady::default());
        let second = Arc::new(RuntimeFilterReady::default());
        let abort_notify = Arc::new(WatchNotify::new());

        tokio::time::timeout(
            Duration::from_millis(180),
            wait_runtime_filters(
                0,
                &output,
                abort_notify,
                &[first, second],
                Duration::from_millis(100),
            ),
        )
        .await
        .expect("two filters should share one 100ms wait budget")
        .expect("wait timeout should not return an error");
    }
}
