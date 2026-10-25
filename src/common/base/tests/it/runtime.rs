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

use std::sync::mpsc;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

use databend_common_base::runtime::JoinHandle;
use databend_common_base::runtime::Runtime;
use databend_common_base::runtime::Thread;
use databend_common_base::runtime::TrySpawn;
use databend_common_exception::Result;
use rand::distributions::Distribution;
use rand::distributions::Uniform;
use tokio::sync::Semaphore;
use tokio::time::sleep;

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_runtime() -> Result<()> {
    let counter = Arc::new(Mutex::new(0));

    let runtime = Runtime::with_default_worker_threads()?;
    let runtime_counter = Arc::clone(&counter);
    let runtime_header = runtime.spawn(async move {
        let rt1 = Runtime::with_default_worker_threads().unwrap();
        let rt1_counter = Arc::clone(&runtime_counter);
        let rt1_header = rt1.spawn(async move {
            let rt2 = Runtime::with_worker_threads(1, None).unwrap();
            let rt2_counter = Arc::clone(&rt1_counter);
            let rt2_header = rt2.spawn(async move {
                let rt3 = Runtime::with_default_worker_threads().unwrap();
                let rt3_counter = Arc::clone(&rt2_counter);
                let rt3_header = rt3.spawn(async move {
                    let mut num = rt3_counter.lock().unwrap();
                    *num += 1;
                });
                rt3_header.await.unwrap();

                let mut num = rt2_counter.lock().unwrap();
                *num += 1;
            });
            rt2_header.await.unwrap();

            let mut num = rt1_counter.lock().unwrap();
            *num += 1;
        });
        rt1_header.await.unwrap();

        let mut num = runtime_counter.lock().unwrap();
        *num += 1;
    });
    runtime_header.await.unwrap();

    let result = *counter.lock().unwrap();
    assert_eq!(result, 4);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_shutdown_long_run_runtime() -> Result<()> {
    let runtime = Runtime::with_default_worker_threads()?;

    runtime.spawn(async move {
        tokio::time::sleep(Duration::from_secs(6)).await;
    });

    let instant = Instant::now();
    drop(runtime);
    assert!(instant.elapsed() < Duration::from_secs(6));

    Ok(())
}

struct RuntimeOwner {
    _runtime: Arc<Runtime>,
}

#[test]
fn test_runtime_can_be_dropped_from_own_worker() -> anyhow::Result<()> {
    let runtime = Arc::new(Runtime::with_worker_threads(
        1,
        Some("drop-runtime-from-own-worker".to_string()),
    )?);
    let owner = Arc::new(RuntimeOwner {
        _runtime: runtime.clone(),
    });
    let owner_for_task = owner.clone();
    let (release_tx, release_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();

    runtime.spawn(async move {
        release_rx.recv().unwrap();
        drop(owner_for_task);
        let _ = done_tx.send(());
    });

    drop(owner);
    drop(runtime);
    release_tx.send(())?;

    done_rx.recv_timeout(Duration::from_secs(2))?;
    Ok(())
}

#[test]
fn test_runtime_can_be_dropped_from_own_blocking_worker() -> anyhow::Result<()> {
    let runtime = Arc::new(Runtime::with_worker_threads(1, None)?);
    let owner = runtime.clone();
    let (release_tx, release_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    #[allow(clippy::disallowed_methods)]
    runtime.inner().spawn_blocking(move || {
        release_rx.recv().unwrap();
        drop(owner);
        let _ = done_tx.send(());
    });
    drop(runtime);
    release_tx.send(())?;
    done_rx.recv_timeout(Duration::from_secs(2))?;
    Ok(())
}

struct DropSignal(mpsc::Sender<()>);

impl Drop for DropSignal {
    fn drop(&mut self) {
        let _ = self.0.send(());
    }
}

#[test]
fn test_self_dropped_runtime_cleans_up_pending_tasks() -> anyhow::Result<()> {
    let runtime = Arc::new(Runtime::with_worker_threads(1, None)?);
    let (started_tx, started_rx) = mpsc::channel();
    let (cleaned_tx, cleaned_rx) = mpsc::channel();
    let guard = DropSignal(cleaned_tx);
    runtime.spawn(async move {
        let _guard = guard;
        started_tx.send(()).unwrap();
        std::future::pending::<()>().await;
    });
    started_rx.recv_timeout(Duration::from_secs(2))?;

    let owner = runtime.clone();
    let (release_tx, release_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    runtime.spawn(async move {
        release_rx.recv().unwrap();
        drop(owner);
        let _ = done_tx.send(());
    });
    drop(runtime);
    release_tx.send(())?;
    done_rx.recv_timeout(Duration::from_secs(2))?;
    cleaned_rx.recv_timeout(Duration::from_secs(2))?;
    Ok(())
}

#[test]
fn test_runtime_can_be_dropped_when_own_task_is_aborted() -> anyhow::Result<()> {
    // Field order makes notification happen only after Runtime::drop returns.
    struct Owner {
        _runtime: Arc<Runtime>,
        _done: DropSignal,
    }
    let runtime = Arc::new(Runtime::with_worker_threads(1, None)?);
    let (done_tx, done_rx) = mpsc::channel();
    let owner = Owner {
        _runtime: runtime.clone(),
        _done: DropSignal(done_tx),
    };
    let (started_tx, started_rx) = mpsc::channel();
    let task = runtime.spawn(async move {
        let _owner = owner;
        started_tx.send(()).unwrap();
        std::future::pending::<()>().await;
    });
    started_rx.recv_timeout(Duration::from_secs(2))?;
    drop(runtime);
    task.abort();
    done_rx.recv_timeout(Duration::from_secs(2))?;
    Ok(())
}

fn assert_runtime_drop_waits_for_blocking_task(from_other_runtime: bool) -> anyhow::Result<()> {
    let name = Some("same-runtime-name".to_string());
    let runtime = Runtime::with_worker_threads(1, name.clone())?;
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    #[allow(clippy::disallowed_methods)]
    runtime.inner().spawn_blocking(move || {
        started_tx.send(()).unwrap();
        release_rx.recv().unwrap();
    });
    started_rx.recv_timeout(Duration::from_secs(2))?;

    let (dropping_tx, dropping_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let dropper = Thread::spawn(move || {
        let drop_runtime = async move {
            dropping_tx.send(()).unwrap();
            drop(runtime);
            done_tx.send(()).unwrap();
            Ok::<_, anyhow::Error>(())
        };
        if from_other_runtime {
            // Identical names must not make a foreign runtime count as self.
            let other = Runtime::with_worker_threads(1, name).unwrap();
            other.block_on(other.spawn(drop_runtime)).unwrap().unwrap();
        } else {
            futures::executor::block_on(drop_runtime).unwrap();
        }
    });
    dropping_rx.recv_timeout(Duration::from_secs(2))?;
    let before_release = done_rx.recv_timeout(Duration::from_millis(200));
    release_tx.send(())?;
    assert!(matches!(
        before_release,
        Err(mpsc::RecvTimeoutError::Timeout)
    ));
    done_rx.recv_timeout(Duration::from_secs(2))?;
    dropper.join().unwrap();
    Ok(())
}

#[test]
fn test_runtime_dropped_outside_still_waits_for_shutdown() -> anyhow::Result<()> {
    assert_runtime_drop_waits_for_blocking_task(false)
}

#[test]
fn test_same_named_foreign_runtime_still_waits_for_shutdown() -> anyhow::Result<()> {
    assert_runtime_drop_waits_for_blocking_task(true)
}

fn await_task_with_timeout<T: Send + 'static>(task: JoinHandle<T>) -> anyhow::Result<T> {
    let (done_tx, done_rx) = mpsc::channel();
    let waiter = Thread::spawn(move || {
        let _ = done_tx.send(futures::executor::block_on(task));
    });
    let output = done_rx.recv_timeout(Duration::from_secs(2))?;
    waiter.join().unwrap();
    Ok(output?)
}

#[test]
fn test_self_drop_preserves_completed_task_output() -> anyhow::Result<()> {
    let runtime = Arc::new(Runtime::with_worker_threads(2, None)?);
    let owner = runtime.clone();
    let (release_tx, release_rx) = mpsc::channel();
    let task = runtime.spawn(async move {
        release_rx.recv().unwrap();
        let result: Vec<_> = (0..1024).map(|i| i * 17 - 31).collect();
        drop(owner);
        // No await here: a completed polling task must still publish its result.
        result
    });
    drop(runtime);
    release_tx.send(())?;
    assert_eq!(
        await_task_with_timeout(task)?,
        (0..1024).map(|i| i * 17 - 31).collect::<Vec<_>>()
    );
    Ok(())
}

#[test]
fn test_self_shutdown_reports_pending_task_cancellation() -> anyhow::Result<()> {
    let runtime = Arc::new(Runtime::with_worker_threads(1, None)?);
    let (started_tx, started_rx) = mpsc::channel();
    let pending_task = runtime.spawn(async move {
        started_tx.send(()).unwrap();
        std::future::pending::<u64>().await
    });
    started_rx.recv_timeout(Duration::from_secs(5))?;

    let owner = runtime.clone();
    let (release_tx, release_rx) = mpsc::channel();
    let closing_task = runtime.spawn(async move {
        release_rx.recv().unwrap();
        drop(owner);
        42
    });
    drop(runtime);
    release_tx.send(())?;
    assert_eq!(await_task_with_timeout(closing_task)?, 42);
    // A task interrupted by shutdown must fail, not return a fabricated value.
    let error = await_task_with_timeout(pending_task).unwrap_err();
    assert!(error.to_string().contains("cancelled"));
    Ok(())
}

#[test]
fn test_default_runtime_can_be_dropped_from_own_worker() -> anyhow::Result<()> {
    let runtime = Arc::new(Runtime::with_default_worker_threads()?);
    let owner = runtime.clone();
    let (release_tx, release_rx) = mpsc::channel();
    let task = runtime.spawn(async move {
        release_rx.recv().unwrap();
        drop(owner);
        42
    });
    drop(runtime);
    release_tx.send(())?;
    assert_eq!(await_task_with_timeout(task)?, 42);
    Ok(())
}

#[test]
fn test_self_drop_with_foreign_handle_entered() -> anyhow::Result<()> {
    let runtime = Arc::new(Runtime::with_worker_threads(1, Some("same-name".into()))?);
    let other = Runtime::with_worker_threads(1, Some("same-name".into()))?;
    let other_handle = other.inner();
    let owner = runtime.clone();
    let (release_tx, release_rx) = mpsc::channel();
    let task = runtime.spawn(async move {
        release_rx.recv().unwrap();
        // Handle::try_current now reports the foreign runtime, but this OS thread
        // still belongs to `owner` and must not join its shutdown thread.
        let _entered = other_handle.enter();
        drop(owner);
        42
    });
    drop(runtime);
    release_tx.send(())?;
    assert_eq!(await_task_with_timeout(task)?, 42);
    drop(other);
    Ok(())
}

#[test]
fn test_entered_external_thread_still_waits_for_shutdown() -> anyhow::Result<()> {
    let runtime = Runtime::with_worker_threads(1, None)?;
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    #[allow(clippy::disallowed_methods)]
    runtime.inner().spawn_blocking(move || {
        started_tx.send(()).unwrap();
        release_rx.recv().unwrap();
    });
    started_rx.recv_timeout(Duration::from_secs(5))?;

    let (dropping_tx, dropping_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let dropper = Thread::spawn(move || {
        let handle = runtime.inner();
        // Entering the runtime is not the same as being one of its workers.
        let _entered = handle.enter();
        dropping_tx.send(()).unwrap();
        drop(runtime);
        let _ = done_tx.send(());
    });
    dropping_rx.recv_timeout(Duration::from_secs(5))?;
    let before_release = done_rx.recv_timeout(Duration::from_millis(200));
    release_tx.send(())?;
    assert!(matches!(
        before_release,
        Err(mpsc::RecvTimeoutError::Timeout)
    ));
    done_rx.recv_timeout(Duration::from_secs(5))?;
    dropper.join().unwrap();
    Ok(())
}

#[test]
fn test_self_drop_from_block_in_place() -> anyhow::Result<()> {
    let runtime = Arc::new(Runtime::with_worker_threads(1, None)?);
    let owner = runtime.clone();
    let (release_tx, release_rx) = mpsc::channel();
    let task = runtime.spawn(async move {
        release_rx.recv().unwrap();
        #[allow(clippy::disallowed_methods)]
        tokio::task::block_in_place(|| drop(owner));
        42
    });
    drop(runtime);
    release_tx.send(())?;
    assert_eq!(await_task_with_timeout(task)?, 42);
    Ok(())
}

#[test]
fn test_self_drop_from_block_in_place_replacement_worker() -> anyhow::Result<()> {
    let runtime = Arc::new(Runtime::with_worker_threads(1, None)?);
    let (blocked_tx, blocked_rx) = mpsc::channel();
    let (unblock_tx, unblock_rx) = mpsc::channel();
    let blocked_task = runtime.spawn(async move {
        #[allow(clippy::disallowed_methods)]
        tokio::task::block_in_place(|| {
            blocked_tx.send(()).unwrap();
            unblock_rx.recv().unwrap();
        });
        21
    });
    blocked_rx.recv_timeout(Duration::from_secs(5))?;

    // The original worker is inside block_in_place, so this runs on a replacement
    // OS thread. That thread must also receive the runtime ownership token.
    let owner = runtime.clone();
    let (release_tx, release_rx) = mpsc::channel();
    let task = runtime.spawn(async move {
        release_rx.recv().unwrap();
        drop(owner);
        42
    });
    drop(runtime);
    release_tx.send(())?;
    let result = await_task_with_timeout(task);
    // Always unblock the first worker, including when the assertion would fail.
    unblock_tx.send(())?;
    assert_eq!(result?, 42);
    assert_eq!(await_task_with_timeout(blocked_task)?, 21);
    Ok(())
}

#[test]
fn test_self_drop_during_task_panic() -> anyhow::Result<()> {
    let runtime = Arc::new(Runtime::with_worker_threads(1, None)?);
    let owner = runtime.clone();
    let (release_tx, release_rx) = mpsc::channel();
    let task = runtime.spawn(async move {
        let _owner = owner;
        release_rx.recv().unwrap();
        panic!("runtime-self-drop-panic-sentinel");
    });
    drop(runtime);
    release_tx.send(())?;
    let error = await_task_with_timeout(task).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("runtime-self-drop-panic-sentinel")
    );
    Ok(())
}

#[test]
fn test_normal_completion_preserves_results_across_runtime_churn() -> anyhow::Result<()> {
    for generation in 0..16_u64 {
        let runtime = Runtime::with_worker_threads(2, Some("reused-name".into()))?;
        let tasks: Vec<_> = (0..16_u64)
            .map(|task_id| {
                runtime.spawn(async move {
                    tokio::task::yield_now().await;
                    (0..64_u64)
                        .map(|row| generation * 100_000 + task_id * 100 + row)
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        for (task_id, task) in tasks.into_iter().enumerate() {
            assert_eq!(
                await_task_with_timeout(task)?,
                (0..64_u64)
                    .map(|row| generation * 100_000 + task_id as u64 * 100 + row)
                    .collect::<Vec<_>>()
            );
        }
        // All results were consumed before normal synchronous shutdown.
        drop(runtime);
    }
    Ok(())
}

static START_TIME: LazyLock<Instant> = LazyLock::new(Instant::now);

// println can more clearly know if they are parallel
async fn mock_get_page(i: usize) -> Vec<usize> {
    let millis = Uniform::from(0..10).sample(&mut rand::thread_rng());
    println!(
        "[{}] > get_page({}) will complete in {} ms, {:?}",
        START_TIME.elapsed().as_millis(),
        i,
        millis,
        std::thread::current().id(),
    );

    sleep(Duration::from_millis(millis)).await;
    println!(
        "[{}] < get_page({}) completed",
        START_TIME.elapsed().as_millis(),
        i
    );

    (i..(i + 1)).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_runtime_try_spawn_batch() -> Result<()> {
    let runtime = Runtime::with_default_worker_threads()?;

    let mut futs = vec![];
    for i in 0..20 {
        futs.push(mock_get_page(i));
    }

    let max_concurrency = Semaphore::new(3);
    let handlers = runtime.try_spawn_batch(max_concurrency, futs).await?;
    let result = futures::future::try_join_all(handlers).await.unwrap();
    assert_eq!(result.len(), 20);
    Ok(())
}
