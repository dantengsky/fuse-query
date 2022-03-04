//  Copyright 2022 Datafuse Labs.
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

//! This module contains a dedicated thread pool for running "cpu
//! intensive" workloads such as DataFusion plans

use std::pin::Pin;
use std::sync::Arc;

use common_base::tokio;
use common_infallible::Mutex;
use common_tracing::tracing::warn;
use futures::Future;
use pin_project::pin_project;
use pin_project::pinned_drop;
use tokio::sync::oneshot::Receiver;
use tokio_util::sync::CancellationToken;

// from https://thenewstack.io/using-rustlangs-async-tokio-runtime-for-cpu-bound-tasks/

/// Task that can be added to the executor-internal queue.
///
/// Every task within the executor is represented by a [`Job`] that can be polled by the API user.
struct Task {
    fut: Pin<Box<dyn Future<Output = ()> + Send>>,
    cancel: CancellationToken,

    #[allow(dead_code)]
    task_ref: Arc<()>,
}

impl Task {
    /// Run task.
    ///
    /// This runs the payload or cancels if the linked [`Job`] is dropped.
    async fn run(self) {
        tokio::select! {
            _ = self.cancel.cancelled() => (),
            _ = self.fut => (),
        }
    }
}

/// The type of error that is returned from tasks in this module
#[allow(dead_code)]
pub type Error = tokio::sync::oneshot::error::RecvError;

/// Job within the executor.
///
/// Dropping the job will cancel its linked task.
#[pin_project(PinnedDrop)]
pub struct Job<T> {
    cancel: CancellationToken,
    #[pin]
    rx: Receiver<T>,
}

impl<T> Future for Job<T> {
    type Output = Result<T, Error>;

    fn poll(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = self.project();
        this.rx.poll(cx)
    }
}

#[pinned_drop]
impl<T> PinnedDrop for Job<T> {
    fn drop(self: Pin<&mut Self>) {
        self.cancel.cancel();
    }
}

/// Runs futures (and any `tasks` that are `tokio::task::spawned` by
/// them) on a separate tokio Executor
#[derive(Clone)]
pub struct DedicatedExecutor {
    state: Arc<Mutex<State>>,
}

/// Runs futures (and any `tasks` that are `tokio::task::spawned` by
/// them) on a separate tokio Executor
struct State {
    /// Channel for requests -- the dedicated executor takes requests
    /// from here and runs them.
    requests: Option<std::sync::mpsc::Sender<Task>>,

    /// The thread that is doing the work
    thread: Option<std::thread::JoinHandle<()>>,

    /// Task counter (uses Arc strong count).
    task_refs: Arc<()>,
}

/// The default worker priority (value passed to `libc::setpriority`);
const WORKER_PRIORITY: i32 = 10;

impl std::fmt::Debug for DedicatedExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Avoid taking the mutex in debug formatting
        write!(f, "DedicatedExecutor")
    }
}

impl DedicatedExecutor {
    /// Creates a new `DedicatedExecutor` with a dedicated tokio
    /// executor that is separate from the threadpool created via
    /// `[tokio::main]` or similar.
    ///
    /// The worker thread priority is set to low so that such tasks do
    /// not starve other more important tasks (such as answering health checks)
    ///
    /// Follows the example from to stack overflow and spawns a new
    /// thread to install a Tokio runtime "context"
    /// <https://stackoverflow.com/questions/62536566>
    ///
    /// If you try to do this from a async context you see something like
    /// thread 'plan::stringset::tests::test_builder_plan' panicked at 'Cannot
    /// drop a runtime in a context where blocking is not allowed. This
    /// happens when a runtime is dropped from within an asynchronous
    /// context.', .../tokio-1.4.0/src/runtime/blocking/shutdown.rs:51:21
    pub fn new(thread_name: &str, num_threads: usize) -> Self {
        let thread_name = thread_name.to_string();

        let (tx, rx) = std::sync::mpsc::channel::<Task>();

        let thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .thread_name(&thread_name)
                .worker_threads(num_threads)
                .on_thread_start(move || set_current_thread_priority(WORKER_PRIORITY))
                .build()
                .expect("Creating tokio runtime");

            runtime.block_on(async move {
                // Dropping the tokio runtime only waits for tasks to yield not to complete
                //
                // We therefore use a RwLock to wait for tasks to complete
                let join = Arc::new(tokio::sync::RwLock::new(()));

                while let Ok(task) = rx.recv() {
                    let join = Arc::clone(&join);
                    let handle = join.read_owned().await;

                    tokio::task::spawn(async move {
                        task.run().await;
                        std::mem::drop(handle);
                    });
                }

                // Wait for all tasks to finish
                join.write().await;
            })
        });

        let state = State {
            requests: Some(tx),
            thread: Some(thread),
            task_refs: Arc::new(()),
        };

        Self {
            state: Arc::new(Mutex::new(state)),
        }
    }

    /// Runs the specified Future (and any tasks it spawns) on the
    /// `DedicatedExecutor`.
    ///
    /// Currently all tasks are added to the tokio executor
    /// immediately and compete for the threadpool's resources.
    pub fn spawn<T>(&self, task: T) -> Job<T::Output>
    where
        T: Future + Send + 'static,
        T::Output: Send + 'static,
    {
        let (tx, rx) = tokio::sync::oneshot::channel();

        let fut = Box::pin(async move {
            let task_output = task.await;
            if tx.send(task_output).is_err() {
                warn!("Spawned task output ignored: receiver dropped")
            }
        });
        let cancel = CancellationToken::new();
        let mut state = self.state.lock();
        let task = Task {
            fut,
            cancel: cancel.clone(),
            task_ref: Arc::clone(&state.task_refs),
        };

        if let Some(requests) = &mut state.requests {
            // would fail if someone has started shutdown
            requests.send(task).ok();
        } else {
            warn!("tried to schedule task on an executor that was shutdown");
        }

        Job { rx, cancel }
    }

    /// Number of currently active tasks.
    pub fn tasks(&self) -> usize {
        let state = self.state.lock();

        // the strong count is always `1 + jobs` because of the Arc we hold within Self
        Arc::strong_count(&state.task_refs).saturating_sub(1)
    }

    /// signals shutdown of this executor and any Clones
    pub fn shutdown(&self) {
        // hang up the channel which will cause the dedicated thread
        // to quit
        let mut state = self.state.lock();
        state.requests = None;
    }

    /// Stops all subsequent task executions, and waits for the worker
    /// thread to complete. Note this will shutdown all clones of this
    /// `DedicatedExecutor` as well.
    ///
    /// Only the first all to `join` will actually wait for the
    /// executing thread to complete. All other calls to join will
    /// complete immediately.
    pub fn join(&self) {
        self.shutdown();

        // take the thread out when mutex is held
        let thread = {
            let mut state = self.state.lock();
            state.thread.take()
        };

        // wait for completion while not holding the mutex to avoid
        // deadlocks
        if let Some(thread) = thread {
            thread.join().ok();
        }
    }
}

#[cfg(unix)]
fn set_current_thread_priority(_prio: i32) {
    // on linux setpriority sets the current thread's priority
    // (as opposed to the current process).
    // unsafe { libc::setpriority(0, 0, prio) };
}

#[cfg(not(unix))]
fn set_current_thread_priority(prio: i32) {
    warn!("Setting worker thread priority not supported on this platform");
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Barrier;
    use std::time::Duration;

    use tokio::sync::Barrier as AsyncBarrier;

    use super::*;

    #[cfg(unix)]
    fn get_current_thread_priority() -> i32 {
        // on linux setpriority sets the current thread's priority
        // (as opposed to the current process).
        unsafe { libc::getpriority(0, 0) }
    }

    #[cfg(not(unix))]
    fn get_current_thread_priority() -> i32 {
        WORKER_PRIORITY
    }

    #[tokio::test]
    async fn basic() {
        let barrier = Arc::new(Barrier::new(2));

        let exec = DedicatedExecutor::new("Test DedicatedExecutor", 1);
        let dedicated_task = exec.spawn(do_work(42, Arc::clone(&barrier)));

        // Note the dedicated task will never complete if it runs on
        // the main tokio thread (as this test is not using the
        // 'multithreaded' version of the executor and the call to
        // barrier.wait actually blocks the tokio thread)
        barrier.wait();

        // should be able to get the result
        assert_eq!(dedicated_task.await.unwrap(), 42);
    }

    #[tokio::test]
    async fn basic_clone() {
        let barrier = Arc::new(Barrier::new(2));
        let exec = DedicatedExecutor::new("Test DedicatedExecutor", 1);
        // Run task on clone should work fine
        let dedicated_task = exec.clone().spawn(do_work(42, Arc::clone(&barrier)));
        barrier.wait();
        assert_eq!(dedicated_task.await.unwrap(), 42);
    }

    #[tokio::test]
    async fn multi_task() {
        let barrier = Arc::new(Barrier::new(3));

        // make an executor with two threads
        let exec = DedicatedExecutor::new("Test DedicatedExecutor", 2);
        let dedicated_task1 = exec.spawn(do_work(11, Arc::clone(&barrier)));
        let dedicated_task2 = exec.spawn(do_work(42, Arc::clone(&barrier)));

        // block main thread until completion of other two tasks
        barrier.wait();

        // should be able to get the result
        assert_eq!(dedicated_task1.await.unwrap(), 11);
        assert_eq!(dedicated_task2.await.unwrap(), 42);

        exec.join();
    }

    #[tokio::test]
    async fn worker_priority() {
        let exec = DedicatedExecutor::new("Test DedicatedExecutor", 2);

        let dedicated_task = exec.spawn(async move { get_current_thread_priority() });

        assert_eq!(dedicated_task.await.unwrap(), WORKER_PRIORITY);
    }

    #[tokio::test]
    async fn tokio_spawn() {
        let exec = DedicatedExecutor::new("Test DedicatedExecutor", 2);

        // spawn a task that spawns to other tasks and ensure they run on the dedicated
        // executor
        let dedicated_task = exec.spawn(async move {
            // spawn separate tasks
            let t1 = tokio::task::spawn(async {
                assert_eq!(
                    std::thread::current().name(),
                    Some("Test DedicatedExecutor")
                );
                25usize
            });
            t1.await.unwrap()
        });

        // Validate the inner task ran to completion (aka it did not panic)
        assert_eq!(dedicated_task.await.unwrap(), 25);
    }

    #[tokio::test]
    async fn panic_on_executor() {
        let exec = DedicatedExecutor::new("Test DedicatedExecutor", 1);
        let dedicated_task = exec.spawn(async move {
            if true {
                panic!("At the disco, on the dedicated task scheduler");
            } else {
                42
            }
        });

        // should not be able to get the result
        dedicated_task.await.unwrap_err();
    }

    #[tokio::test]
    async fn executor_shutdown_while_task_running() {
        let barrier = Arc::new(Barrier::new(2));
        let captured = Arc::clone(&barrier);

        let exec = DedicatedExecutor::new("Test DedicatedExecutor", 1);
        let dedicated_task = exec.spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
            do_work(42, captured).await
        });

        exec.shutdown();
        // block main thread until completion of the outstanding task
        barrier.wait();

        // task should complete successfully
        assert_eq!(dedicated_task.await.unwrap(), 42);
    }

    #[tokio::test]
    async fn executor_submit_task_after_shutdown() {
        let exec = DedicatedExecutor::new("Test DedicatedExecutor", 1);

        // Simulate trying to submit tasks once executor has shutdown
        exec.shutdown();
        let dedicated_task = exec.spawn(async { 11 });

        // task should complete, but return an error
        dedicated_task.await.unwrap_err();
    }

    #[tokio::test]
    async fn executor_submit_task_after_clone_shutdown() {
        let exec = DedicatedExecutor::new("Test DedicatedExecutor", 1);

        // shutdown the clone (but not the exec)
        exec.clone().join();

        // Simulate trying to submit tasks once executor has shutdown
        let dedicated_task = exec.spawn(async { 11 });

        // task should complete, but return an error
        dedicated_task.await.unwrap_err();
    }

    #[tokio::test]
    async fn executor_join() {
        let exec = DedicatedExecutor::new("Test DedicatedExecutor", 1);
        // test it doesn't hang
        exec.join()
    }

    #[tokio::test]
    #[allow(clippy::redundant_clone)]
    async fn executor_clone_join() {
        let exec = DedicatedExecutor::new("Test DedicatedExecutor", 1);
        // test it doesn't hang
        exec.clone().join();
        exec.clone().join();
        exec.join();
    }

    #[tokio::test]
    async fn drop_receiver() {
        // create empty executor
        let exec = DedicatedExecutor::new("Test DedicatedExecutor", 1);
        assert_eq!(exec.tasks(), 0);

        // create first blocked task
        let barrier1 = Arc::new(AsyncBarrier::new(2));
        let dedicated_task1 = exec.spawn(do_work_async(11, Arc::clone(&barrier1)));
        assert_eq!(exec.tasks(), 1);

        // create second blocked task
        let barrier2 = Arc::new(AsyncBarrier::new(2));
        let dedicated_task2 = exec.spawn(do_work_async(22, Arc::clone(&barrier2)));
        assert_eq!(exec.tasks(), 2);

        // cancel task
        drop(dedicated_task1);

        // cancelation might take a short while
        wait_for_tasks(&exec, 1).await;

        // unblock other task
        barrier2.wait().await;
        assert_eq!(dedicated_task2.await.unwrap(), 22);
        wait_for_tasks(&exec, 0).await;
        assert_eq!(exec.tasks(), 0);

        exec.join()
    }

    /// Wait for the barrier and then return `result`
    async fn do_work(result: usize, barrier: Arc<Barrier>) -> usize {
        barrier.wait();
        result
    }

    /// Wait for the barrier and then return `result`
    async fn do_work_async(result: usize, barrier: Arc<AsyncBarrier>) -> usize {
        barrier.wait().await;
        result
    }

    // waits for up to 1 sec for the correct number of tasks
    async fn wait_for_tasks(exec: &DedicatedExecutor, num: usize) {
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if dbg!(exec.tasks()) == num {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("Did not find expected num tasks within a second")
    }
}
