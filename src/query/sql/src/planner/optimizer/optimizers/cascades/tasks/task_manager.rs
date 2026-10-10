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

use std::cell::RefCell;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::rc::Rc;
use std::time::Duration;

use databend_common_exception::Result;
use log::info;

use crate::IndexType;
use crate::optimizer::ir::RequiredProperty;
use crate::optimizer::optimizers::CascadesOptimizer;
use crate::optimizer::optimizers::cascades::tasks::SharedCounter;
use crate::optimizer::optimizers::cascades::tasks::Task;

/// Extra waiters of an in-flight `OptimizeGroupTask`.
///
/// The task owns one handle and the scheduler keeps another one in
/// `TaskManager::in_flight_optimize_groups`. Every `SharedCounter` stored here is a
/// clone of the `ref_count` of a task that is blocked on the in-flight task; dropping
/// the list (when the in-flight task finishes) releases all of them at once.
pub type OptimizeGroupWaiters = Rc<RefCell<Vec<SharedCounter>>>;

/// It will cost about 4000ns to execute a task in average,
/// so the default task limit is 1,250,000 which means the
/// optimizer will cost at most 5s to optimize a query.
pub const DEFAULT_TASK_LIMIT: u64 = 1_250_000;

#[derive(Debug, Clone)]
pub struct SchedulerStat {
    pub scheduled_task_count: u64,
    pub explore_task_count: u64,
    pub apply_rule_task_count: u64,
    pub optimize_task_count: u64,
    pub total_execution_time: Duration,

    pub optimize_group_count: u64,
    pub optimize_group_prune_count: u64,
    pub optimize_group_expr_count: u64,
    pub optimize_group_expr_prune_count: u64,
    /// Requests that attached to an in-flight `OptimizeGroupTask` instead of
    /// scheduling a duplicate one.
    pub optimize_group_wait_count: u64,
}

pub struct TaskManager {
    task_queue: VecDeque<Task>,

    /// The maximum number of tasks that can be scheduled.
    /// If the number of scheduled tasks exceeds this limit,
    /// the scheduler will stop scheduling new tasks.
    task_limit: u64,

    /// Statistics of the scheduler.
    pub(super) stat: SchedulerStat,

    /// `OptimizeGroupTask`s that have been scheduled but not finished yet, keyed by
    /// `(group_index, required_prop)`.
    ///
    /// Tasks are scheduled breadth-first, so a group that is reachable through several
    /// parent expressions (e.g. every level of a deep `UNION ALL` chain enumerates two
    /// required distributions for its children) would otherwise get one
    /// `OptimizeGroupTask` per request before any of them finishes and populates
    /// `Group::best_prop`. That doubles the work at every level and blows up
    /// exponentially with plan depth. Requests for an in-flight task attach to it as an
    /// extra waiter instead of scheduling a duplicate.
    in_flight_optimize_groups: HashMap<(IndexType, RequiredProperty), OptimizeGroupWaiters>,
}

impl TaskManager {
    pub fn new() -> Self {
        Self {
            task_queue: Default::default(),
            task_limit: u64::MAX,
            stat: SchedulerStat {
                scheduled_task_count: 0,
                explore_task_count: 0,
                apply_rule_task_count: 0,
                optimize_task_count: 0,
                total_execution_time: Duration::default(),
                optimize_group_count: 0,
                optimize_group_prune_count: 0,
                optimize_group_expr_count: 0,
                optimize_group_expr_prune_count: 0,
                optimize_group_wait_count: 0,
            },
            in_flight_optimize_groups: HashMap::new(),
        }
    }

    /// Attach `waiter` to the in-flight `OptimizeGroupTask` of `(group_index, required_prop)`.
    ///
    /// Returns `false` if there is no such task, in which case the caller must schedule
    /// one (and register it with [`Self::register_optimize_group`]).
    pub fn attach_optimize_group_waiter(
        &mut self,
        group_index: IndexType,
        required_prop: &RequiredProperty,
        waiter: &SharedCounter,
    ) -> bool {
        let key = (group_index, required_prop.clone());
        match self.in_flight_optimize_groups.get(&key) {
            // The task still holds its handle, so it is alive and will release the
            // waiters when it finishes.
            Some(waiters) if Rc::strong_count(waiters) > 1 => {
                waiters.borrow_mut().push(waiter.clone());
                self.stat.optimize_group_wait_count += 1;
                true
            }
            // Stale entry: the task has already been dropped without unregistering
            // itself. Never attach to it, the waiter would block forever.
            Some(_) => {
                self.in_flight_optimize_groups.remove(&key);
                false
            }
            None => false,
        }
    }

    pub fn register_optimize_group(
        &mut self,
        group_index: IndexType,
        required_prop: RequiredProperty,
        waiters: OptimizeGroupWaiters,
    ) {
        self.in_flight_optimize_groups
            .insert((group_index, required_prop), waiters);
    }

    /// Remove the registration of a finished `OptimizeGroupTask`.
    ///
    /// Only removes the entry if it still refers to `waiters`, so a task never
    /// unregisters a newer task for the same key.
    pub fn unregister_optimize_group(
        &mut self,
        group_index: IndexType,
        required_prop: &RequiredProperty,
        waiters: &OptimizeGroupWaiters,
    ) {
        let key = (group_index, required_prop.clone());
        if self
            .in_flight_optimize_groups
            .get(&key)
            .is_some_and(|registered| Rc::ptr_eq(registered, waiters))
        {
            self.in_flight_optimize_groups.remove(&key);
        }
    }

    /// Set the maximum number of tasks that can be scheduled.
    pub fn with_task_limit(mut self, task_limit: u64) -> Self {
        self.task_limit = task_limit;
        self
    }

    pub fn run(&mut self, optimizer: &mut CascadesOptimizer) -> Result<()> {
        let start = std::time::Instant::now();
        while let Some(mut task) = self.task_queue.pop_front() {
            if self.stat.scheduled_task_count > self.task_limit {
                // The number of scheduled tasks exceeds the limit, stop scheduling new tasks.
                info!(
                    "CascadesOptimizer: scheduled task count exceeds limit {}",
                    self.task_limit
                );
                break;
            }

            if task.ref_count() > 0 {
                // The task is still referenced by other tasks, requeue it.
                self.add_task(task);
                continue;
            }

            // Update the counter
            self.stat.scheduled_task_count += 1;
            match task {
                Task::ExploreGroup(_) | Task::ExploreExpr(_) => self.stat.explore_task_count += 1,
                Task::ApplyRule(_) => self.stat.apply_rule_task_count += 1,
                Task::OptimizeGroup(_) => {
                    self.stat.optimize_group_count += 1;
                    self.stat.optimize_task_count += 1;
                }
                Task::OptimizeExpr(_) => {
                    self.stat.optimize_group_expr_count += 1;
                    self.stat.optimize_task_count += 1;
                }
            }

            // Execute the task until it is finished or it is blocked by other tasks.
            while let Some(new_task) = task.execute(optimizer, self)? {
                if new_task.ref_count() > 0 {
                    // The task is still referenced by other tasks, requeue it.
                    self.add_task(new_task);
                    break;
                } else {
                    task = new_task;
                    continue;
                }
            }
        }

        self.stat.total_execution_time = start.elapsed();

        info!(
            "optimizer stats - total task number: {}, total execution time: {:.3}s, average execution time: {}ns, explore task number: {}, apply_rule task number: {}, optimize task number: {}, optimize_group task number: {}, optimize_group pruned number: {}, optimize_group_expr task number: {}, optimize group expr pruned number: {}, optimize_group waited number: {}",
            self.stat.scheduled_task_count,
            self.stat.total_execution_time.as_secs_f64(),
            self.stat.total_execution_time.as_nanos() / self.stat.scheduled_task_count as u128,
            self.stat.explore_task_count,
            self.stat.apply_rule_task_count,
            self.stat.optimize_task_count,
            self.stat.optimize_group_count,
            self.stat.optimize_group_prune_count,
            self.stat.optimize_group_expr_count,
            self.stat.optimize_group_expr_prune_count,
            self.stat.optimize_group_wait_count,
        );

        Ok(())
    }

    pub fn add_task(&mut self, task: Task) {
        self.task_queue.push_back(task);
    }

    pub fn into_stat(self) -> SchedulerStat {
        self.stat
    }
}

impl Default for TaskManager {
    fn default() -> Self {
        Self::new()
    }
}
