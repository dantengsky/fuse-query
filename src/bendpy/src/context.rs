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

use common_exception::Result;
use common_meta_app::principal::GrantObject;
use common_meta_app::principal::UserInfo;
use common_meta_app::principal::UserPrivilegeSet;
use databend_query::sessions::QueryContext;
use databend_query::sessions::Session;
use databend_query::sessions::SessionManager;
use databend_query::sessions::SessionType;
use databend_query::sql::Planner;
use pyo3::prelude::*;

use crate::dataframe::default_box_size;
use crate::dataframe::PyDataFrame;
use crate::utils::wait_for_future;
use crate::utils::RUNTIME;

#[pyclass(name = "SessionContext", module = "databend", subclass)]
#[derive(Clone)]
pub(crate) struct PySessionContext {
    pub(crate) session: Arc<Session>,
}

#[pymethods]
impl PySessionContext {
    #[new]
    #[pyo3(signature = (tenant = None))]
    fn new(tenant: Option<&str>, py: Python) -> PyResult<Self> {
        let session = RUNTIME.block_on(async {
            let session = SessionManager::instance()
                .create_session(SessionType::Local)
                .await
                .unwrap();

            if let Some(tenant) = tenant {
                session.set_current_tenant(tenant.to_owned());
            } else {
                session.set_current_tenant(uuid::Uuid::new_v4().to_string());
            }

            let mut user = UserInfo::new_no_auth("root", "%");
            user.grants.grant_privileges(
                &GrantObject::Global,
                UserPrivilegeSet::available_privileges_on_global(),
            );

            user.grants.grant_privileges(
                &GrantObject::Global,
                UserPrivilegeSet::available_privileges_on_stage(),
            );

            session.set_authed_user(user, None).await.unwrap();
            session
        });

        let mut res = Self { session };

        res.sql("CREATE DATABASE IF NOT EXISTS default", py)
            .and_then(|df| df.collect(py))?;
        Ok(res)
    }

    fn sql(&mut self, sql: &str, py: Python) -> PyResult<PyDataFrame> {
        let ctx = wait_for_future(py, self.session.create_query_context()).unwrap();
        let res = wait_for_future(py, plan_sql(&ctx, sql));

        match res {
            Err(err) => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Error: {}",
                err
            ))),
            Ok(res) => {
                // if res.df.has_result_set() {
                //     return Ok(res);
                // } else {
                //     return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                //         "Error: sql method only supports SELECT queries",
                //     ));
                // }
                Ok(res)
            }
        }
    }

    fn register_parquet(
        &mut self,
        name: &str,
        path: &str,
        pattern: Option<&str>,
        py: Python,
    ) -> PyResult<()> {
        self.register_table(name, path, "parquet", pattern, py)
    }

    fn register_csv(
        &mut self,
        name: &str,
        path: &str,
        pattern: Option<&str>,
        py: Python,
    ) -> PyResult<()> {
        self.register_table(name, path, "csv", pattern, py)
    }

    fn register_ndjson(
        &mut self,
        name: &str,
        path: &str,
        pattern: Option<&str>,
        py: Python,
    ) -> PyResult<()> {
        self.register_table(name, path, "ndjson", pattern, py)
    }

    fn register_tsv(
        &mut self,
        name: &str,
        path: &str,
        pattern: Option<&str>,
        py: Python,
    ) -> PyResult<()> {
        self.register_table(name, path, "tsv", pattern, py)
    }

    fn register_table(
        &mut self,
        name: &str,
        path: &str,
        file_format: &str,
        pattern: Option<&str>,
        py: Python,
    ) -> PyResult<()> {
        let mut path = path.to_owned();
        if path.starts_with('/') {
            path = format!("fs://{}", path);
        }

        if !path.contains("://") {
            path = format!(
                "fs://{}/{}",
                std::env::current_dir().unwrap().to_str().unwrap(),
                path.as_str()
            );
        }

        // Example: select * from '/home/sundy/dataset/hits_p/' (file_format => 'parquet', pattern => '.*.parquet') limit 3;
        let sql = if let Some(pattern) = pattern {
            format!(
                "create view {} as select * from '{}' (file_format => '{}', pattern => '{}')",
                name, path, file_format, pattern
            )
        } else {
            format!(
                "create view {} as select * from '{}' (file_format => '{}')",
                name, path, file_format
            )
        };

        let _ = self.sql(&sql, py)?.collect(py)?;
        Ok(())
    }
}

async fn plan_sql(ctx: &Arc<QueryContext>, sql: &str) -> Result<PyDataFrame> {
    let mut planner = Planner::new(ctx.clone());
    let (plan, _) = planner.plan_sql(sql).await?;
    Ok(PyDataFrame::new(ctx.clone(), plan, default_box_size()))
}
