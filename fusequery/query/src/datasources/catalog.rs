// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
//

use std::sync::Arc;

use common_exception::Result;
use common_planners::CreateDatabasePlan;
use common_planners::DropDatabasePlan;

use crate::datasources::Database;
use crate::datasources::Table;
use crate::datasources::TableFunction;

#[async_trait::async_trait]
pub trait Catalog: Sync + Send {
    fn get_database(&self, db_name: &str) -> Result<Arc<dyn Database>>;
    fn get_databases(&self) -> Result<Vec<String>>;
    fn get_table(&self, db_name: &str, table_name: &str) -> Result<Arc<dyn Table>>;
    async fn get_remote_table(
        &self,
        db_name: &str,
        table_name: &str,
    ) -> common_exception::Result<Arc<dyn Table>>;
    fn get_all_tables(&self) -> Result<Vec<(String, Arc<dyn Table>)>>;
    fn get_table_function(&self, name: &str) -> Result<Arc<dyn TableFunction>>;
    async fn create_database(&self, plan: CreateDatabasePlan) -> Result<()>;
    async fn drop_database(&self, plan: DropDatabasePlan) -> Result<()>;
}

struct OverlayCatalog {
    above: Arc<dyn Catalog>,
    under: Arc<dyn Catalog>,
}

#[async_trait::async_trait]
impl Catalog for OverlayCatalog {
    fn get_database(&self, db_name: &str) -> Result<Arc<dyn Database>> {
        self.above
            .get_database(db_name)
            .or_else(|_| self.under.get_database(db_name))
    }

    fn get_databases(&self) -> Result<Vec<String>> {
        self.above
            .get_databases()
            .or_else(|_| self.under.get_databases())
    }

    fn get_table(&self, db_name: &str, table_name: &str) -> Result<Arc<dyn Table>> {
        self.above
            .get_table(db_name, table_name)
            .or_else(|_| self.under.get_table(db_name, table_name))
    }

    async fn get_remote_table(&self, db_name: &str, table_name: &str) -> Result<Arc<dyn Table>> {
        self.above
            .get_remote_table(db_name, table_name)
            .await
            .or_else(|_| self.under.get_remote_table(db_name, table_name))
    }

    fn get_all_tables(&self) -> Result<Vec<(String, Arc<dyn Table>)>> {
        self.above
            .get_all_tables()
            .or_else(|_| self.under.get_all_tables())
    }

    fn get_table_function(&self, name: &str) -> Result<Arc<dyn TableFunction>> {
        self.above
            .get_table_function(name)
            .or_else(|_| self.under.get_table_function(name))
    }

    async fn create_database(&self, plan: CreateDatabasePlan) -> Result<()> {
        todo!()
    }

    async fn drop_database(&self, plan: DropDatabasePlan) -> Result<()> {
        todo!()
    }
}

pub fn overlay_catalog(above: Arc<dyn Catalog>, under: Arc<dyn Catalog>) {
    todo!()
}
