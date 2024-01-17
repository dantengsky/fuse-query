use std::collections::HashSet;
use std::fs::File;
use std::io::Write;
use std::sync::Arc;

use databend_common_catalog::catalog::CatalogManager;
use databend_common_config::InnerConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_storages_factory::Table;
use databend_common_storages_fuse::io::MetaReaders;
use databend_common_storages_fuse::FuseTable;
use databend_storages_common_cache::LoadParams;
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::meta::Versioned;
use tokio::sync::mpsc::Sender;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;

pub async fn revise_table_meta(conf: &InnerConfig) -> Result<()> {
    output_message("============");
    output_message(format!("target db [{}]", conf.revise_db));
    output_message(format!("target tables [{}]", conf.revise_tables));
    output_message(format!("parallelism {}", conf.revise_parallel));
    output_message(format!("output file {}", conf.revise_output_file));
    output_message("============");

    let collector = ReviseInfoCollector::try_create(&conf.revise_output_file)?;

    let target_db = {
        let trimmed = conf.revise_db.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_owned())
        }
    };

    let tables = conf.revise_tables.clone();

    let target_tables = {
        let tbls = HashSet::from_iter(
            tables
                .trim()
                .split(',')
                .map(|v| v.trim())
                .filter(|v| !v.is_empty())
                .map(|v| v.trim().to_owned()),
        );
        if tbls.is_empty() { None } else { Some(tbls) }
    };

    let reviser = Reviser {
        tenant: conf.query.tenant_id.to_owned(),
        target_db,
        target_tables,
        collector,
        sem: Arc::new(Semaphore::new(conf.revise_parallel as usize)),
    };

    Arc::new(reviser).scan_table_meta().await?;

    output_message("===DONE===");
    output_message(format!(
        "please check output file {}",
        conf.revise_output_file
    ));

    Ok(())
}
#[derive(Debug)]
struct TableReviseInfo {
    pub db_name: String,
    pub tbl_name: String,
    pub snapshot_need_revised: bool,
    pub segments_need_revised: Vec<String>,
    pub segments_of_older_versions: Vec<String>,
}

impl TableReviseInfo {
    fn new(db_name: String, tbl_name: String) -> Self {
        Self {
            db_name,
            tbl_name,
            snapshot_need_revised: false,
            segments_need_revised: vec![],
            segments_of_older_versions: vec![],
        }
    }
    fn need_revised(&self) -> bool {
        self.snapshot_need_revised || !self.segments_need_revised.is_empty()
    }
}

struct ReviseInfoCollector {
    sender: Sender<TableReviseInfo>,
}

impl ReviseInfoCollector {
    fn try_create(output_file_path: &str) -> Result<Self> {
        let mut output_file: File = File::create(output_file_path)?;
        let (tx, mut rx) = tokio::sync::mpsc::channel::<TableReviseInfo>(10);

        // spawn task that receive TableReviseInfo and write them to file
        tokio::spawn(async move {
            while let Some(table_revise_info) = rx.recv().await {
                output_file.write_all(format!("{:#?}", table_revise_info).as_bytes())?;
                output_file.write_all("================\n".as_bytes())?;
            }

            output_file.flush()
        });

        Ok(Self { sender: tx })
    }

    async fn report(&self, table_revise_info: TableReviseInfo) -> Result<()> {
        self.sender
            .send(table_revise_info)
            .await
            .map_err(ErrorCode::from_std_error)
    }
}

struct Reviser {
    tenant: String,
    target_db: Option<String>,
    target_tables: Option<HashSet<String>>,
    collector: ReviseInfoCollector,
    sem: Arc<Semaphore>,
}

impl Reviser {
    pub async fn scan_table_meta(self: Arc<Self>) -> Result<()> {
        let catalog = CatalogManager::instance().get_default_catalog()?;

        let db_ignored: HashSet<&str> = HashSet::from_iter(vec!["system", "information_schema"]);
        let dbs = catalog.list_databases(&self.tenant).await?;

        // cap protected by semaphore
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

        let target_dbs = dbs
            .iter()
            .filter(|db| {
                !db_ignored.contains(db.name())
                    && (self.target_db.is_none() || db.name() == self.target_db.as_ref().unwrap())
            })
            .collect::<Vec<_>>();

        output_message(format!("{} dbs need to be scanned", target_dbs.len()));

        for db in target_dbs {
            let tables = db.list_tables().await?;

            output_message(format!("scanning db {}", db.name()));

            for table in tables {
                if let Some(tgt_tbls) = &self.target_tables {
                    if !tgt_tbls.contains(table.name()) {
                        continue;
                    }
                }

                let permit = self.acquire_permit().await?;
                let this = self.clone();
                let db_name = db.name().to_owned();
                let tx = tx.clone();
                tokio::spawn(async move {
                    if let Ok(fuse_table) = FuseTable::try_from_table(table.as_ref()) {
                        this.process_table(db_name, fuse_table, permit).await?;
                    }
                    tx.send(()).map_err(ErrorCode::from_std_error)?;
                    Ok::<_, ErrorCode>(())
                });
            }
        }

        drop(tx);
        while let Some(_) = rx.recv().await {}

        Ok(())
    }

    pub async fn process_table(
        &self,
        db_name: String,
        fuse_table: &FuseTable,
        mut permit: OwnedSemaphorePermit,
    ) -> Result<()> {
        output_message(format!(
            "scanning table {}",
            fuse_table.get_table_info().desc
        ));

        let tbl_name = fuse_table.get_table_info().name.clone();
        if let Some(snapshot) = fuse_table.read_table_snapshot().await? {
            let mut table_revise_info = TableReviseInfo::new(db_name, tbl_name.clone());
            table_revise_info.snapshot_need_revised = snapshot.fixed;
            let seg_locs = &snapshot.segments;
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

            output_message(format!(
                "table {} has {} segments to be processed",
                tbl_name,
                seg_locs.len()
            ));

            let mut spawns = 0u32;
            for (path, version) in seg_locs {
                if *version < SegmentInfo::VERSION {
                    // segments of old versions.
                    // no fix is needed, but they might need to be upgraded
                    table_revise_info
                        .segments_of_older_versions
                        .push(path.to_owned());
                } else {
                    // segment of current version (which is 4),
                    // need to check if segment should be revised.
                    let operator = fuse_table.get_operator();
                    let schema = fuse_table.get_table_info().schema();
                    let tx = tx.clone();
                    let path = path.to_owned();
                    let ver = *version;
                    tokio::spawn(async move {
                        let _permit = permit;
                        let segment_reader = MetaReaders::segment_info_reader(operator, schema);
                        let load_parameter = LoadParams {
                            location: path.clone(),
                            len_hint: None,
                            ver,
                            put_cache: false,
                        };
                        let compact_segment_info = segment_reader
                            .read(&load_parameter)
                            .await
                            .expect("read failure");
                        tx.send((compact_segment_info.fixed, path))
                            .expect("receiver should not be closed");
                    });
                    spawns += 1;
                }

                permit = self.acquire_permit().await?;
            }

            // drop the extra tx
            drop(tx);

            let mut got = 0u32;
            while let Some((fixed, path)) = rx.recv().await {
                if fixed {
                    table_revise_info.segments_need_revised.push(path);
                }
                got += 1;

                // Calculate the current progress as a percentage
                let progress = got as f32 / spawns as f32 * 100.0;

                // Log the progress at each 10% interval
                if progress % 10.0 < 5.0 {
                    println!("Progress of table {} : {:.0}%", tbl_name, progress);
                }
            }

            assert_eq!(
                got, spawns,
                "something wrong during processing segment in async task, spawned {}, got {}",
                spawns, got
            );

            if table_revise_info.need_revised() {
                self.collector.report(table_revise_info).await?;
            }
        }
        output_message(format!(
            "scanning table {} done",
            fuse_table.get_table_info().desc
        ));
        Ok(())
    }

    async fn acquire_permit(&self) -> Result<OwnedSemaphorePermit> {
        self.sem
            .clone()
            .acquire_owned()
            .await
            .map_err(ErrorCode::from_std_error)
    }
}

fn output_message(msg: impl AsRef<str>) {
    println!("{}", msg.as_ref())
}
