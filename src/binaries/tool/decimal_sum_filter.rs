use std::collections::HashSet;
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use databend_common_base::mem_allocator::TrackingGlobalAllocator;
use databend_common_base::runtime::Runtime;
use databend_common_base::runtime::ThreadTracker;
use databend_common_catalog::session_type::SessionType;
use databend_common_config::Config;
use databend_common_config::InnerConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DecimalSize;
use databend_common_license::license_manager::LicenseManager;
use databend_common_license::license_manager::OssLicenseManager;
use databend_common_meta_app::principal::AuthInfo;
use databend_common_meta_app::principal::GrantObject;
use databend_common_meta_app::principal::PasswordHashMethod;
use databend_common_meta_app::principal::UserInfo;
use databend_common_meta_app::principal::UserPrivilegeSet;
use databend_common_sql::Planner;
use databend_common_sql::optimizer::ir::SExpr;
use databend_common_sql::plans::AggregateFunction;
use databend_common_sql::plans::BoundColumnRef;
use databend_common_sql::plans::Plan;
use databend_common_sql::plans::RelOperator;
use databend_common_sql::plans::ScalarExpr;
use databend_common_sql::plans::WindowFuncType;
use databend_common_tracing::set_panic_hook;
use databend_common_version::BUILD_INFO;
use databend_common_version::DATABEND_COMMIT_VERSION;
use databend_query::GlobalServices;
use databend_query::clusters::ClusterDiscovery;
use databend_query::sessions::QueryContext;
use databend_query::sessions::Session;
use databend_query::sessions::SessionManager;
use serde::Deserialize;

#[global_allocator]
pub static GLOBAL_ALLOCATOR: TrackingGlobalAllocator = TrackingGlobalAllocator::create();

#[derive(Parser)]
#[command(
    name = "databend-decimal-sum-filter",
    about = "Filter collected queries containing sum family aggregates over Decimal(19..38) inputs"
)]
struct Args {
    #[arg(short = 'i', long, value_name = "PATH")]
    input: PathBuf,

    #[arg(short = 'o', long, value_name = "PATH")]
    output: PathBuf,

    #[arg(long, value_name = "PATH")]
    unresolved: Option<PathBuf>,

    #[arg(long, default_value_t = 19)]
    min_precision: u8,

    #[arg(long, default_value_t = 38)]
    max_precision: u8,

    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    dedup: bool,

    #[arg(long, short = 'c', value_name = "PATH", default_value_t)]
    config_file: String,

    #[command(flatten)]
    config: Config,
}

#[derive(Deserialize)]
struct CollectedQuery {
    query_id: Option<String>,
    query_text: String,
    current_database: Option<String>,
}

#[derive(Default)]
struct Stats {
    total_rows: usize,
    planned_rows: usize,
    matched_rows: usize,
    matched_unique_rows: usize,
    unresolved_rows: usize,
    duplicated_rows: usize,
}

#[derive(Clone)]
struct DecimalSumMatch {
    location: &'static str,
    display_name: String,
    argument: ArgumentInfo,
    data_type: DataType,
    precision: u8,
    scale: u8,
}

#[derive(Clone)]
struct ArgumentInfo {
    column_name: Option<String>,
    table_name: Option<String>,
    database_name: Option<String>,
    binding_index: String,
}

fn main() {
    ThreadTracker::init();

    match Runtime::with_default_worker_threads() {
        Err(cause) => {
            eprintln!("databend-decimal-sum-filter start failure: {cause:?}");
            std::process::exit(cause.code() as i32);
        }
        Ok(rt) => {
            if let Err(cause) = rt.block_on(run()) {
                eprintln!("databend-decimal-sum-filter failed: {cause:?}");
                std::process::exit(cause.code() as i32);
            }
        }
    }
}

async fn run() -> Result<()> {
    let args = Args::parse();
    validate_precision_range(&args)?;

    set_panic_hook(DATABEND_COMMIT_VERSION.clone());

    let config = args.config.clone().merge(&args.config_file)?;
    let conf = InnerConfig::init(config, true).await?;
    init_query_services(&conf).await?;

    let session = create_planner_session().await?;
    let ctx = session.create_query_context(&BUILD_INFO).await?;

    if let Some(parent) = args.output.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)?;
    }
    let unresolved_path = args
        .unresolved
        .clone()
        .unwrap_or_else(|| default_unresolved_path(&args.output));
    if let Some(parent) = unresolved_path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)?;
    }

    let mut output = BufWriter::new(File::create(&args.output)?);
    let mut unresolved = BufWriter::new(File::create(&unresolved_path)?);

    write_output_header(&mut output, &args)?;
    write_unresolved_header(&mut unresolved, &args)?;

    let mut stats = Stats::default();
    let mut seen_sql = HashSet::new();
    let input = BufReader::new(File::open(&args.input)?);

    for (line_index, line) in input.lines().enumerate() {
        let line_no = line_index + 1;
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }

        stats.total_rows += 1;
        let row = match serde_json::from_str::<CollectedQuery>(&line) {
            Ok(row) => row,
            Err(cause) => {
                stats.unresolved_rows += 1;
                write_unresolved(
                    &mut unresolved,
                    line_no,
                    None,
                    None,
                    &format!("invalid JSONL row: {cause}"),
                )?;
                continue;
            }
        };

        if !maybe_contains_sum(&row.query_text) {
            continue;
        }

        stats.planned_rows += 1;
        let matches = match find_decimal_sum_matches(&ctx, &row, &args).await {
            Ok(matches) => matches,
            Err(cause) => {
                stats.unresolved_rows += 1;
                write_unresolved(
                    &mut unresolved,
                    line_no,
                    row.query_id.as_deref(),
                    Some(&row.query_text),
                    &cause.message(),
                )?;
                continue;
            }
        };

        if matches.is_empty() {
            continue;
        }

        stats.matched_rows += 1;
        let normalized = normalize_sql(&row.query_text);
        if args.dedup && !seen_sql.insert(normalized) {
            stats.duplicated_rows += 1;
            continue;
        }

        stats.matched_unique_rows += 1;
        write_match(&mut output, line_no, &row, &matches)?;
    }

    output.flush()?;
    unresolved.flush()?;

    eprintln!(
        "[decimal-sum-filter] rows={} planned={} matched={} unique={} unresolved={} duplicates={}",
        stats.total_rows,
        stats.planned_rows,
        stats.matched_rows,
        stats.matched_unique_rows,
        stats.unresolved_rows,
        stats.duplicated_rows,
    );
    eprintln!("[decimal-sum-filter] output: {}", args.output.display());
    eprintln!(
        "[decimal-sum-filter] unresolved: {}",
        unresolved_path.display()
    );

    Ok(())
}

fn validate_precision_range(args: &Args) -> Result<()> {
    if args.min_precision == 0 || args.min_precision > args.max_precision {
        return Err(ErrorCode::BadArguments(format!(
            "invalid precision range: {}..{}",
            args.min_precision, args.max_precision
        )));
    }
    Ok(())
}

async fn init_query_services(conf: &InnerConfig) -> Result<()> {
    GlobalServices::init(conf, &BUILD_INFO, false).await?;
    OssLicenseManager::init(conf.query.tenant_id.tenant_name().to_string())?;
    ClusterDiscovery::instance()
        .register_to_metastore(conf)
        .await?;
    Ok(())
}

async fn create_planner_session() -> Result<Arc<Session>> {
    let mut user_info = UserInfo::new("root", "%", AuthInfo::Password {
        hash_method: PasswordHashMethod::Sha256,
        hash_value: Vec::from("pass"),
        need_change: false,
    });
    user_info.grants.grant_privileges(
        &GrantObject::Global,
        UserPrivilegeSet::available_privileges_on_global(),
    );

    let session_manager = SessionManager::instance();
    let session = session_manager.create_session(SessionType::Dummy).await?;
    let session = session_manager.register_session(session)?;
    session.set_authed_user(user_info, None).await?;
    session.get_settings().set_max_threads(8)?;
    Ok(session)
}

async fn find_decimal_sum_matches(
    ctx: &Arc<QueryContext>,
    row: &CollectedQuery,
    args: &Args,
) -> Result<Vec<DecimalSumMatch>> {
    let database = row
        .current_database
        .as_deref()
        .filter(|database| !database.is_empty())
        .unwrap_or("default");
    ctx.set_current_database(database.to_string()).await?;

    let mut planner = Planner::new(ctx.clone());
    let (plan, _) = planner.plan_sql(&row.query_text).await?;
    let Plan::Query { s_expr, .. } = plan else {
        return Ok(vec![]);
    };

    let mut matches = vec![];
    collect_matches(s_expr.as_ref(), args, &mut matches)?;
    dedup_matches(&mut matches);
    Ok(matches)
}

fn collect_matches(s_expr: &SExpr, args: &Args, matches: &mut Vec<DecimalSumMatch>) -> Result<()> {
    match s_expr.plan() {
        RelOperator::Aggregate(aggregate) => {
            for item in &aggregate.aggregate_functions {
                if let ScalarExpr::AggregateFunction(func) = &item.scalar {
                    inspect_aggregate_function("aggregate", func, args, matches)?;
                }
            }
        }
        RelOperator::Window(window) => {
            if let WindowFuncType::Aggregate(func) = &window.function {
                inspect_aggregate_function("window", func, args, matches)?;
            }
        }
        _ => {}
    }

    for child in s_expr.children() {
        collect_matches(child, args, matches)?;
    }
    Ok(())
}

fn inspect_aggregate_function(
    location: &'static str,
    func: &AggregateFunction,
    args: &Args,
    matches: &mut Vec<DecimalSumMatch>,
) -> Result<()> {
    if !is_sum_family(&func.func_name) {
        return Ok(());
    }

    let Some(argument) = func.args.first() else {
        return Ok(());
    };
    let data_type = argument.data_type()?;
    let Some(decimal) = decimal_size(&data_type) else {
        return Ok(());
    };
    let precision = decimal.precision();
    if precision < args.min_precision || precision > args.max_precision {
        return Ok(());
    }

    matches.push(DecimalSumMatch {
        location,
        display_name: func.display_name.clone(),
        argument: argument_info(argument),
        data_type,
        precision,
        scale: decimal.scale(),
    });
    Ok(())
}

fn decimal_size(data_type: &DataType) -> Option<DecimalSize> {
    match data_type {
        DataType::Decimal(size) => Some(*size),
        DataType::Nullable(inner) => decimal_size(inner),
        _ => None,
    }
}

fn is_sum_family(func_name: &str) -> bool {
    matches!(
        func_name.to_ascii_lowercase().as_str(),
        "sum" | "sum_if" | "sum_distinct" | "sum_state"
    )
}

fn dedup_matches(matches: &mut Vec<DecimalSumMatch>) {
    let mut seen = HashSet::new();
    matches.retain(|item| seen.insert(match_key(item)));
}

fn match_key(item: &DecimalSumMatch) -> String {
    format!(
        "{}|{}|{}|{}|{}|{}|{}",
        item.location,
        item.display_name,
        item.data_type,
        item.argument.database_name.as_deref().unwrap_or(""),
        item.argument.table_name.as_deref().unwrap_or(""),
        item.argument.column_name.as_deref().unwrap_or(""),
        item.argument.binding_index
    )
}

fn argument_info(argument: &ScalarExpr) -> ArgumentInfo {
    match argument {
        ScalarExpr::BoundColumnRef(BoundColumnRef { column, .. }) => ArgumentInfo {
            column_name: Some(column.column_name.clone()),
            table_name: column.table_name.clone(),
            database_name: column.database_name.clone(),
            binding_index: column.index.to_string(),
        },
        _ => ArgumentInfo {
            column_name: None,
            table_name: None,
            database_name: None,
            binding_index: String::new(),
        },
    }
}

fn maybe_contains_sum(sql: &str) -> bool {
    sql.to_ascii_lowercase().contains("sum")
}

fn normalize_sql(sql: &str) -> String {
    sql.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn default_unresolved_path(output: &Path) -> PathBuf {
    let mut path = output.to_path_buf();
    let file_name = output
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("decimal_sum.sql");
    path.set_file_name(format!("{file_name}.unresolved.sql"));
    path
}

fn write_output_header(out: &mut dyn Write, args: &Args) -> Result<()> {
    writeln!(out, "-- Databend decimal sum filter")?;
    writeln!(
        out,
        "-- Matches sum family inputs with Decimal precision in [{}..{}].",
        args.min_precision, args.max_precision
    )?;
    writeln!(
        out,
        "-- Queries are planned through Databend binder; derived aliases use bound argument types."
    )?;
    writeln!(out)?;
    Ok(())
}

fn write_unresolved_header(out: &mut dyn Write, args: &Args) -> Result<()> {
    writeln!(out, "-- Databend decimal sum filter unresolved queries")?;
    writeln!(
        out,
        "-- These rows could not be parsed or planned and should be reviewed separately."
    )?;
    writeln!(
        out,
        "-- Intended precision range: [{}..{}].",
        args.min_precision, args.max_precision
    )?;
    writeln!(out)?;
    Ok(())
}

fn write_match(
    out: &mut dyn Write,
    line_no: usize,
    row: &CollectedQuery,
    matches: &[DecimalSumMatch],
) -> Result<()> {
    writeln!(
        out,
        "-- Collected line: L{} | query_id: {}",
        line_no,
        row.query_id.as_deref().unwrap_or("")
    )?;
    if let Some(database) = row.current_database.as_ref() {
        writeln!(out, "-- Database context: {database}")?;
    }
    for item in matches {
        write_match_comment(out, item)?;
    }
    write_sql(out, &row.query_text)?;
    writeln!(out)?;
    Ok(())
}

fn write_match_comment(out: &mut dyn Write, item: &DecimalSumMatch) -> Result<()> {
    write!(
        out,
        "-- Match: {} {} | arg_type: {} | Decimal({}, {})",
        item.location, item.display_name, item.data_type, item.precision, item.scale
    )?;
    if item.argument.database_name.is_some()
        || item.argument.table_name.is_some()
        || item.argument.column_name.is_some()
    {
        write!(out, " | bound_arg: ")?;
        if let Some(database) = item.argument.database_name.as_ref() {
            write!(out, "{database}.")?;
        }
        if let Some(table) = item.argument.table_name.as_ref() {
            write!(out, "{table}.")?;
        }
        if let Some(column) = item.argument.column_name.as_ref() {
            write!(out, "{column}")?;
        }
    }
    if !item.argument.binding_index.is_empty() {
        write!(out, " | binding: #{}", item.argument.binding_index)?;
    }
    writeln!(out)?;
    Ok(())
}

fn write_unresolved(
    out: &mut dyn Write,
    line_no: usize,
    query_id: Option<&str>,
    sql: Option<&str>,
    reason: &str,
) -> Result<()> {
    writeln!(
        out,
        "-- Collected line: L{} | query_id: {}",
        line_no,
        query_id.unwrap_or("")
    )?;
    writeln!(out, "-- Reason: {}", reason.replace('\n', "\\n"))?;
    if let Some(sql) = sql {
        write_sql(out, sql)?;
    }
    writeln!(out)?;
    Ok(())
}

fn write_sql(out: &mut dyn Write, sql: &str) -> Result<()> {
    let trimmed = sql.trim();
    if trimmed.ends_with(';') {
        writeln!(out, "{trimmed}")?;
    } else {
        writeln!(out, "{trimmed};")?;
    }
    Ok(())
}
