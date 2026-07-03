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

use std::collections::BTreeMap;
use std::env;
use std::fs::File;
use std::io;
use std::io::BufRead;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Write;
use std::time::Duration;

use clap::Parser;
use databend_common_base::base::GlobalUniqName;
use databend_common_config::Commands;
use databend_common_config::Config;
use databend_common_config::InnerConfig;
use databend_common_config::DATABEND_COMMIT_VERSION;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::Scalar;
use databend_common_license::license_manager::LicenseManager;
use databend_common_license::license_manager::OssLicenseManager;
use databend_common_meta_app::principal::GrantObject;
use databend_common_meta_app::principal::UserInfo;
use databend_common_meta_app::principal::UserPrivilegeSet;
use databend_query::sessions::Session;
use databend_query::sessions::SessionManager;
use databend_query::sessions::SessionType;
use databend_query::sql::format_scalar;
use databend_query::sql::optimizer::SExpr;
use databend_query::sql::plans::Exchange;
use databend_query::sql::plans::InsertInputSource;
use databend_query::sql::plans::Plan;
use databend_query::sql::plans::RelOperator;
use databend_query::sql::plans::ScalarExpr;
use databend_query::sql::plans::WindowFuncType;
use databend_query::sql::Planner;
use databend_query::GlobalServices;
use serde::Serialize;
use serde_json::json;
use serde_json::Map;
use serde_json::Value;
use serfig::collectors::from_env;
use serfig::collectors::from_file;
use serfig::collectors::from_self;
use serfig::parsers::Toml;
use tokio::time::timeout;

const QUERY_INFO_FIELDS: &[&str] = &[
    "current_database",
    "sql_user",
    "query_start_time",
    "cluster_id",
    "node_id",
    "query_duration_ms",
    "query_parameterized_hash",
];

#[derive(Debug)]
struct ToolArgs {
    input: Option<String>,
    output: Option<String>,
    progress_every: usize,
    plan_timeout_ms: u64,
    report_plan_failures: bool,
}

impl Default for ToolArgs {
    fn default() -> Self {
        Self {
            input: None,
            output: None,
            progress_every: 100,
            plan_timeout_ms: 30_000,
            report_plan_failures: false,
        }
    }
}

#[derive(Debug)]
struct QueryRecord {
    line_no: usize,
    raw: Value,
    sql: String,
    database: String,
    sql_user: String,
}

#[derive(Debug, Serialize)]
struct Evidence {
    location: String,
    cast_expr: String,
    cast_source_expr: String,
    cast_source_type: String,
    cast_target_type: String,
    inner_source_expr: String,
    inner_source_type: String,
    nearest_if_expr: String,
    decimal_context_exprs: Vec<String>,
    coalesce_like_rewrite: bool,
}

#[derive(Default)]
struct Summary {
    records: usize,
    skipped_bad_lines: usize,
    planned: usize,
    plan_failed: usize,
    findings: usize,
    high: usize,
    plan_failed_summary: BTreeMap<String, usize>,
}

fn main() {
    if let Err(cause) = databend_common_base::runtime::Runtime::with_default_worker_threads()
        .and_then(|rt| rt.block_on(run()))
    {
        eprintln!("coalesce-risk-analyzer-636 failed: {:?}", cause);
        std::process::exit(cause.code() as i32);
    }
}

async fn run() -> Result<()> {
    let (tool_args, databend_args) = split_args()?;
    let conf = load_inner_config(databend_args).await?;

    GlobalServices::init(&conf).await?;
    OssLicenseManager::init(conf.query.tenant_id.tenant_name().to_string())?;

    let session = create_session("root").await?;
    let mut current_user = "root".to_string();

    let mut reader = open_input(&tool_args)?;
    let mut writer = open_output(&tool_args)?;
    let mut line = String::new();
    let mut summary = Summary::default();

    loop {
        line.clear();
        if reader.read_line(&mut line)? == 0 {
            break;
        }

        let Some(record) = parse_record(summary.records + summary.skipped_bad_lines + 1, &line)
        else {
            summary.skipped_bad_lines += 1;
            continue;
        };

        summary.records += 1;
        if current_user != record.sql_user {
            set_session_user(&session, &record.sql_user).await?;
            current_user = record.sql_user.clone();
        }

        if should_report_progress(summary.records, tool_args.progress_every) {
            eprintln!(
                "[planning] record={}, line_no={}",
                summary.records, record.line_no
            );
        }

        match plan_record(&session, &record, tool_args.plan_timeout_ms).await {
            Ok(plan) => {
                summary.planned += 1;
                let mut evidence = Vec::new();
                analyze_plan(&plan, "plan", &mut evidence);

                for item in evidence {
                    let risk =
                        if item.coalesce_like_rewrite && !item.decimal_context_exprs.is_empty() {
                            "HIGH_636_STRING_TO_INTEGER_CAST"
                        } else if item.coalesce_like_rewrite {
                            "POSSIBLE_636_COALESCE_STRING_TO_INTEGER_CAST"
                        } else {
                            "POSSIBLE_STRING_TO_INTEGER_CAST_IN_DECIMAL_CONTEXT"
                        };

                    if risk.starts_with("HIGH_") {
                        summary.high += 1;
                    }
                    summary.findings += 1;

                    let output = finding_value(&record, risk, item)?;
                    serde_json::to_writer(&mut writer, &output)?;
                    writer.write_all(b"\n")?;
                }
            }
            Err(cause) => {
                summary.plan_failed += 1;
                *summary
                    .plan_failed_summary
                    .entry(error_category(&cause))
                    .or_insert(0) += 1;

                if tool_args.report_plan_failures {
                    let output = plan_failure_value(&record, &cause);
                    serde_json::to_writer(&mut writer, &output)?;
                    writer.write_all(b"\n")?;
                }
            }
        }

        if tool_args.progress_every > 0 && summary.records % tool_args.progress_every == 0 {
            eprintln!(
                "[progress] records={}, planned={}, findings={}, high={}, plan_failed={}",
                summary.records,
                summary.planned,
                summary.findings,
                summary.high,
                summary.plan_failed
            );
        }
    }

    writer.flush()?;
    print_summary(&summary);
    Ok(())
}

fn should_report_progress(records: usize, progress_every: usize) -> bool {
    progress_every > 0 && (records == 1 || records % progress_every == 0)
}

fn split_args() -> Result<(ToolArgs, Vec<String>)> {
    let mut tool_args = ToolArgs::default();
    let mut databend_args = Vec::new();
    let mut args = env::args();

    if let Some(program) = args.next() {
        databend_args.push(program);
    }

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--help" | "-h" => {
                print_help();
                std::process::exit(0);
            }
            "--input" | "-i" => {
                tool_args.input = Some(next_arg_value(&arg, &mut args)?);
            }
            "--output" | "-o" => {
                tool_args.output = Some(next_arg_value(&arg, &mut args)?);
            }
            "--progress-every" => {
                tool_args.progress_every =
                    next_arg_value(&arg, &mut args)?.parse().map_err(|e| {
                        ErrorCode::InvalidConfig(format!("invalid --progress-every: {e}"))
                    })?;
            }
            "--plan-timeout-ms" => {
                tool_args.plan_timeout_ms =
                    next_arg_value(&arg, &mut args)?.parse().map_err(|e| {
                        ErrorCode::InvalidConfig(format!("invalid --plan-timeout-ms: {e}"))
                    })?;
            }
            "--report-plan-failures" => {
                tool_args.report_plan_failures = true;
            }
            _ if arg.starts_with("--input=") => {
                tool_args.input = Some(arg["--input=".len()..].to_string());
            }
            _ if arg.starts_with("--output=") => {
                tool_args.output = Some(arg["--output=".len()..].to_string());
            }
            _ if arg.starts_with("--progress-every=") => {
                tool_args.progress_every =
                    arg["--progress-every=".len()..].parse().map_err(|e| {
                        ErrorCode::InvalidConfig(format!("invalid --progress-every: {e}"))
                    })?;
            }
            _ if arg.starts_with("--plan-timeout-ms=") => {
                tool_args.plan_timeout_ms =
                    arg["--plan-timeout-ms=".len()..].parse().map_err(|e| {
                        ErrorCode::InvalidConfig(format!("invalid --plan-timeout-ms: {e}"))
                    })?;
            }
            _ => databend_args.push(arg),
        }
    }

    Ok((tool_args, databend_args))
}

fn next_arg_value(arg: &str, args: &mut impl Iterator<Item = String>) -> Result<String> {
    args.next()
        .ok_or_else(|| ErrorCode::InvalidConfig(format!("missing value for {arg}")))
}

fn print_help() {
    println!("coalesce-risk-analyzer-636 {}", &**DATABEND_COMMIT_VERSION);
    println!();
    println!("Typed planner analyzer for v1.2.636 coalesce(VARCHAR, integer) risks.");
    println!();
    println!("Tool options:");
    println!(
        "  -i, --input <PATH>             collect_queries.py JSONL input, or '-' / omitted for stdin"
    );
    println!("  -o, --output <PATH>            JSONL findings output, omitted for stdout");
    println!("      --progress-every <N>       stderr progress interval, default 100, 0 disables");
    println!("                                  prints the current record before planning it");
    println!("      --plan-timeout-ms <N>      per-query planner timeout, default 30000");
    println!("      --report-plan-failures     also emit PLAN_FAILED JSONL rows");
    println!();
    println!("Databend query options are passed through, for example:");
    println!(
        "  --meta-endpoints <HOST:PORT> --tenant-id <TENANT> --storage-type fs --storage-fs-data-path <PATH>"
    );
}

async fn load_inner_config(databend_args: Vec<String>) -> Result<InnerConfig> {
    let mut arg_conf = Config::try_parse_from(databend_args)
        .map_err(|e| ErrorCode::InvalidConfig(e.to_string()))?;

    if arg_conf.cmd == Some("ver".to_string()) {
        arg_conf.subcommand = Some(Commands::Ver);
    }

    if arg_conf.subcommand.is_some() {
        return Err(ErrorCode::InvalidConfig(
            "databend-query subcommands are not supported by coalesce-risk-analyzer-636",
        ));
    }

    let mut builder: serfig::Builder<Config> = serfig::Builder::default();

    let config_file = if !arg_conf.config_file.is_empty() {
        arg_conf.config_file.clone()
    } else {
        env::var("CONFIG_FILE").unwrap_or_default()
    };

    if !config_file.is_empty() {
        builder = builder.collect(from_file(Toml, &config_file));
    }

    builder = builder.collect(from_env());
    builder = builder.collect(from_self(arg_conf));

    let read_config = builder.build()?;
    let mut cfg: InnerConfig = read_config.try_into()?;
    cfg.query.node_id = GlobalUniqName::unique();
    cfg.query.node_secret = GlobalUniqName::unique();
    cfg.storage.params = cfg.storage.params.auto_detect().await?;
    cfg.meta.check_valid()?;
    Ok(cfg)
}

async fn create_session(user_name: &str) -> Result<std::sync::Arc<Session>> {
    let session_manager = SessionManager::instance();
    let session = session_manager.create_session(SessionType::Local).await?;
    let session = session_manager.register_session(session)?;
    set_session_user(&session, user_name).await?;
    Ok(session)
}

async fn set_session_user(session: &std::sync::Arc<Session>, user_name: &str) -> Result<()> {
    let mut user = UserInfo::new_no_auth(user_name, "%");
    user.grants.grant_privileges(
        &GrantObject::Global,
        UserPrivilegeSet::available_privileges_on_global(),
    );
    session.set_authed_user(user, None).await
}

fn open_input(args: &ToolArgs) -> Result<Box<dyn BufRead>> {
    match args.input.as_deref() {
        Some("-") | None => Ok(Box::new(BufReader::new(io::stdin()))),
        Some(path) => Ok(Box::new(BufReader::new(File::open(path)?))),
    }
}

fn open_output(args: &ToolArgs) -> Result<Box<dyn Write>> {
    match args.output.as_deref() {
        Some("-") | None => Ok(Box::new(BufWriter::new(io::stdout()))),
        Some(path) => Ok(Box::new(BufWriter::new(File::create(path)?))),
    }
}

fn parse_record(line_no: usize, line: &str) -> Option<QueryRecord> {
    let raw: Value = serde_json::from_str(line.trim()).ok()?;
    let sql = raw
        .get("query_text")
        .or_else(|| raw.get("query"))
        .and_then(Value::as_str)?
        .to_string();

    if sql.trim().is_empty() {
        return None;
    }

    let database = raw
        .get("current_database")
        .or_else(|| raw.get("database"))
        .and_then(Value::as_str)
        .filter(|v| !v.is_empty())
        .unwrap_or("default")
        .to_string();

    let sql_user = raw
        .get("sql_user")
        .and_then(Value::as_str)
        .filter(|v| !v.is_empty())
        .unwrap_or("root")
        .to_string();

    Some(QueryRecord {
        line_no,
        raw,
        sql,
        database,
        sql_user,
    })
}

async fn plan_record(
    session: &std::sync::Arc<Session>,
    record: &QueryRecord,
    plan_timeout_ms: u64,
) -> std::result::Result<Plan, String> {
    session.set_current_database(record.database.clone());
    let sql = record.sql.clone();
    let database = record.database.clone();
    let session = session.clone();

    let future = async move {
        let context = session.create_query_context().await?;
        context.set_current_database(database).await?;
        let mut planner = Planner::new(context);
        let (plan, _) = planner.plan_sql(&sql).await?;
        Ok::<_, ErrorCode>(plan)
    };
    let mut handle = tokio::spawn(future);

    match timeout(Duration::from_millis(plan_timeout_ms), &mut handle).await {
        Ok(Ok(Ok(plan))) => Ok(plan),
        Ok(Ok(Err(cause))) => Err(cause.to_string()),
        Ok(Err(cause)) => Err(format!("planner task failed: {cause}")),
        Err(_) => {
            handle.abort();
            Err(format!("planner timed out after {plan_timeout_ms}ms"))
        }
    }
}

fn analyze_plan(plan: &Plan, path: &str, findings: &mut Vec<Evidence>) {
    match plan {
        Plan::Query { s_expr, .. } => analyze_s_expr(s_expr, path, findings),
        Plan::Explain { plan, .. } | Plan::ExplainAnalyze { plan, .. } => {
            analyze_plan(plan, path, findings);
        }
        Plan::ReclusterTable { s_expr, .. }
        | Plan::OptimizeCompactBlock { s_expr, .. }
        | Plan::DataMutation { s_expr, .. } => analyze_s_expr(s_expr, path, findings),
        Plan::Insert(insert) => analyze_insert_source(&insert.source, path, findings),
        Plan::Replace(replace) => analyze_insert_source(&replace.source, path, findings),
        Plan::InsertMultiTable(insert) => {
            analyze_plan(
                &insert.input_source,
                &format!("{path}.input_source"),
                findings,
            );
            for (idx, when) in insert.whens.iter().enumerate() {
                let mut ancestors = Vec::new();
                visit_scalar(
                    &when.condition,
                    &format!("{path}.when[{idx}].condition"),
                    &mut ancestors,
                    findings,
                );
                for (into_idx, into) in when.intos.iter().enumerate() {
                    analyze_insert_into_exprs(
                        into.source_scalar_exprs.as_deref(),
                        &format!("{path}.when[{idx}].into[{into_idx}]"),
                        findings,
                    );
                }
            }
            if let Some(else_plan) = &insert.opt_else {
                for (idx, into) in else_plan.intos.iter().enumerate() {
                    analyze_insert_into_exprs(
                        into.source_scalar_exprs.as_deref(),
                        &format!("{path}.else.into[{idx}]"),
                        findings,
                    );
                }
            }
        }
        _ => {}
    }
}

fn analyze_insert_source(source: &InsertInputSource, path: &str, findings: &mut Vec<Evidence>) {
    match source {
        InsertInputSource::SelectPlan(plan) | InsertInputSource::Stage(plan) => {
            analyze_plan(plan, path, findings);
        }
        InsertInputSource::Values(_) => {}
    }
}

fn analyze_insert_into_exprs(
    exprs: Option<&[ScalarExpr]>,
    path: &str,
    findings: &mut Vec<Evidence>,
) {
    if let Some(exprs) = exprs {
        for (idx, expr) in exprs.iter().enumerate() {
            let mut ancestors = Vec::new();
            visit_scalar(
                expr,
                &format!("{path}.source_scalar_expr[{idx}]"),
                &mut ancestors,
                findings,
            );
        }
    }
}

fn analyze_s_expr(s_expr: &SExpr, path: &str, findings: &mut Vec<Evidence>) {
    visit_rel_operator(s_expr.plan(), path, findings);

    for (idx, child) in s_expr.children().enumerate() {
        analyze_s_expr(child, &format!("{path}.child[{idx}]"), findings);
    }
}

fn visit_rel_operator(op: &RelOperator, path: &str, findings: &mut Vec<Evidence>) {
    match op {
        RelOperator::Scan(scan) => {
            if let Some(predicates) = &scan.push_down_predicates {
                visit_scalar_list(predicates, &format!("{path}.scan.push_down"), findings);
            }
            if let Some(prewhere) = &scan.prewhere {
                visit_scalar_list(
                    &prewhere.predicates,
                    &format!("{path}.scan.prewhere"),
                    findings,
                );
            }
            if let Some(agg_index) = &scan.agg_index {
                for (idx, item) in agg_index.selection.iter().enumerate() {
                    visit_scalar_item(
                        &item.scalar,
                        &format!("{path}.scan.agg_index.selection[{idx}]"),
                        findings,
                    );
                }
                visit_scalar_list(
                    &agg_index.predicates,
                    &format!("{path}.scan.agg_index.predicates"),
                    findings,
                );
            }
        }
        RelOperator::Join(join) => {
            for (idx, condition) in join.equi_conditions.iter().enumerate() {
                visit_scalar_item(
                    &condition.left,
                    &format!("{path}.join.equi[{idx}].left"),
                    findings,
                );
                visit_scalar_item(
                    &condition.right,
                    &format!("{path}.join.equi[{idx}].right"),
                    findings,
                );
            }
            visit_scalar_list(
                &join.non_equi_conditions,
                &format!("{path}.join.non_equi"),
                findings,
            );
        }
        RelOperator::EvalScalar(eval) => {
            for (idx, item) in eval.items.iter().enumerate() {
                visit_scalar_item(
                    &item.scalar,
                    &format!("{path}.eval_scalar.item[{idx}]"),
                    findings,
                );
            }
        }
        RelOperator::Filter(filter) => {
            visit_scalar_list(&filter.predicates, &format!("{path}.filter"), findings);
        }
        RelOperator::Aggregate(aggregate) => {
            for (idx, item) in aggregate.group_items.iter().enumerate() {
                visit_scalar_item(
                    &item.scalar,
                    &format!("{path}.aggregate.group[{idx}]"),
                    findings,
                );
            }
            for (idx, item) in aggregate.aggregate_functions.iter().enumerate() {
                visit_scalar_item(
                    &item.scalar,
                    &format!("{path}.aggregate.func[{idx}]"),
                    findings,
                );
            }
        }
        RelOperator::Sort(sort) => {
            for (idx, item) in sort.window_partition.iter().enumerate() {
                visit_scalar_item(
                    &item.scalar,
                    &format!("{path}.sort.window_partition[{idx}]"),
                    findings,
                );
            }
        }
        RelOperator::Exchange(Exchange::Hash(keys)) => {
            visit_scalar_list(keys, &format!("{path}.exchange.hash"), findings);
        }
        RelOperator::Window(window) => {
            for (idx, item) in window.arguments.iter().enumerate() {
                visit_scalar_item(
                    &item.scalar,
                    &format!("{path}.window.argument[{idx}]"),
                    findings,
                );
            }
            for (idx, item) in window.partition_by.iter().enumerate() {
                visit_scalar_item(
                    &item.scalar,
                    &format!("{path}.window.partition_by[{idx}]"),
                    findings,
                );
            }
            for (idx, item) in window.order_by.iter().enumerate() {
                visit_scalar_item(
                    &item.order_by_item.scalar,
                    &format!("{path}.window.order_by[{idx}]"),
                    findings,
                );
            }
            visit_window_func_type(
                &window.function,
                &format!("{path}.window.function"),
                findings,
            );
        }
        RelOperator::ProjectSet(project_set) => {
            for (idx, item) in project_set.srfs.iter().enumerate() {
                visit_scalar_item(
                    &item.scalar,
                    &format!("{path}.project_set.srf[{idx}]"),
                    findings,
                );
            }
        }
        RelOperator::ExpressionScan(expression_scan) => {
            for (row_idx, row) in expression_scan.values.iter().enumerate() {
                for (col_idx, expr) in row.iter().enumerate() {
                    visit_scalar_item(
                        expr,
                        &format!("{path}.expression_scan.values[{row_idx}][{col_idx}]"),
                        findings,
                    );
                }
            }
        }
        RelOperator::Udf(udf) => {
            for (idx, item) in udf.items.iter().enumerate() {
                visit_scalar_item(&item.scalar, &format!("{path}.udf.item[{idx}]"), findings);
            }
        }
        RelOperator::AsyncFunction(async_func) => {
            for (idx, item) in async_func.items.iter().enumerate() {
                visit_scalar_item(
                    &item.scalar,
                    &format!("{path}.async_function.item[{idx}]"),
                    findings,
                );
            }
        }
        RelOperator::Mutation(mutation) => {
            for (idx, matched) in mutation.matched_evaluators.iter().enumerate() {
                if let Some(condition) = &matched.condition {
                    visit_scalar_item(
                        condition,
                        &format!("{path}.mutation.matched[{idx}].condition"),
                        findings,
                    );
                }
                if let Some(update) = &matched.update {
                    for (field_idx, expr) in update {
                        visit_scalar_item(
                            expr,
                            &format!("{path}.mutation.matched[{idx}].update[{field_idx}]"),
                            findings,
                        );
                    }
                }
            }
            for (idx, unmatched) in mutation.unmatched_evaluators.iter().enumerate() {
                if let Some(condition) = &unmatched.condition {
                    visit_scalar_item(
                        condition,
                        &format!("{path}.mutation.unmatched[{idx}].condition"),
                        findings,
                    );
                }
                for (value_idx, expr) in unmatched.values.iter().enumerate() {
                    visit_scalar_item(
                        expr,
                        &format!("{path}.mutation.unmatched[{idx}].value[{value_idx}]"),
                        findings,
                    );
                }
            }
            if let Some(filter) = &mutation.direct_filter {
                visit_scalar_item(filter, &format!("{path}.mutation.direct_filter"), findings);
            }
        }
        RelOperator::MutationSource(source) => {
            if let Some(filter) = &source.filter {
                visit_scalar_item(filter, &format!("{path}.mutation_source.filter"), findings);
            }
        }
        _ => {}
    }
}

fn visit_scalar_list(exprs: &[ScalarExpr], path: &str, findings: &mut Vec<Evidence>) {
    for (idx, expr) in exprs.iter().enumerate() {
        visit_scalar_item(expr, &format!("{path}[{idx}]"), findings);
    }
}

fn visit_scalar_item(expr: &ScalarExpr, path: &str, findings: &mut Vec<Evidence>) {
    let mut ancestors = Vec::new();
    visit_scalar(expr, path, &mut ancestors, findings);
}

fn visit_window_func_type(func: &WindowFuncType, path: &str, findings: &mut Vec<Evidence>) {
    match func {
        WindowFuncType::Aggregate(aggregate) => {
            for (idx, arg) in aggregate.args.iter().enumerate() {
                visit_scalar_item(arg, &format!("{path}.aggregate.arg[{idx}]"), findings);
            }
        }
        WindowFuncType::LagLead(func) => {
            visit_scalar_item(&func.arg, &format!("{path}.lag_lead.arg"), findings);
            if let Some(default) = &func.default {
                visit_scalar_item(default, &format!("{path}.lag_lead.default"), findings);
            }
        }
        WindowFuncType::NthValue(func) => {
            visit_scalar_item(&func.arg, &format!("{path}.nth_value.arg"), findings);
        }
        WindowFuncType::RowNumber
        | WindowFuncType::Rank
        | WindowFuncType::DenseRank
        | WindowFuncType::PercentRank
        | WindowFuncType::Ntile(_)
        | WindowFuncType::CumeDist => {}
    }
}

fn visit_scalar<'a>(
    expr: &'a ScalarExpr,
    path: &str,
    ancestors: &mut Vec<&'a ScalarExpr>,
    findings: &mut Vec<Evidence>,
) {
    if let ScalarExpr::CastExpr(cast) = expr {
        if let Some(evidence) = inspect_cast(
            expr,
            cast.argument.as_ref(),
            cast.target_type.as_ref(),
            ancestors,
            path,
        ) {
            findings.push(evidence);
        }
    }

    ancestors.push(expr);
    match expr {
        ScalarExpr::BoundColumnRef(_) | ScalarExpr::ConstantExpr(_) => {}
        ScalarExpr::WindowFunction(window) => {
            for (idx, part) in window.partition_by.iter().enumerate() {
                visit_scalar(
                    part,
                    &format!("{path}.window.partition_by[{idx}]"),
                    ancestors,
                    findings,
                );
            }
            for (idx, order) in window.order_by.iter().enumerate() {
                visit_scalar(
                    &order.expr,
                    &format!("{path}.window.order_by[{idx}]"),
                    ancestors,
                    findings,
                );
            }
            visit_window_func_type_with_ancestors(
                &window.func,
                &format!("{path}.window.func"),
                ancestors,
                findings,
            );
        }
        ScalarExpr::AggregateFunction(aggregate) => {
            for (idx, arg) in aggregate.args.iter().enumerate() {
                visit_scalar(
                    arg,
                    &format!("{path}.aggregate.arg[{idx}]"),
                    ancestors,
                    findings,
                );
            }
        }
        ScalarExpr::LambdaFunction(lambda) => {
            for (idx, arg) in lambda.args.iter().enumerate() {
                visit_scalar(
                    arg,
                    &format!("{path}.lambda.arg[{idx}]"),
                    ancestors,
                    findings,
                );
            }
        }
        ScalarExpr::FunctionCall(func) => {
            for (idx, arg) in func.arguments.iter().enumerate() {
                visit_scalar(
                    arg,
                    &format!("{path}.{}[{idx}]", func.func_name),
                    ancestors,
                    findings,
                );
            }
        }
        ScalarExpr::CastExpr(cast) => {
            visit_scalar(
                &cast.argument,
                &format!("{path}.cast_arg"),
                ancestors,
                findings,
            );
        }
        ScalarExpr::SubqueryExpr(subquery) => {
            if let Some(child_expr) = &subquery.child_expr {
                visit_scalar(
                    child_expr,
                    &format!("{path}.subquery.child_expr"),
                    ancestors,
                    findings,
                );
            }
            analyze_s_expr(
                &subquery.subquery,
                &format!("{path}.subquery.plan"),
                findings,
            );
        }
        ScalarExpr::UDFCall(udf) => {
            for (idx, arg) in udf.arguments.iter().enumerate() {
                visit_scalar(arg, &format!("{path}.udf.arg[{idx}]"), ancestors, findings);
            }
        }
        ScalarExpr::UDFLambdaCall(udf) => {
            visit_scalar(
                &udf.scalar,
                &format!("{path}.udf_lambda.scalar"),
                ancestors,
                findings,
            );
        }
        ScalarExpr::AsyncFunctionCall(async_func) => {
            for (idx, arg) in async_func.arguments.iter().enumerate() {
                visit_scalar(
                    arg,
                    &format!("{path}.async.arg[{idx}]"),
                    ancestors,
                    findings,
                );
            }
        }
    }
    ancestors.pop();
}

fn visit_window_func_type_with_ancestors<'a>(
    func: &'a WindowFuncType,
    path: &str,
    ancestors: &mut Vec<&'a ScalarExpr>,
    findings: &mut Vec<Evidence>,
) {
    match func {
        WindowFuncType::Aggregate(aggregate) => {
            for (idx, arg) in aggregate.args.iter().enumerate() {
                visit_scalar(
                    arg,
                    &format!("{path}.aggregate.arg[{idx}]"),
                    ancestors,
                    findings,
                );
            }
        }
        WindowFuncType::LagLead(func) => {
            visit_scalar(
                &func.arg,
                &format!("{path}.lag_lead.arg"),
                ancestors,
                findings,
            );
            if let Some(default) = &func.default {
                visit_scalar(
                    default,
                    &format!("{path}.lag_lead.default"),
                    ancestors,
                    findings,
                );
            }
        }
        WindowFuncType::NthValue(func) => {
            visit_scalar(
                &func.arg,
                &format!("{path}.nth_value.arg"),
                ancestors,
                findings,
            );
        }
        WindowFuncType::RowNumber
        | WindowFuncType::Rank
        | WindowFuncType::DenseRank
        | WindowFuncType::PercentRank
        | WindowFuncType::Ntile(_)
        | WindowFuncType::CumeDist => {}
    }
}

fn inspect_cast(
    cast_expr: &ScalarExpr,
    source_expr: &ScalarExpr,
    target_type: &DataType,
    ancestors: &[&ScalarExpr],
    path: &str,
) -> Option<Evidence> {
    let source_type = source_expr.data_type().ok()?;
    if !is_string_type(&source_type) || !is_integer_type(target_type) {
        return None;
    }

    let (inner_source_expr, inner_source_type) = assume_not_null_arg(source_expr)
        .and_then(|inner| {
            inner
                .data_type()
                .ok()
                .map(|ty| (format_scalar_limited(inner), ty.to_string()))
        })
        .unwrap_or_else(|| (String::new(), String::new()));

    let nearest_if = ancestors.iter().rev().copied().find(is_if_function);
    let coalesce_like_rewrite = assume_not_null_arg(source_expr).is_some()
        && nearest_if.map(has_integer_constant_arg).unwrap_or(false);

    let decimal_context_exprs = decimal_context_exprs(ancestors);
    if !coalesce_like_rewrite && decimal_context_exprs.is_empty() {
        return None;
    }

    Some(Evidence {
        location: path.to_string(),
        cast_expr: format_scalar_limited(cast_expr),
        cast_source_expr: format_scalar_limited(source_expr),
        cast_source_type: source_type.to_string(),
        cast_target_type: target_type.to_string(),
        inner_source_expr,
        inner_source_type,
        nearest_if_expr: nearest_if.map(format_scalar_limited).unwrap_or_default(),
        decimal_context_exprs,
        coalesce_like_rewrite,
    })
}

fn assume_not_null_arg(expr: &ScalarExpr) -> Option<&ScalarExpr> {
    match expr {
        ScalarExpr::FunctionCall(func)
            if func.func_name.eq_ignore_ascii_case("assume_not_null")
                && func.arguments.len() == 1 =>
        {
            Some(&func.arguments[0])
        }
        _ => None,
    }
}

fn is_if_function(expr: &&ScalarExpr) -> bool {
    matches!(
        expr,
        ScalarExpr::FunctionCall(func) if func.func_name.eq_ignore_ascii_case("if")
    )
}

fn has_integer_constant_arg(expr: &ScalarExpr) -> bool {
    match expr {
        ScalarExpr::FunctionCall(func) => func.arguments.iter().any(is_integer_constant),
        _ => false,
    }
}

fn is_integer_constant(expr: &ScalarExpr) -> bool {
    matches!(
        expr,
        ScalarExpr::ConstantExpr(constant)
            if matches!(
                constant.value,
                Scalar::Number(NumberScalar::UInt8(_))
                    | Scalar::Number(NumberScalar::UInt16(_))
                    | Scalar::Number(NumberScalar::UInt32(_))
                    | Scalar::Number(NumberScalar::UInt64(_))
                    | Scalar::Number(NumberScalar::Int8(_))
                    | Scalar::Number(NumberScalar::Int16(_))
                    | Scalar::Number(NumberScalar::Int32(_))
                    | Scalar::Number(NumberScalar::Int64(_))
            )
    )
}

fn decimal_context_exprs(ancestors: &[&ScalarExpr]) -> Vec<String> {
    let mut contexts = Vec::new();
    for ancestor in ancestors.iter().rev() {
        if expr_type_is_decimal(ancestor) || expr_has_decimal_argument(ancestor) {
            contexts.push(format_scalar_limited(ancestor));
            if contexts.len() >= 3 {
                break;
            }
        }
    }
    contexts
}

fn expr_type_is_decimal(expr: &ScalarExpr) -> bool {
    expr.data_type()
        .map(|ty| is_decimal_type(&ty))
        .unwrap_or(false)
}

fn expr_has_decimal_argument(expr: &ScalarExpr) -> bool {
    match expr {
        ScalarExpr::FunctionCall(func) => func.arguments.iter().any(|arg| {
            arg.data_type()
                .map(|ty| is_decimal_type(&ty))
                .unwrap_or(false)
        }),
        ScalarExpr::CastExpr(cast) => is_decimal_type(&cast.target_type),
        ScalarExpr::AggregateFunction(aggregate) => aggregate.args.iter().any(|arg| {
            arg.data_type()
                .map(|ty| is_decimal_type(&ty))
                .unwrap_or(false)
        }),
        _ => false,
    }
}

fn is_string_type(ty: &DataType) -> bool {
    matches!(ty.remove_nullable(), DataType::String)
}

fn is_decimal_type(ty: &DataType) -> bool {
    matches!(ty.remove_nullable(), DataType::Decimal(_))
}

fn is_integer_type(ty: &DataType) -> bool {
    matches!(
        ty.remove_nullable(),
        DataType::Number(
            NumberDataType::UInt8
                | NumberDataType::UInt16
                | NumberDataType::UInt32
                | NumberDataType::UInt64
                | NumberDataType::Int8
                | NumberDataType::Int16
                | NumberDataType::Int32
                | NumberDataType::Int64
        )
    )
}

fn finding_value(record: &QueryRecord, risk: &str, evidence: Evidence) -> Result<Value> {
    let mut output = base_output(record, risk);
    output.insert("evidence".to_string(), serde_json::to_value(evidence)?);
    Ok(Value::Object(output))
}

fn plan_failure_value(record: &QueryRecord, cause: &str) -> Value {
    let mut output = base_output(record, "PLAN_FAILED");
    output.insert("error".to_string(), Value::String(cause.to_string()));
    Value::Object(output)
}

fn base_output(record: &QueryRecord, risk: &str) -> Map<String, Value> {
    let mut output = Map::new();
    output.insert("risk".to_string(), Value::String(risk.to_string()));
    output.insert("line_no".to_string(), json!(record.line_no));
    for field in QUERY_INFO_FIELDS {
        output.insert(
            (*field).to_string(),
            record.raw.get(*field).cloned().unwrap_or(Value::Null),
        );
    }
    output.insert(
        "query_id".to_string(),
        record.raw.get("query_id").cloned().unwrap_or(Value::Null),
    );
    output.insert("query_text".to_string(), Value::String(record.sql.clone()));
    output
}

fn format_scalar_limited(expr: &ScalarExpr) -> String {
    limit_text(format_scalar(expr), 1_000)
}

fn limit_text(mut text: String, limit: usize) -> String {
    text = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if text.len() <= limit {
        text
    } else {
        let mut truncated = text;
        truncated.truncate(limit);
        truncated.push_str("...");
        truncated
    }
}

fn error_category(cause: &str) -> String {
    let text = cause.split_whitespace().collect::<Vec<_>>().join(" ");
    if text.is_empty() {
        "(empty)".to_string()
    } else {
        limit_text(text, 180)
    }
}

fn print_summary(summary: &Summary) {
    eprintln!(
        "[summary] records={}, skipped_bad_lines={}, planned={}, findings={}, high={}, plan_failed={}",
        summary.records,
        summary.skipped_bad_lines,
        summary.planned,
        summary.findings,
        summary.high,
        summary.plan_failed
    );
    if !summary.plan_failed_summary.is_empty() {
        eprintln!("[summary] plan failed categories:");
        for (category, count) in &summary.plan_failed_summary {
            eprintln!("  {count}: {category}");
        }
    }
}
