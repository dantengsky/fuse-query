# Private Task 系列修复 + Proto 兼容性 Cherry-Pick 说明

## 背景

分支 `release/v1.2.911-rc7` 从 main 分叉点为 2026-05-15（`5685eda38b`）。该分支将与 main 分支的集群**共享同一个 meta server（同 tenant）**，因此需要确保两个版本的 binary 对 meta 中存储的 protobuf 数据互相兼容。

---

## 一、Cherry-Pick 的 PR 列表

| 序号 | PR | 标题 | 合入时间 | 状态 |
|------|-----|------|---------|------|
| 1 | #19918 | fix(query): improve private task compatibility | 2026-05-27 | 无冲突 |
| 2 | #19943 | fix(query): align private task history with cloud | 2026-06-03 | 无冲突 |
| 3 | #19965 | fix(query): unblock failed private task runs | 2026-06-06 | 无冲突 |
| 4 | #19976 | fix(query): preserve task options on alter set | 2026-06-10 | 无冲突 |
| 5 | #20120 | fix(query): trigger all ready task successors | 2026-07-08 | 无冲突 |
| 6 | #20109 | fix(query): cancel open private task runs on drop | 2026-07-08 | 冲突已解决 |
| 7 | #20141 | fix(query): preserve scalar bloom hashes in replace into | 2026-07-14 | 无冲突 |

PR #1–#6 作者均为 @KKould，#7 为 @dantengsky。

### #20109 冲突解决

冲突原因：proto-conv 版本号偏移（main 上 v177/v178 被无关 PR 占用）。

处理：将 #20109 新增的 `DeleteTask.task_id` 版本号从 v179 → **v177**（rc7 的下一个可用版本），调整测试文件名、内嵌 varint 字节和断言。保留分支的 fallible `to_pb()?` 签名。

---

## 二、Proto 兼容性补丁（仅 proto schema + 反序列化，不引入完整 feature）

### 问题

main 分支新增了两类 proto 定义，rc7 原本不认识：

| main 版本 | 内容 | 影响 |
|-----------|------|------|
| v177 | `file_format.proto` Arrow/ArrowStream 文件格式 | rc7 读不了 main 创建的 Arrow stage |
| v178 | `config.proto` StorageConfig: Azblob/Ftp/Http/Ipfs/Memory | rc7 读不了 main 创建的这些存储配置 |

prost 遇到未知 oneof field tag 时解码为 `None` → `from_pb` 返回 `Err(Incompatible)` → **SHOW STAGES 等操作失败**。

### 解决方案

在 rc7 上添加这些 proto 定义和 `from_pb`/`to_pb` 实现（**不引入完整 Arrow feature**），使 rc7 能正确反序列化这些元数据。

- Arrow stage 的 COPY 操作会在 `stage_table.rs` 层返回 `ErrorCode::Unimplemented("Arrow stage table read is not supported on this version")`
- SHOW STAGES / DESC STAGE 正常工作
- rc7 上 VER 升至 **178**

### 修改的文件

| 文件 | 变更 |
|------|------|
| `src/meta/protos/proto/file_format.proto` | 新增 Arrow=9, ArrowStream=10 enum + oneof fields + ArrowFileFormatParams message |
| `src/meta/protos/proto/config.proto` | 新增 Azblob/Ftp/Http/Ipfs/Memory oneof fields + message 定义 |
| `src/meta/app/src/principal/file_format.rs` | ArrowFileFormatParams struct + Arrow/ArrowStream enum variants |
| `src/meta/app/src/principal/user_stage.rs` | StageFileFormatType::Arrow/ArrowStream |
| `src/meta/proto-conv/src/util.rs` | v178 changelog entry |
| `src/meta/proto-conv/src/impls/file_format.rs` | from_pb/to_pb arms + ArrowFileFormatParams FromToProto impl |
| `src/meta/proto-conv/src/impls/config.rs` | from_pb/to_pb arms + Azblob/Ftp/Http/Ipfs FromToProto impls |
| `src/query/storages/stage/src/stage_table.rs` | Arrow/ArrowStream → Unimplemented error |
| `src/meta/proto-conv/tests/it/v178_arrow_and_storage_compat.rs` | 8 个 round-trip 兼容性测试 |

---

## 三、版本号兼容性分析

rc7 分支版本号链：v176 (script_sql) → v177 (DeleteTask.task_id) → v178 (Arrow + StorageConfig stubs)

main 分支版本号链：v176 (script_sql) → v177 (Arrow formats) → v178 (StorageConfig) → v179 (DeleteTask.task_id)

**版本号不同不影响运行时兼容性**：
1. 反序列化靠 protobuf field tag（字段编号），不靠 VER 数字做语义分支
2. proto 定义两边一致：`task.proto` DeleteTask field 1/2/3、`file_format.proto` Arrow oneof field 10/11、`config.proto` Azblob/Ftp/Http/Ipfs/Memory oneof field 10–14，编号完全匹配
3. VER/MIN_READER_VER 门槛检查双向通过：
   - rc7(VER=178) 读 main 数据(ver≤179, min_reader_ver=24)：`178 >= 24` ✓
   - main(VER=179) 读 rc7 数据(ver≤178, min_reader_ver=24)：`179 >= 24` ✓
4. main 的 v176–v179 引入的所有 proto schema 变更，rc7 已全部覆盖，不存在"rc7声称支持但实际缺失定义"的情况

---

## 四、验证结果

### 编译和单元测试

- `cargo check -p databend-query`：通过（无 error）
- `cargo test -p databend-common-proto-conv`：232 tests passed
- v178 兼容性测试：8 tests passed（Arrow round-trip + StorageConfig round-trip）

### 端到端双实例兼容性测试

测试环境：
- Meta：main release binary (v1.2.918-nightly, VER≥178)
- Query A：main release binary (Jun 25, VER=178, 含 Arrow/StorageConfig/DeleteTask.task_id)
- Query B：rc7 debug binary (Jul 14, VER=178, 含 proto 兼容性补丁)
- 共享同一 meta-server，同一 tenant (`compat_test`)

| # | 测试场景 | 结果 |
|---|----------|------|
| 1 | main 创建 Arrow stage (`CREATE STAGE ... FILE_FORMAT = (TYPE = ARROW)`) | ✅ 成功 |
| 2 | main 创建 internal stage | ✅ 成功 |
| 3 | main `SELECT name, stage_type FROM system.stages` | ✅ 列出 arrow_test_stage, main_internal_stage |
| 4 | **rc7 读取 main 创建的 Arrow stage** (`system.stages`) | ✅ 正确显示，含 `"type":"Arrow"` |
| 5 | **rc7 DESC STAGE arrow_test_stage** | ✅ 返回完整元数据：`{"missing_field_as":"Error","type":"Arrow"}` |
| 6 | **rc7 查询 Arrow stage 数据** (`SELECT * FROM @arrow_test_stage`) | ✅ 返回清晰错误："Unsupported file format in query stage"（非 panic） |
| 7 | rc7 创建自己的 stage | ✅ 成功 |
| 8 | **main 读取 rc7 创建的 stage** | ✅ `rc7_stage` 可见 |
| 9 | 日志中 panic / Incompatible / SIGSEGV | ✅ 零（三个进程日志均无） |

### Task 兼容性说明

Task 相关的 SQL（CREATE TASK / SHOW TASKS）需要 `cloud_control_grpc_server_address` 配置，本地双实例环境无法直接通过 SQL 层测试。但：
- proto-conv 单元测试 `test_decode_v177_task_delete_task_id` 已验证 TaskMessage（含 `DeleteTask.task_id`）的 round-trip 兼容性
- task.proto 定义（field 编号）在 rc7 和 main 上完全一致
- 建议在带有 cloud control 的测试环境中做进一步端到端验证

---

## 五、集成测试计划（两集群共享 meta，同 tenant）

### Stage / StorageConfig 兼容性（已验证 ✅）

1. 部署共享 meta-server
2. 启动 main 集群 → 创建 Arrow stage
3. 启动 rc7 集群连接同一 meta
4. 在 rc7 上验证：
   - `system.stages` 成功列出 Arrow stage ✅
   - `DESC STAGE arrow_test_stage` 正常显示 ✅
   - 查询 Arrow stage 返回清晰的 "Unsupported" 错误 ✅
   - 创建/使用自己的 stage 正常 ✅
5. 验证 main 不受 rc7 影响 ✅

### Private Task 跨版本兼容性（需 cloud control 环境验证）

6. 在 main 集群创建 private task（含 schedule、after 依赖链）
7. 在 rc7 集群验证：
   - `SHOW TASKS;` 能看到 main 创建的 task
   - 执行/调度 task 正常（TaskMessage 互相可读）
   - DROP TASK 正常（DeleteTask 含 task_id 字段，两边都认识）
8. 反向：在 rc7 创建 task，main 集群能正常读取和操作
9. 跑 `tests/task/test-private-task.sh` 和 `tests/task/test-private-task-warehouse.sh`

---

## 六、需要协调的事项

1. **功能测试**：建议 @KKould 在 rc7 环境跑 `tests/task/test-private-task.sh` 和 `tests/task/test-private-task-warehouse.sh`
2. **infallible proto refactor**：`#20051` 未 pick，依赖它的 PR 需手动适配 fallible 形式
