# Outer Join 空基数规划问题备忘录

## 范围与脱敏说明

本文记录一个位于 `UNION ALL` 与顶层 `LIMIT` 下方的 Outer Join 优化器回归。文中不包含原始
SQL、Schema 名称、业务字段、数据规模和性能测量数据。

## 问题现象

两个版本生成了语义等价、执行成本却不同的 Hash Join：

- 快计划把 `LEFT OUTER JOIN` 交换成 `RIGHT OUTER JOIN`，将能够低成本确定为空的输入放在
  Hash Build 侧。
- 慢计划保留 `LEFT OUTER JOIN`，将昂贵输入放在 Hash Build 侧；执行器完成 Build 后，才发现
  Preserve/Probe 输入为空。

Hash Build 是阻塞阶段，因此外层 `LIMIT` 无法提前终止这部分工作。

## 证据与根因

两个版本的 Join 交换规则本身没有实质差异。规则按左右子树的输出基数决定 Build 方向；回归发生
在两侧基数同时坍缩为零时。

新版选择率估算器把表统计中的 min/max 作为常量折叠输入域。范围谓词落在统计范围之外时，整个
Filter 会提前得到零行估算；另一侧即使还有更高选择性的谓词，也会被同样的零覆盖。`CommuteJoin`
因而看到无法区分的 `0 == 0` 平局，并保留了不利的 Build 方向。

现场复验还确认，物理计划构建阶段不能补救这个问题。`EXPLAIN ANALYZE` 中最终的零分区是执行
worker 完成裁剪后回传的运行时统计；物理 Join 创建时尚未获得该结果。列统计是估算依据而不是数据
约束，也可能落后于新追加数据，因此不能据此证明扫描为空。

规划修复部署后，Join 已按预期交换为 `RIGHT OUTER JOIN`，空输入位于 Hash Build 侧，但新 Hash
Join 执行器仍对每个 Probe block 计算 Join Key 和 hash。旧执行器在空 Build 时已跳过这些工作，
因此剩余回归位于执行阶段，而不是基数估算或 Join 方向。

诊断还发现，缺少频率统计时，严重倾斜的等值谓词可能被当作近似唯一值。这会进一步恶化基数，
但不能通过特殊处理某个业务值或假设统一选择率来安全修复。

## 修复设计

修复同时保留“确定为空”和“统计估算”的边界：

1. 常量折叠只接收类型域和精确全 NULL 事实，不再把表 min/max 当作数据约束。
2. `StatEvaluator` 仍使用 min/max、NDV 和 Histogram 估算非零选择率；如果仅凭这些统计得到零，
   则回退到未知谓词的默认选择率。
3. 联合谓词继续保留更小的已知非零选择率。这样高选择性输入仍小于只有陈旧范围谓词的输入，
   `CommuteJoin` 会将前者放到 Hash Build 侧。
4. `precise_cardinality = Some(0)` 只保留给精确空输入、常量 False、精确全 NULL 和表达式自身可
   证明的矛盾；Outer Join 仍按 Preserve 侧语义传播精确零。
5. 两侧确实同为零时，`CommuteJoin` 继续优先把唯一的精确空输入放在 Build 侧；两个非精确零或
   两个精确零保持原有 canonicalization。
6. `RIGHT OUTER JOIN` 的 Build 是 Preserve 侧；Build 行数为零时结果必为空。新 Hash Join 在
   `probe_block` 入口直接返回空流，避免 Probe Key 求值、projection 和 hash probe。
7. 所有 Build worker 完成 `final_build` 并通过同步屏障后，执行器检查空 Build 快速返回条件；满足
   条件时关闭 Probe 输入和 Join 输出，使上游扫描与表达式 pipeline 一并停止。Grace spill 模式不
   使用这条判定，避免将已经写入 spill 的 Build 数据误判为空。

## 未采用的方案

- 把 `<` 改为 `<=`：会翻转所有零基数平局，可能让镜像场景从 Build 空小侧退化为 Build 大侧。
- 比较底表总行数：无法反映过滤和裁剪后的成本，可能选择相反方向。
- 在物理计划阶段按空分区折叠 Outer Join：最终分区裁剪结果在执行 worker 上产生，此时不可用。
- 特殊处理哨兵值：依赖业务数据，无法泛化。
- 要求立即分析整张大表：操作成本高，而且不能保证缺失或陈旧统计下的健壮性。

## 测试覆盖

- 确定性 False 谓词会标记精确空集。
- 数值估算零不会被提升为精确空集。
- 陈旧范围统计推导出的零会回退到未知选择率，并保留列分布。
- 陈旧范围谓词与高选择性等值谓词合并时，后者的相对选择率不会丢失。
- 脱敏 SQL replay 覆盖 `LEFT OUTER JOIN` 交换为 `RIGHT OUTER JOIN`，更小输入位于 Build 侧。
- Outer Join 只从 Preserve 侧传播精确空集。
- 零基数平局时，精确空侧被放到 Build。
- 精确空侧本来就在 Build 时不交换。
- 原有零值 canonicalization 保持不变。
- 新 Hash Join 完成空 Build 后不会求值 `RIGHT OUTER JOIN` 的 Probe Key；执行器定向测试用一个
  必然报错的 Probe Key 验证快速路径确实位于表达式求值之前。
- 新 Hash Join 在 BuildFinal 同步后关闭空 Build 对应的 Probe pipeline；执行器定向测试把必然
  报错的表达式放在 Join 上游，验证该 pipeline 不会被拉取。
- SQL 回归覆盖非空 Probe 与空 Preserve/Build 输入的 `RIGHT OUTER JOIN` 空结果。
- SQL 回归同时覆盖空 Build 时不求值 Probe pipeline 中的 Join Key。

验证结果：

- `databend-common-sql` 单元测试全部通过。
- SQL crate 轻量 planner/integration 测试全部通过。
- Query service 的脱敏 optimizer replay 和 Physical Hash Join 单元测试通过。
- SQL crate `clippy -D warnings` 通过。
- 本次修改涉及的 Rust 文件通过格式和 diff 检查；生成的 golden 文件保留测试框架既有的结尾空行。

## 后续方向

- 为推导 NDV 保留置信度和来源。
- 为倾斜列维护 Top-N/频率统计。
- 将裁剪后的扫描工作量纳入 Join Cost，而非只依赖输出基数。
- 研究 Preserve 输入可能低成本为空时的自适应 Outer Join 执行。
