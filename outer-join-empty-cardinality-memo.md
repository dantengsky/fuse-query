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

新版选择率估算器在内部已经区分：

- 由常量、布尔规则或列域矛盾确定的空集；
- 普通数值估算得到的零。

但生成算子统计信息时，这个区别丢失了，两者都只剩 `cardinality = 0.0`。`CommuteJoin` 因而看到
无法区分的平局，并保留了不利的 Build 方向。

现场复验进一步确认，逻辑优化阶段仍不足以覆盖真实场景：时间范围可能依据陈旧列统计被估算为
零，而存储层直到构建物理扫描时才能根据实际分区裁剪确定输入为空。列统计是估算依据而不是数据
约束，不能把由列域推导出的零当作确定空集。

诊断还发现，缺少频率统计时，严重倾斜的等值谓词可能被当作近似唯一值。这会进一步恶化基数，
但不能通过特殊处理某个业务值或假设统一选择率来安全修复。

## 修复设计

使用 `precise_cardinality = Some(0)` 保留“确定为空”的信息：

1. `SelectivityEstimator` 暴露零是否来自确定性规则，例如精确空输入、常量 False 或列域矛盾。
2. `Filter` 和 `Scan` 将确定空集保存为精确零；普通估算零继续保持非精确。
3. `Join` 按 Join 语义传播精确零。
4. 普通 `LEFT/RIGHT OUTER JOIN` 两侧同为零、且仅一侧为精确零时，`CommuteJoin` 将精确空侧放到
   Build 侧。
5. 物理扫描完成分区裁剪后，如果 Outer Join 的 Preserve 侧能够沿物理计划确定为空，则直接将
   Join 折叠为空结果，避免先构建另一侧的 Hash Table。

其中，列统计推导出的空域仍可用于估算，但不再设置 `precise_cardinality = Some(0)`。

非零估算、两个非精确零和两个精确零仍保持原行为。

## 未采用的方案

- 把 `<` 改为 `<=`：会翻转所有零基数平局，可能让镜像场景从 Build 空小侧退化为 Build 大侧。
- 比较底表总行数：无法反映过滤和裁剪后的成本，可能选择相反方向。
- 特殊处理哨兵值：依赖业务数据，无法泛化。
- 要求立即分析整张大表：操作成本高，而且不能保证缺失或陈旧统计下的健壮性。

## 测试覆盖

- 确定性 False 谓词会标记精确空集。
- 数值估算零不会被提升为精确空集。
- Outer Join 只从 Preserve 侧传播精确空集。
- 零基数平局时，精确空侧被放到 Build。
- 精确空侧本来就在 Build 时不交换。
- 原有零值 canonicalization 保持不变。
- 物理 Preserve 侧为空时折叠 Outer Join，并能穿透行数保持不变的标量计算和嵌套 Outer Join。

验证结果：

- `databend-common-sql` 单元测试全部通过。
- SQL crate 轻量 planner/integration 测试全部通过。
- SQL crate `clippy -D warnings` 通过。
- 本次修改涉及的 Rust 文件通过格式和 diff 检查。

## 后续方向

- 为推导 NDV 保留置信度和来源。
- 为倾斜列维护 Top-N/频率统计。
- 将裁剪后的扫描工作量纳入 Join Cost，而非只依赖输出基数。
- 研究 Preserve 输入可能低成本为空时的自适应 Outer Join 执行。
