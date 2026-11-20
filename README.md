# distributed-sql-exchange-planner

规划分布式 SQL 算子切分、数据交换和汇聚边界。

将逻辑计划（Scan / Filter / Join / Aggregate / Sort / Limit）转换为可在
各节点执行的**片段（Fragment）**，并在片段边界选择交换方式：
`broadcast`、`hash repartition`、`range repartition`、`gather`。

> **注意：本项目只生成执行计划，不实现数据执行。**
> 输出为片段列表、交换边、预计行数、内存峰值与一致性问题，
> 不包含任何执行引擎、数据传输或运行时。

## 环境

- Python 3.8.2+（仅标准库，无第三方依赖）

## 快速开始

```python
from planner import (Catalog, ColumnStats, TableMeta, DistributedPlanner,
                     PlannerConfig, Scan, Join, JoinCondition)

catalog = Catalog()
catalog.add(TableMeta("orders", row_count=1_000_000, partition_key="user_id",
                      num_partitions=4,
                      columns={"user_id": ColumnStats(ndv=100_000)}))
catalog.add(TableMeta("users", row_count=100_000, partition_key="user_id",
                      num_partitions=4,
                      columns={"user_id": ColumnStats(ndv=100_000)}))

planner = DistributedPlanner(catalog, PlannerConfig(
    num_nodes=4,
    node_memory_bytes=256 * 1024 * 1024,
    network_budget_bytes=4 * 1024 * 1024 * 1024))

result = planner.plan(Join(Scan("orders"), Scan("users"),
                           JoinCondition(["user_id"], ["user_id"])))
print(result.fragments, result.exchanges, result.issues)
```

## 输入

- 表元数据：分区键、分区数、行数、空分区数
- 列统计：NDV、NULL 比例、值域（min/max）、平均宽度
- 逻辑计划：过滤条件、Join 条件（含 broadcast hint）、聚合、排序、Limit
- 资源预算：节点数、单节点内存、网络预算

## 输出（`PlanResult`）

- `fragments`：执行片段（节点分配、算子流水线、估计行数/字节、单节点内存峰值）
- `exchanges`：交换边（类型、键、估计行数/字节）
- `est_rows` / `peak_memory_bytes` / `network_bytes`：全局估计
- `issues`：一致性问题（空分区、NULL 连接键、缺少值域统计、聚合状态超内存等）

## 交换边界正确性规则

- **共分区 Join**：两侧分区键与 Join 键一致且节点映射相同 → 本地 Join，零 shuffle
- **广播 Join**：小表估计字节 ≤ 单节点内存且广播成本 < shuffle 成本时选择；
  广播侧超过单节点内存（或 hint 强制广播超大表）时在**规划阶段直接拒绝**
  （抛出 `PlanningError`）
- **Hash repartition**：Join 两侧分区键不一致时，仅对未按 Join 键分区的一侧
  （或两侧）按 Join 键重分区
- **两阶段聚合**：先 `partial_aggregate`，再按分组键 hash repartition 后
  `final_aggregate`；输入已按分组键分区时省略微 shuffle；无分组键时 gather 到单节点
- **NULL 连接键**：inner join 下 NULL 键不可能匹配，shuffle 前插入
  `filter_null_keys`，估计行数同步扣减，并记录一致性问题
- **空分区**：空分区 / 空表仍生成片段（产出 0 行），并记录一致性问题
- **排序**：本地排序后按值域统计做 range repartition + 归并排序；
  缺少值域统计时退化为 gather 单机归并并记录问题
- **Limit 下推**：每个分片先 `limit_pushdown(n)`，gather 后再应用最终 `limit(n)`；
  `Limit(Sort)` 识别为 top-k，避免全量 range shuffle
- **网络预算**：累计 shuffle 字节（广播按 字节 × 节点数 计）超过预算时
  抛出 `PlanningError`

## 测试

```bash
python -m unittest discover -s tests
# 或
python -m unittest
```

覆盖：共分区 Join、广播 Join、超大广播拒绝、三表 Join、两阶段聚合、
NULL 连接键、range 排序归并、Limit 下推、top-k、网络预算不足、空分区。

## 目录结构

```
planner/
  stats.py      # 表 / 列统计与 Catalog
  logical.py    # 逻辑计划算子
  estimator.py  # 基数与字节估计
  physical.py   # Fragment / ExchangeEdge / PlanResult
  planner.py    # DistributedPlanner 主逻辑
tests/
  test_planner.py
```
