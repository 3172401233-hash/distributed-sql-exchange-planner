import unittest

from planner import (
    Aggregate,
    Catalog,
    ColumnStats,
    DistributedPlanner,
    Filter,
    Join,
    JoinCondition,
    Limit,
    PlannerConfig,
    PlanningError,
    Predicate,
    Scan,
    Sort,
    TableMeta,
)
from planner.physical import BROADCAST, GATHER, HASH, RANGE

MB = 1024 * 1024


def make_catalog():
    catalog = Catalog()
    catalog.add(TableMeta(
        name="orders", row_count=1_000_000, partition_key="user_id",
        num_partitions=4,
        columns={
            "user_id": ColumnStats(ndv=100_000, avg_width=8),
            "amount": ColumnStats(ndv=50_000, avg_width=8),
            "region": ColumnStats(ndv=10, avg_width=8),
        }))
    catalog.add(TableMeta(
        name="users", row_count=100_000, partition_key="user_id",
        num_partitions=4,
        columns={
            "user_id": ColumnStats(ndv=100_000, avg_width=8),
            "age": ColumnStats(ndv=100, avg_width=4),
        }))
    catalog.add(TableMeta(
        name="dim", row_count=100, partition_key=None, num_partitions=1,
        columns={"region": ColumnStats(ndv=10, avg_width=8)}))
    return catalog


def make_planner(catalog=None, **config_kwargs):
    kwargs = dict(num_nodes=4, node_memory_bytes=256 * MB,
                  network_budget_bytes=4096 * MB)
    kwargs.update(config_kwargs)
    return DistributedPlanner(catalog or make_catalog(), PlannerConfig(**kwargs))


def exchange_kinds(result):
    return [e.kind for e in result.exchanges]


def all_ops(result):
    return [op for frag in result.fragments for op in frag.ops]


class CoPartitionedJoinTest(unittest.TestCase):
    def test_no_exchange_when_partition_keys_match(self):
        plan = Join(Scan("orders"), Scan("users"),
                    JoinCondition(["user_id"], ["user_id"]))
        result = make_planner().plan(plan)
        self.assertEqual(result.exchanges, [])
        self.assertIn("colocated_join(user_id)", all_ops(result))
        self.assertGreater(result.est_rows, 0)


class BroadcastJoinTest(unittest.TestCase):
    def test_small_table_is_broadcast(self):
        plan = Join(Scan("orders"), Scan("dim"),
                    JoinCondition(["region"], ["region"]))
        result = make_planner().plan(plan)
        self.assertEqual(exchange_kinds(result), [BROADCAST])
        edge = result.exchanges[0]
        source = result.fragments[edge.source]
        self.assertTrue(any(op.startswith("scan(dim)") for op in source.ops))
        self.assertLessEqual(result.peak_memory_bytes,
                             make_planner().config.node_memory_bytes)

    def test_oversized_broadcast_rejected_at_planning(self):
        big = Catalog().add(TableMeta(
            name="huge", row_count=10_000_000, partition_key="k",
            num_partitions=4,
            columns={"k": ColumnStats(ndv=1_000_000, avg_width=512)}))
        big.add(TableMeta(
            name="fact", row_count=10_000_000, partition_key="k",
            num_partitions=4,
            columns={"k": ColumnStats(ndv=1_000_000, avg_width=512)}))
        planner = make_planner(catalog=big, node_memory_bytes=1 * MB)
        plan = Join(Scan("fact"), Scan("huge"),
                    JoinCondition(["k"], ["k"]), hint=BROADCAST,
                    hint_side="right")
        with self.assertRaises(PlanningError) as ctx:
            planner.plan(plan)
        self.assertIn("广播被拒绝", str(ctx.exception))


class ThreeTableJoinTest(unittest.TestCase):
    def test_three_table_join(self):
        catalog = make_catalog()
        catalog.add(TableMeta(
            name="items", row_count=500_000, partition_key="user_id",
            num_partitions=4,
            columns={
                "user_id": ColumnStats(ndv=100_000, avg_width=8),
                "sku": ColumnStats(ndv=50_000, avg_width=8),
            }))
        plan = Join(
            Join(Scan("orders"), Scan("users"),
                 JoinCondition(["user_id"], ["user_id"])),
            Scan("items"),
            JoinCondition(["user_id"], ["user_id"]))
        result = make_planner(catalog=catalog).plan(plan)
        # 第一次 join 共分区，第二次 join 输出已按 user_id 分布，无需 shuffle
        self.assertEqual(result.exchanges, [])
        joins = [op for op in all_ops(result) if "join" in op]
        self.assertEqual(len(joins), 2)
        self.assertGreater(result.est_rows, 0)


class TwoPhaseAggregateTest(unittest.TestCase):
    def test_partial_then_final_with_repartition(self):
        plan = Aggregate(
            Filter(Scan("orders"), [Predicate("amount", ">", 100)]),
            group_keys=["region"], aggregates=["sum(amount)", "count(*)"])
        result = make_planner().plan(plan)
        ops = all_ops(result)
        self.assertTrue(any(op.startswith("partial_aggregate") for op in ops))
        self.assertTrue(any(op.startswith("final_aggregate") for op in ops))
        self.assertEqual(exchange_kinds(result), [HASH])
        self.assertEqual(result.exchanges[0].keys, ["region"])
        # 分组数被 ndv(region)=10 约束
        self.assertEqual(result.est_rows, 10)

    def test_colocated_aggregate_skips_shuffle(self):
        plan = Aggregate(Scan("orders"), group_keys=["user_id"],
                         aggregates=["count(*)"])
        result = make_planner().plan(plan)
        self.assertEqual(result.exchanges, [])
        self.assertIn("final_aggregate(colocated)", all_ops(result))

    def test_global_aggregate_gathers(self):
        plan = Aggregate(Scan("orders"), group_keys=[],
                         aggregates=["count(*)"])
        result = make_planner().plan(plan)
        self.assertEqual(exchange_kinds(result), [GATHER])
        self.assertEqual(result.est_rows, 1)
        final = result.fragments[result.exchanges[0].target]
        self.assertEqual(final.nodes, [0])


class NullJoinKeyTest(unittest.TestCase):
    def test_null_keys_filtered_before_shuffle(self):
        catalog = make_catalog()
        catalog.tables["users"].columns["user_id"].null_frac = 0.2
        plan = Join(Scan("orders"), Scan("users"),
                    JoinCondition(["user_id"], ["user_id"]))
        result = make_planner(catalog=catalog).plan(plan)
        self.assertTrue(any("NULL" in issue for issue in result.issues))
        self.assertTrue(any(op.startswith("filter_null_keys")
                            for op in all_ops(result)))
        # 无 NULL 时估计 1e6 * 1e5 / 1e5 = 1e6，NULL 比例 0.2 后应为 8e5
        self.assertAlmostEqual(result.est_rows, 800_000)


class SortMergeTest(unittest.TestCase):
    def test_range_repartition_and_merge_sort(self):
        catalog = make_catalog()
        catalog.tables["orders"].columns["amount"].min_value = 0
        catalog.tables["orders"].columns["amount"].max_value = 10_000
        plan = Sort(Scan("orders"), ["amount"])
        result = make_planner(catalog=catalog).plan(plan)
        self.assertEqual(exchange_kinds(result), [RANGE])
        ops = all_ops(result)
        self.assertIn("local_sort(amount)", ops)
        self.assertIn("merge_sort(amount)", ops)

    def test_missing_range_stats_falls_back_to_gather(self):
        plan = Sort(Scan("orders"), ["amount"])
        result = make_planner().plan(plan)
        self.assertEqual(exchange_kinds(result), [GATHER])
        self.assertTrue(any("值域统计" in issue for issue in result.issues))


class LimitPushdownTest(unittest.TestCase):
    def test_limit_pushed_down_then_gathered(self):
        plan = Limit(Scan("orders"), 10)
        result = make_planner().plan(plan)
        scan_frag = result.fragments[0]
        self.assertIn("limit_pushdown(10)", scan_frag.ops)
        self.assertEqual(exchange_kinds(result), [GATHER])
        self.assertEqual(result.est_rows, 10)
        final = result.fragments[result.exchanges[0].target]
        self.assertIn("limit(10)", final.ops)

    def test_top_k_sort_limit(self):
        catalog = make_catalog()
        catalog.tables["orders"].columns["amount"].min_value = 0
        catalog.tables["orders"].columns["amount"].max_value = 10_000
        plan = Limit(Sort(Scan("orders"), ["amount"]), 5)
        result = make_planner(catalog=catalog).plan(plan)
        # top-k: 本地排序 + 本地 limit 后直接 gather，不做 range shuffle
        self.assertEqual(exchange_kinds(result), [GATHER])
        self.assertEqual(result.est_rows, 5)
        self.assertIn("limit_pushdown(5)", all_ops(result))


class NetworkBudgetTest(unittest.TestCase):
    def test_insufficient_network_budget_rejected(self):
        plan = Join(Scan("orders"), Scan("dim"),
                    JoinCondition(["region"], ["region"]))
        planner = make_planner(network_budget_bytes=1)
        with self.assertRaises(PlanningError) as ctx:
            planner.plan(plan)
        self.assertIn("网络预算不足", str(ctx.exception))


class EmptyPartitionTest(unittest.TestCase):
    def test_empty_partitions_reported(self):
        catalog = make_catalog()
        catalog.tables["orders"].empty_partitions = 2
        result = make_planner(catalog=catalog).plan(Scan("orders"))
        self.assertTrue(any("空分区" in issue for issue in result.issues))
        self.assertEqual(len(result.fragments), 1)


if __name__ == "__main__":
    unittest.main()
