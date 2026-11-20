"""分布式逻辑计划转换器。

将逻辑计划（Scan/Filter/Join/Aggregate/Sort/Limit）切分为可在各节点
执行的片段（Fragment），并在片段之间插入 broadcast / hash repartition /
range repartition / gather 交换边。只生成计划，不执行数据。
"""

from dataclasses import dataclass
from typing import List, Optional

from . import estimator
from .logical import Aggregate, Filter, Join, Limit, Scan, Sort
from .physical import BROADCAST, GATHER, HASH, RANGE
from .physical import ExchangeEdge, Fragment, PlanResult
from .stats import Catalog, ColumnStats


class PlanningError(Exception):
    """规划阶段即可判定不可执行（如超大广播、网络预算不足）。"""


@dataclass
class PlannerConfig:
    num_nodes: int = 4
    node_memory_bytes: float = 256 * 1024 * 1024
    network_budget_bytes: float = 4 * 1024 * 1024 * 1024


class _Output(object):
    """规划过程中某个子计划的输出特征。"""

    def __init__(self, fragment, partition_keys, est_rows, est_bytes,
                 row_width, columns, single_node):
        self.fragment = fragment
        self.partition_keys = partition_keys
        self.est_rows = est_rows
        self.est_bytes = est_bytes
        self.row_width = row_width
        self.columns = columns
        self.single_node = single_node


class DistributedPlanner(object):
    def __init__(self, catalog: Catalog, config: Optional[PlannerConfig] = None):
        self.catalog = catalog
        self.config = config or PlannerConfig()
        self._fragments = []  # type: List[Fragment]
        self._exchanges = []  # type: List[ExchangeEdge]
        self._issues = []  # type: List[str]
        self._network_bytes = 0.0
        self._peak_memory = 0.0

    # ------------------------------------------------------------------ API

    def plan(self, node) -> PlanResult:
        output = self._plan_node(node)
        if self._network_bytes > self.config.network_budget_bytes:
            raise PlanningError(
                "网络预算不足: 预计 shuffle %.0f 字节, 预算 %.0f 字节"
                % (self._network_bytes, self.config.network_budget_bytes))
        return PlanResult(
            fragments=list(self._fragments),
            exchanges=list(self._exchanges),
            est_rows=output.est_rows,
            peak_memory_bytes=self._peak_memory,
            network_bytes=self._network_bytes,
            issues=list(self._issues),
        )

    # -------------------------------------------------------------- helpers

    def _new_fragment(self, nodes, ops=None, inputs=None) -> Fragment:
        frag = Fragment(id=len(self._fragments), nodes=list(nodes),
                        ops=list(ops or []), inputs=list(inputs or []))
        self._fragments.append(frag)
        return frag

    def _add_exchange(self, kind, source: Fragment, target: Fragment,
                      keys, est_rows, est_bytes) -> ExchangeEdge:
        edge = ExchangeEdge(source=source.id, target=target.id, kind=kind,
                            keys=list(keys), est_rows=est_rows,
                            est_bytes=est_bytes)
        self._exchanges.append(edge)
        if kind == BROADCAST:
            self._network_bytes += est_bytes * max(1, len(target.nodes))
        else:
            self._network_bytes += est_bytes
        return edge

    def _track_memory(self, fragment: Fragment, memory_bytes: float):
        fragment.memory_bytes = max(fragment.memory_bytes, memory_bytes)
        self._peak_memory = max(self._peak_memory, fragment.memory_bytes)

    def _check_broadcast_fits(self, est_bytes: float, table_desc: str):
        if est_bytes > self.config.node_memory_bytes:
            raise PlanningError(
                "广播被拒绝: %s 预计 %.0f 字节, 超过单节点内存 %.0f 字节"
                % (table_desc, est_bytes, self.config.node_memory_bytes))

    # ------------------------------------------------------------- dispatch

    def _plan_node(self, node) -> _Output:
        if isinstance(node, Scan):
            return self._plan_scan(node)
        if isinstance(node, Filter):
            return self._plan_filter(node)
        if isinstance(node, Join):
            return self._plan_join(node)
        if isinstance(node, Aggregate):
            return self._plan_aggregate(node)
        if isinstance(node, Sort):
            return self._plan_sort(node)
        if isinstance(node, Limit):
            return self._plan_limit(node)
        raise PlanningError("unsupported logical node: %r" % (node,))

    # ---------------------------------------------------------------- scan

    def _plan_scan(self, node: Scan) -> _Output:
        meta = self.catalog.get(node.table)
        num_nodes = max(1, self.config.num_nodes)
        nodes = sorted({p % num_nodes for p in range(max(1, meta.num_partitions))})
        frag = self._new_fragment(nodes, ops=["scan(%s)" % meta.name])
        if meta.empty_partitions > 0:
            self._issues.append(
                "表 %s 存在 %d 个空分区, 对应节点该片段产出 0 行"
                % (meta.name, meta.empty_partitions))
        if meta.row_count == 0:
            self._issues.append("表 %s 为空, 片段产出 0 行" % meta.name)
        est_rows = float(meta.row_count)
        est_bytes = est_rows * meta.row_width
        frag.est_rows = est_rows
        frag.est_bytes = est_bytes
        partition_keys = (meta.partition_key,) if meta.partition_key else ()
        return _Output(frag, partition_keys, est_rows, est_bytes,
                       meta.row_width, dict(meta.columns),
                       single_node=len(nodes) == 1)

    # -------------------------------------------------------------- filter

    def _plan_filter(self, node: Filter) -> _Output:
        child = self._plan_node(node.child)
        est_rows = estimator.estimate_filter(child.est_rows, node.predicates,
                                             child.columns)
        ratio = est_rows / child.est_rows if child.est_rows > 0 else 0.0
        desc = ",".join("%s %s %s" % (p.column, p.op, p.value)
                        for p in node.predicates)
        child.fragment.ops.append("filter(%s)" % desc)
        child.est_rows = est_rows
        child.est_bytes = child.est_bytes * ratio
        child.fragment.est_rows = est_rows
        child.fragment.est_bytes = child.est_bytes
        return child

    # ---------------------------------------------------------------- join

    def _plan_join(self, node: Join) -> _Output:
        left = self._plan_node(node.left)
        right = self._plan_node(node.right)
        cond = node.condition

        if node.join_type == "inner":
            self._drop_null_keys(left, cond.left_keys, "left")
            self._drop_null_keys(right, cond.right_keys, "right")

        est_rows = estimator.estimate_join_rows(
            left.est_rows, right.est_rows, left.columns, right.columns,
            cond.left_keys, cond.right_keys, node.join_type)
        row_width = left.row_width + right.row_width
        columns = dict(left.columns)
        columns.update(right.columns)

        co_partitioned = (
            left.partition_keys == tuple(cond.left_keys)
            and right.partition_keys == tuple(cond.right_keys)
            and left.fragment.nodes == right.fragment.nodes
        )

        if node.hint == BROADCAST:
            small, big = self._pick_broadcast_side(node, left, right)
            self._check_broadcast_fits(small.est_bytes, "hint 指定的广播侧")
            join_frag = self._broadcast_join(node, small, big)
        elif co_partitioned:
            join_frag = self._colocated_join(node, left, right)
        else:
            small, big = self._pick_broadcast_side(node, left, right)
            broadcast_cost = small.est_bytes * max(1, len(big.fragment.nodes))
            shuffle_cost = left.est_bytes + right.est_bytes
            if (small.est_bytes <= self.config.node_memory_bytes
                    and broadcast_cost < shuffle_cost):
                join_frag = self._broadcast_join(node, small, big)
            else:
                join_frag = self._repartition_join(node, left, right)

        join_frag.est_rows = est_rows
        join_frag.est_bytes = est_rows * row_width
        partition_keys = tuple(cond.left_keys)
        return _Output(join_frag, partition_keys, est_rows,
                       est_rows * row_width, row_width, columns,
                       single_node=len(join_frag.nodes) == 1)

    def _drop_null_keys(self, out: _Output, keys, side: str):
        null_frac = estimator.max_null_frac(out.columns, list(keys))
        if null_frac > 0:
            out.fragment.ops.append(
                "filter_null_keys(%s)" % ",".join(keys))
            keep = 1.0 - null_frac
            out.est_rows *= keep
            out.est_bytes *= keep
            for key in keys:
                if key in out.columns:
                    out.columns[key] = ColumnStats(
                        ndv=out.columns[key].ndv, null_frac=0.0,
                        min_value=out.columns[key].min_value,
                        max_value=out.columns[key].max_value,
                        avg_width=out.columns[key].avg_width)
            self._issues.append(
                "Join %s 侧键 %s 含 NULL (比例 %.2f), inner join 下已过滤, "
                "不参与 shuffle" % (side, ",".join(keys), null_frac))

    def _pick_broadcast_side(self, node: Join, left: _Output, right: _Output):
        if node.hint_side == "left":
            return left, right
        if node.hint_side == "right":
            return right, left
        return (left, right) if left.est_bytes <= right.est_bytes else (right, left)

    def _broadcast_join(self, node: Join, small: _Output, big: _Output) -> Fragment:
        self._check_broadcast_fits(small.est_bytes, "广播侧")
        join_frag = self._new_fragment(
            big.fragment.nodes,
            ops=["broadcast_join(%s)" % ",".join(node.condition.left_keys)],
            inputs=[small.fragment.id, big.fragment.id])
        self._add_exchange(BROADCAST, small.fragment, join_frag, [],
                           small.est_rows, small.est_bytes)
        self._track_memory(join_frag, small.est_bytes)
        return join_frag

    def _colocated_join(self, node: Join, left: _Output, right: _Output) -> Fragment:
        join_frag = self._new_fragment(
            left.fragment.nodes,
            ops=["colocated_join(%s)" % ",".join(node.condition.left_keys)],
            inputs=[left.fragment.id, right.fragment.id])
        build = min(left.est_bytes, right.est_bytes)
        self._track_memory(join_frag, build / max(1, len(join_frag.nodes)))
        return join_frag

    def _repartition_join(self, node: Join, left: _Output, right: _Output) -> Fragment:
        num_nodes = max(1, self.config.num_nodes)
        join_frag = self._new_fragment(
            list(range(num_nodes)),
            ops=["hash_join(%s)" % ",".join(node.condition.left_keys)],
            inputs=[left.fragment.id, right.fragment.id])
        for out, keys in ((left, node.condition.left_keys),
                          (right, node.condition.right_keys)):
            already = (out.partition_keys == tuple(keys)
                       and len(out.fragment.nodes) == num_nodes)
            if not already:
                self._add_exchange(HASH, out.fragment, join_frag, keys,
                                   out.est_rows, out.est_bytes)
        build = min(left.est_bytes, right.est_bytes)
        self._track_memory(join_frag, build / num_nodes)
        return join_frag

    # ------------------------------------------------------------ aggregate

    def _plan_aggregate(self, node: Aggregate) -> _Output:
        child = self._plan_node(node.child)
        group_rows = estimator.estimate_aggregate_rows(
            child.est_rows, node.group_keys, child.columns)
        agg_width = 16 * max(1, len(node.group_keys)) + 16 * max(1, len(node.aggregates))

        child.fragment.ops.append(
            "partial_aggregate(keys=%s, aggs=%s)"
            % (",".join(node.group_keys) or "-", ",".join(node.aggregates)))
        partial_rows = min(child.est_rows, group_rows * max(1, len(child.fragment.nodes)))
        partial_rows = max(partial_rows, group_rows)
        partial_bytes = partial_rows * agg_width

        if not node.group_keys:
            final = self._new_fragment([0], ops=["final_aggregate(global)"],
                                       inputs=[child.fragment.id])
            self._add_exchange(GATHER, child.fragment, final, [],
                               partial_rows, partial_bytes)
        elif child.partition_keys == tuple(node.group_keys):
            final = child.fragment
            final.ops.append("final_aggregate(colocated)")
        else:
            final = self._new_fragment(
                list(range(max(1, self.config.num_nodes))),
                ops=["final_aggregate(keys=%s)" % ",".join(node.group_keys)],
                inputs=[child.fragment.id])
            self._add_exchange(HASH, child.fragment, final, node.group_keys,
                               partial_rows, partial_bytes)

        per_node_groups = group_rows / max(1, len(final.nodes))
        state_bytes = per_node_groups * agg_width
        self._track_memory(final, state_bytes)
        if state_bytes > self.config.node_memory_bytes:
            self._issues.append(
                "聚合状态预计 %.0f 字节/节点, 超过单节点内存 %.0f 字节, 需落盘"
                % (state_bytes, self.config.node_memory_bytes))

        est_rows = group_rows
        est_bytes = est_rows * agg_width
        final.est_rows = est_rows
        final.est_bytes = est_bytes
        return _Output(final, tuple(node.group_keys), est_rows, est_bytes,
                       agg_width, {}, single_node=len(final.nodes) == 1)

    # ----------------------------------------------------------------- sort

    def _plan_sort(self, node: Sort) -> _Output:
        child = self._plan_node(node.child)
        child.fragment.ops.append("local_sort(%s)" % ",".join(node.order_keys))
        if child.single_node:
            return child

        key_col = child.columns.get(node.order_keys[0]) if node.order_keys else None
        has_range = (key_col is not None and key_col.min_value is not None
                     and key_col.max_value is not None)
        if has_range:
            final = self._new_fragment(
                list(range(max(1, self.config.num_nodes))),
                ops=["merge_sort(%s)" % ",".join(node.order_keys)],
                inputs=[child.fragment.id])
            self._add_exchange(RANGE, child.fragment, final, node.order_keys,
                               child.est_rows, child.est_bytes)
            partition_keys = tuple(node.order_keys)
            single = False
        else:
            self._issues.append(
                "排序键 %s 缺少值域统计, 退化为 gather 后单机归并排序"
                % (node.order_keys[0] if node.order_keys else "?"))
            final = self._new_fragment(
                [0], ops=["merge_sort(%s)" % ",".join(node.order_keys)],
                inputs=[child.fragment.id])
            self._add_exchange(GATHER, child.fragment, final, [],
                               child.est_rows, child.est_bytes)
            partition_keys = ()
            single = True
            self._track_memory(final, child.est_bytes)

        final.est_rows = child.est_rows
        final.est_bytes = child.est_bytes
        return _Output(final, partition_keys, child.est_rows, child.est_bytes,
                       child.row_width, child.columns, single_node=single)

    # ---------------------------------------------------------------- limit

    def _plan_limit(self, node: Limit) -> _Output:
        if isinstance(node.child, Sort):
            return self._plan_top_k(node.child, node.count)
        child = self._plan_node(node.child)
        child.fragment.ops.append("limit_pushdown(%d)" % node.count)
        est_rows = min(float(node.count), child.est_rows)
        if child.single_node:
            child.est_rows = est_rows
            child.est_bytes = est_rows * child.row_width
            return child
        final = self._new_fragment([0], ops=["limit(%d)" % node.count],
                                   inputs=[child.fragment.id])
        self._add_exchange(GATHER, child.fragment, final, [],
                           est_rows * max(1, len(child.fragment.nodes)),
                           est_rows * len(child.fragment.nodes) * child.row_width)
        final.est_rows = est_rows
        final.est_bytes = est_rows * child.row_width
        return _Output(final, (), est_rows, est_rows * child.row_width,
                       child.row_width, child.columns, single_node=True)

    def _plan_top_k(self, sort: Sort, count: int) -> _Output:
        child = self._plan_node(sort.child)
        child.fragment.ops.append("local_sort(%s)" % ",".join(sort.order_keys))
        child.fragment.ops.append("limit_pushdown(%d)" % count)
        est_rows = min(float(count), child.est_rows)
        if child.single_node:
            child.est_rows = est_rows
            child.est_bytes = est_rows * child.row_width
            return child
        final = self._new_fragment(
            [0], ops=["merge_sort(%s)" % ",".join(sort.order_keys),
                      "limit(%d)" % count],
            inputs=[child.fragment.id])
        shuffle_rows = est_rows * max(1, len(child.fragment.nodes))
        self._add_exchange(GATHER, child.fragment, final, [],
                           shuffle_rows, shuffle_rows * child.row_width)
        self._track_memory(final, shuffle_rows * child.row_width)
        final.est_rows = est_rows
        final.est_bytes = est_rows * child.row_width
        return _Output(final, (), est_rows, est_rows * child.row_width,
                       child.row_width, child.columns, single_node=True)
