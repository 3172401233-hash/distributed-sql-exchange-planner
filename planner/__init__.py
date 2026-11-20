"""Distributed SQL logical plan -> physical fragment transformer.

本包只生成执行计划（片段、交换边、行数 / 内存估计、一致性问题），
不执行任何数据。
"""

from .stats import Catalog, ColumnStats, TableMeta
from .logical import (
    Aggregate,
    Filter,
    Join,
    JoinCondition,
    Limit,
    Predicate,
    Scan,
    Sort,
)
from .physical import ExchangeEdge, Fragment, PlanResult
from .planner import DistributedPlanner, PlannerConfig, PlanningError

__all__ = [
    "Aggregate",
    "Catalog",
    "ColumnStats",
    "DistributedPlanner",
    "ExchangeEdge",
    "Filter",
    "Fragment",
    "Join",
    "JoinCondition",
    "Limit",
    "PlanResult",
    "PlannerConfig",
    "PlanningError",
    "Predicate",
    "Scan",
    "Sort",
    "TableMeta",
]
