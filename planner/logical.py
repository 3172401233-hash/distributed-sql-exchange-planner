"""逻辑计划算子定义（输入侧）。"""

from dataclasses import dataclass, field
from typing import Any, List, Optional


@dataclass
class Predicate:
    """过滤条件。op 支持 =, !=, <, <=, >, >=, is_null, is_not_null, in。"""

    column: str
    op: str
    value: Any = None


@dataclass
class Scan:
    table: str


@dataclass
class Filter:
    child: object
    predicates: List[Predicate] = field(default_factory=list)


@dataclass
class JoinCondition:
    left_keys: List[str] = field(default_factory=list)
    right_keys: List[str] = field(default_factory=list)


@dataclass
class Join:
    left: object
    right: object
    condition: JoinCondition
    join_type: str = "inner"  # inner / left / right
    hint: Optional[str] = None  # "broadcast" 强制广播
    hint_side: Optional[str] = None  # "left" / "right"，配合 hint 使用


@dataclass
class Aggregate:
    child: object
    group_keys: List[str] = field(default_factory=list)
    aggregates: List[str] = field(default_factory=list)  # 如 ["sum(amount)", "count(*)"]


@dataclass
class Sort:
    child: object
    order_keys: List[str] = field(default_factory=list)


@dataclass
class Limit:
    child: object
    count: int = 0
