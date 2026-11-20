"""物理计划输出：执行片段、交换边、整体结果。"""

from dataclasses import dataclass, field
from typing import List

# 交换方式
BROADCAST = "broadcast"
HASH = "hash"
RANGE = "range"
GATHER = "gather"


@dataclass
class Fragment:
    """可在一组节点上执行的线性算子流水线。"""

    id: int
    nodes: List[int]
    ops: List[str] = field(default_factory=list)
    inputs: List[int] = field(default_factory=list)  # 上游片段 id（同节点直接传递）
    est_rows: float = 0.0
    est_bytes: float = 0.0
    memory_bytes: float = 0.0  # 该片段单节点峰值内存估计


@dataclass
class ExchangeEdge:
    """片段之间的数据交换边。"""

    source: int
    target: int
    kind: str  # broadcast / hash / range / gather
    keys: List[str] = field(default_factory=list)
    est_rows: float = 0.0
    est_bytes: float = 0.0


@dataclass
class PlanResult:
    fragments: List[Fragment] = field(default_factory=list)
    exchanges: List[ExchangeEdge] = field(default_factory=list)
    est_rows: float = 0.0
    peak_memory_bytes: float = 0.0
    network_bytes: float = 0.0
    issues: List[str] = field(default_factory=list)
