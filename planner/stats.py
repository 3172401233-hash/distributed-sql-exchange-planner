"""表与列的统计信息（规划期输入）。"""

from dataclasses import dataclass, field
from typing import Dict, Optional


@dataclass
class ColumnStats:
    """单列统计：不同值个数、NULL 比例、值域、平均宽度（字节）。"""

    ndv: float = 1.0
    null_frac: float = 0.0
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    avg_width: int = 8


@dataclass
class TableMeta:
    """表的物理分布与统计。

    partition_key: 当前分区键（None 表示未分区 / 单副本）。
    num_partitions: 分区数，按 round-robin 映射到节点。
    empty_partitions: 空分区数量（规划器需要显式处理）。
    """

    name: str
    row_count: int
    partition_key: Optional[str] = None
    num_partitions: int = 1
    columns: Dict[str, ColumnStats] = field(default_factory=dict)
    empty_partitions: int = 0

    @property
    def row_width(self) -> int:
        if not self.columns:
            return 64
        return sum(max(c.avg_width, 1) for c in self.columns.values())


@dataclass
class Catalog:
    tables: Dict[str, TableMeta] = field(default_factory=dict)

    def add(self, table: TableMeta) -> "Catalog":
        self.tables[table.name] = table
        return self

    def get(self, name: str) -> TableMeta:
        if name not in self.tables:
            raise KeyError("unknown table: %s" % name)
        return self.tables[name]
