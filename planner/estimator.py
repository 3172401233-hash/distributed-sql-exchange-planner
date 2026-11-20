"""基于列统计的基数 / 字节估计。"""

from typing import Dict, List

from .logical import Predicate
from .stats import ColumnStats

_DEFAULT_SELECTIVITY = 0.1
_RANGE_SELECTIVITY = 0.25


def combined_ndv(columns: Dict[str, ColumnStats], keys: List[str]) -> float:
    ndv = 1.0
    for key in keys:
        col = columns.get(key)
        ndv *= col.ndv if col and col.ndv > 0 else 1.0
    return max(ndv, 1.0)


def max_null_frac(columns: Dict[str, ColumnStats], keys: List[str]) -> float:
    fracs = [columns[k].null_frac for k in keys if k in columns]
    return max(fracs) if fracs else 0.0


def predicate_selectivity(pred: Predicate, columns: Dict[str, ColumnStats]) -> float:
    col = columns.get(pred.column)
    ndv = col.ndv if col and col.ndv > 0 else 1.0
    null_frac = col.null_frac if col else 0.0
    op = pred.op
    if op == "=":
        return min(1.0, 1.0 / ndv) if ndv > 1 else _DEFAULT_SELECTIVITY
    if op == "!=":
        return 1.0 - (min(1.0, 1.0 / ndv) if ndv > 1 else _DEFAULT_SELECTIVITY)
    if op in ("<", "<=", ">", ">="):
        return _RANGE_SELECTIVITY
    if op == "is_null":
        return max(null_frac, 0.0)
    if op == "is_not_null":
        return 1.0 - null_frac
    if op == "in":
        values = pred.value if isinstance(pred.value, (list, tuple)) else [pred.value]
        return min(1.0, len(values) / ndv) if ndv > 1 else _DEFAULT_SELECTIVITY
    return _DEFAULT_SELECTIVITY


def estimate_filter(rows: float, predicates: List[Predicate],
                    columns: Dict[str, ColumnStats]) -> float:
    selectivity = 1.0
    for pred in predicates:
        selectivity *= predicate_selectivity(pred, columns)
    return rows * selectivity


def estimate_join_rows(left_rows: float, right_rows: float,
                       left_columns: Dict[str, ColumnStats],
                       right_columns: Dict[str, ColumnStats],
                       left_keys: List[str], right_keys: List[str],
                       join_type: str = "inner") -> float:
    """等值连接估计：rows_l * rows_r / max(ndv_l, ndv_r)，并扣除 NULL 键。"""
    ndv = max(combined_ndv(left_columns, left_keys),
              combined_ndv(right_columns, right_keys))
    keep_l = 1.0 - max_null_frac(left_columns, left_keys)
    keep_r = 1.0 - max_null_frac(right_columns, right_keys)
    matched = left_rows * keep_l * right_rows * keep_r / ndv
    if join_type == "left":
        return max(matched, left_rows)
    if join_type == "right":
        return max(matched, right_rows)
    return matched


def estimate_aggregate_rows(rows: float, group_keys: List[str],
                            columns: Dict[str, ColumnStats]) -> float:
    if not group_keys:
        return 1.0
    return max(1.0, min(rows, combined_ndv(columns, group_keys)))
