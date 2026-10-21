"""Compile a small, type-checked subset of Ray's public expression nodes."""

from __future__ import annotations

from importlib import import_module
from typing import Any, Optional

import pyarrow as pa

from ray_doris._errors import DorisConfigurationError
from ray_doris._models import DorisPredicate
from ray_doris._sql import quote_identifier


def compile_predicate(expr: Any, schema: pa.Schema) -> Optional[DorisPredicate]:
    """Return None unless the complete expression has equivalent Doris semantics.

    Import DeveloperAPI expression nodes only after the compatibility gate. No
    expression text, private visitor, or Arrow expression string is parsed.
    """
    nodes = import_module("ray.data.expressions")

    comparisons = {
        nodes.Operation.EQ: "=",
        nodes.Operation.NE: "!=",
        nodes.Operation.LT: "<",
        nodes.Operation.LE: "<=",
        nodes.Operation.GT: ">",
        nodes.Operation.GE: ">=",
    }
    reverse = {"=": "=", "!=": "!=", "<": ">", "<=": ">=", ">": "<", ">=": "<="}

    def column(node: Any) -> Optional[tuple[str, pa.DataType]]:
        if not isinstance(node, nodes.ColumnExpr) or not isinstance(node.name, str):
            return None
        index = schema.get_field_index(node.name)
        if index < 0:
            return None
        dtype = schema.field(index).type
        if not (pa.types.is_signed_integer(dtype) or pa.types.is_boolean(dtype)):
            return None
        try:
            return quote_identifier(node.name), dtype
        except DorisConfigurationError:
            return None

    def visit(node: Any, depth: int = 0) -> Optional[DorisPredicate]:
        # Bound recursive compilation without changing Ray's residual evaluator.
        if depth > 64:
            return None
        if isinstance(node, nodes.BinaryExpr):
            if node.op in (nodes.Operation.AND, nodes.Operation.OR):
                left, right = visit(node.left, depth + 1), visit(node.right, depth + 1)
                if left is None or right is None:
                    return None
                op = "AND" if node.op == nodes.Operation.AND else "OR"
                return DorisPredicate(
                    f"({left.mysql_sql}) {op} ({right.mysql_sql})",
                    left.parameters + right.parameters,
                    f"({left.literal_sql}) {op} ({right.literal_sql})",
                )
            if node.op not in comparisons:
                return None
            op = comparisons[node.op]
            lhs, rhs = node.left, node.right
            if isinstance(lhs, nodes.LiteralExpr) and isinstance(rhs, nodes.ColumnExpr):
                lhs, rhs, op = rhs, lhs, reverse[op]
            field = column(lhs)
            if field is None or not isinstance(rhs, nodes.LiteralExpr):
                return None
            name, dtype = field
            value = rhs.value
            if pa.types.is_boolean(dtype):
                if type(value) is not bool or op not in ("=", "!="):
                    return None
                literal = "TRUE" if value else "FALSE"
            else:
                if type(value) is not int:
                    return None
                bound = 1 << (dtype.bit_width - 1)
                if not -bound <= value < bound:
                    return None
                literal = str(value)
            return DorisPredicate(f"{name} {op} %s", (value,), f"{name} {op} {literal}")
        if isinstance(node, nodes.UnaryExpr):
            if node.op == nodes.Operation.NOT:
                child = visit(node.operand, depth + 1)
                if child is None:
                    return None
                return DorisPredicate(
                    f"NOT ({child.mysql_sql})", child.parameters, f"NOT ({child.literal_sql})"
                )
            if node.op in (nodes.Operation.IS_NULL, nodes.Operation.IS_NOT_NULL):
                field = column(node.operand)
                if field is None:
                    return None
                op = "IS NULL" if node.op == nodes.Operation.IS_NULL else "IS NOT NULL"
                sql = f"{field[0]} {op}"
                return DorisPredicate(sql, (), sql)
        return None

    return visit(expr)
