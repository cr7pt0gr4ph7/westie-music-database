from collections.abc import Sequence
from typing import Any, Literal

import polars as pl
import polars.selectors as cs

type IntoExpr = str | list[str] | pl.Expr | cs.Selector
type ColumnNameOrSelector = str | cs.Selector
type PivotAgg = Literal["min", "max", "first", "last", "sum", "mean", "median", "len"]


def into_expr(expr: IntoExpr) -> pl.Expr:
    if isinstance(expr, str):
        return pl.col(expr)
    if isinstance(expr, list):
        return pl.col(expr)
    if cs.is_selector(expr):
        return expr.as_expr()
    return expr


def optional_alias(expr: pl.Expr, new_name: str | None) -> pl.Expr:
    return expr.alias(new_name) if new_name else expr


def get_aggregate_expression(expr: pl.Expr, aggregate_function: PivotAgg | None):
    match aggregate_function:
        case "min":
            return expr.min()
        case "max":
            return expr.max()
        case "first":
            return expr.first()
        case "last":
            return expr.last()
        case "sum":
            return expr.sum()
        case "mean":
            return expr.mean()
        case "median":
            return expr.median()
        case "len":
            return expr.len()
        case None:
            return expr.item(allow_empty=True)
        case _:
            raise ValueError(f"Invalid aggregate_function value: {aggregate_function}")


def lazy_pivot(
    lf: pl.LazyFrame,
    on: ColumnNameOrSelector | Sequence[ColumnNameOrSelector],
    unique_on_values: Sequence[Any],
    *,
    index: ColumnNameOrSelector | Sequence[ColumnNameOrSelector] | None = None,
    values: ColumnNameOrSelector | Sequence[ColumnNameOrSelector] | None = None,
    aggregate_function: PivotAgg | None = None,
    maintain_order: bool = True,
    sort_columns: bool = False,
) -> pl.LazyFrame:
    if sort_columns:
        unique_on_values = list(unique_on_values)
        unique_on_values.sort()

    return lf.group_by(
        into_expr(index),
        maintain_order=maintain_order,
    ).agg(
        get_aggregate_expression(
            into_expr(values).filter(on == value),
            aggregate_function
        ).alias(value)
        for value in unique_on_values
    ).collect()
