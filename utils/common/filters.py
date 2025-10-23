"""Utility functions for parsing query parameters into polars filters."""
from typing import Literal

import polars as pl

IntoExpr = str | list[str] | pl.Expr


def into_expr(expr: IntoExpr) -> pl.Expr:
    if isinstance(expr, str):
        return pl.col(expr)
    if isinstance(expr, list):
        return pl.col(expr)
    return expr


def or_filter(*filters: pl.Expr | None) -> pl.Expr | None:
    expr: pl.Exp | None = None
    for filter in filters:
        if filter is not None:
            expr = expr | filter if expr is not None else filter
    return expr


def create_text_filter(
    filter_expression: str | list[str] | None,
    column: IntoExpr,
    *,
    no_value: str = '',
    is_list_column: bool = False,
    ascii_case_insensitive: bool = True,
    match_mode: Literal['exact', 'contains'] = 'contains',
) -> pl.Expr | None:
    """Parse a filter expression for a text column."""
    if filter_expression is None:
        return None

    if isinstance(filter_expression, list):
        if ascii_case_insensitive:
            values = [item.lower() for item in filter_expression if item]
        else:
            values = list(filter(bool, filter_expression))
    else:
        if ascii_case_insensitive:
            values = list(
                filter(bool, filter_expression.strip().lower().split(',')))
        else:
            values = list(filter(bool, filter_expression.strip().split(',')))

    if not values:
        return None

    def is_match(expr: pl.Expr) -> pl.Expr:
        if match_mode == 'contains':
            return expr.cast(pl.String).str.contains_any(values, ascii_case_insensitive=ascii_case_insensitive)
        elif match_mode == 'exact':
            if ascii_case_insensitive:
                return expr.cast(pl.String).str.to_lowercase().is_in(values)
            else:
                return expr.cast(pl.String).is_in(values)
        else:
            raise ValueError(f'Invalid match mode: {match_mode}')

    if no_value and not is_list_column:
        raise ValueError("no_value may only be specified when is_list_column is True")

    if no_value and is_list_column and no_value.lower() in values:
        return into_expr(column).pipe(lambda x: pl.any_horizontal(x.is_null(), x.list.len().eq(0)))
    elif is_list_column:
        return into_expr(column).list.eval(is_match(pl.element())).list.any()
    else:
        return is_match(into_expr(column))


def create_date_filter(filter_expression: str, column: IntoExpr, *, is_list_column: bool = False) -> pl.Expr | None:
    """Parse a filter expression for a date column"""
    return create_text_filter(
        filter_expression,
        into_expr(column).dt.to_string(),
        ascii_case_insensitive=False,
        is_list_column=is_list_column)
