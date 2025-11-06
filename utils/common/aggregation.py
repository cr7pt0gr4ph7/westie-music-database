from typing import Callable
import polars as pl

from utils.common.polars import IntoExpr, into_expr, optional_alias


def _rolling_window(
    aggregated_column: IntoExpr,
    prev_window: int,
    next_window: int,
    fill_value: IntoExpr | None = None,
    reduce: Callable[[pl.Expr, pl.Expr], pl.Expr] | None = None,
) -> pl.Expr:
    aggregated_column = into_expr(aggregated_column)

    exprs = [
        aggregated_column.shift(i, fill_value=fill_value)
        for i in range(prev_window, 0)
    ] + [
        aggregated_column.shift((-1)-i, fill_value=fill_value)
        for i in range(0, next_window)
    ]

    if reduce is not None:
        result = None
        first = True

        for expr in exprs:
            if first:
                result = expr
                first = False
            else:
                result = reduce(result, expr)

        return pl.lit(None) if result is None else result
    else:
        return pl.concat_arr(exprs)


def rolling_fixed(
    data: pl.LazyFrame,
    aggregated_column: IntoExpr,
    prev_window: int,
    next_window: int,
    group_by: IntoExpr | None = None,
    reduce: Callable[[pl.Expr, pl.Expr], pl.Expr] | None = None,
    output_column: str | None = None,
) -> pl.LazyFrame:
    aggregated_column = into_expr(aggregated_column)

    result = data\
        .with_columns(_rolling_window(aggregated_column,
                                      prev_window=prev_window,
                                      next_window=next_window,
                                      reduce=reduce)
                      .pipe(optional_alias, output_column))

    if group_by is not None:
        result = result\
            .filter(group_by.eq(group_by.shift(prev_window)) if prev_window else pl.lit(True),
                    group_by.eq(group_by.shift(-next_window)) if next_window else pl.lit(True))
    else:
        result = result\
            .head(-prev_window)\
            .tail(-next_window)

    return result
