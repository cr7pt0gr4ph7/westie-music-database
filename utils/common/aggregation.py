import polars as pl

from utils.common.polars import IntoExpr, into_expr, optional_alias


def _rolling_window(
    aggregated_column: IntoExpr,
    prev_window: int,
    next_window: int,
    fill_value: IntoExpr | None = None
) -> pl.Expr:
    aggregated_column = into_expr(aggregated_column)

    return pl.concat_arr([
        aggregated_column.shift(i, fill_value=fill_value)
        for i in range(prev_window, 0)
    ] + [
        aggregated_column.shift((-1)-i, fill_value=fill_value)
        for i in range(0, next_window)
    ])


def rolling_fixed(
    data: pl.LazyFrame,
    aggregated_column: IntoExpr,
    prev_window: int,
    next_window: int,
    output_column: str | None = None,
) -> pl.LazyFrame:
    aggregated_column = into_expr(aggregated_column)

    return data\
        .with_columns(_rolling_window(aggregated_column, prev_window, next_window)
                      .pipe(optional_alias, output_column))\
        .head(-prev_window)\
        .tail(-next_window)


def rolling_fixed_group_by(
    data: pl.LazyFrame,
    aggregated_column: IntoExpr,
    prev_window: int,
    next_window: int,
    group_by: IntoExpr,
    output_column: str | None = None,
) -> pl.LazyFrame:
    aggregated_column = into_expr(aggregated_column)
    group_by = into_expr(group_by)

    # TODO: Document that this requires the input data to be sorted on the group_by field
    return data\
        .with_columns(_rolling_window(aggregated_column, prev_window, next_window)
                      .pipe(optional_alias, output_column))\
        .filter(group_by.eq(group_by.shift(prev_window)) if prev_window else pl.lit(True),
                group_by.eq(group_by.shift(-next_window)) if next_window else pl.lit(True))
