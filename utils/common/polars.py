from typing import Final

import polars as pl


type IntoExpr = str | list[str] | pl.Expr


def into_expr(expr: IntoExpr) -> pl.Expr:
    if isinstance(expr, str):
        return pl.col(expr)
    if isinstance(expr, list):
        return pl.col(expr)
    return expr


def optional_alias(expr: pl.Expr, new_name: str | None) -> pl.Expr:
    return expr.alias(new_name) if new_name else expr


def is_in_range(expr: IntoExpr, range: tuple[int | None, int | None] | None) -> pl.Expr:
    expr = into_expr(expr)

    if range is None:
        return pl.lit(True)
    elif range[0] is not None and range[1] is not None:
        return expr.is_between(range[0], range[1], "both")
    elif range[0] is not None:
        return expr.ge(range[0])
    elif range[1] is not None:
        return expr.le(range[1])
    else:
        return pl.lit(True)


def sort_list_workaround(expr: pl.Expr, sort_by_expr: pl.Expr) -> pl.Expr:
    # Workaround for "sort_by of empty list fails" bug in Polars.
    # Bug is tracked here: https://github.com/pola-rs/polars/issues/25433
    # Workaround can be removed once the bug is fixed.
    use_workaround: Final = True
    if use_workaround:
        # Workaround: Ensure the list is never empty by temporarily appending
        #             a `null` entry that is removed after sorting.
        return pl.concat_list([expr, pl.lit(None, pl.dtype_of(expr).list.inner_dtype())])\
            .list.eval(sort_by_expr)\
            .list.drop_nulls()
    else:
        return expr.list.eval(sort_by_expr)
