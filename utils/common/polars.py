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


def sort_list_workaround(expr: pl.Expr, sort_by_expr: pl.Expr) -> pl.Expr:
    # Workaround for "sort_by of empty list fails" bug in Polars.
    # Bug is tracked here: https://github.com/pola-rs/polars/issues/25433
    # Workaround can be removed once the bug is fixed.
    use_workaround: Final = True
    if use_workaround:
        # Workaround: Ensure the list is never empty by temporarily appending
        #             a `null` entry that is removed after sorting.
        return pl.concat_list([expr, pl.lit(None)])\
            .list.eval(sort_by_expr)\
            .list.drop_nulls()
    else:
        return expr.list.eval(sort_by_expr)
