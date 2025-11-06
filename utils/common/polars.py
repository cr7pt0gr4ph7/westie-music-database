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
