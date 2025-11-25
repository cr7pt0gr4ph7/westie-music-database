"""Utility classes for creating table definitions that provide column names in a self-documenting manner."""
from typing import ClassVar, LiteralString, Self

import polars as pl
import polars.selectors as cs

from utils.common.ast import get_attr_docstrings
from utils.common.typing import get_type_args_of_base


class Entity:
    _attr_docs: ClassVar[dict[str, str] | None] = None

    @classmethod
    def _get_attr_doc(cls: type[Self], name: str) -> str | None:
        """Gets the docstring for the specified attribute, if any."""
        if cls == Entity:
            return None

        if cls._attr_docs is None:
            cls._attr_docs = get_attr_docstrings(cls)

        if (out := cls._attr_docs.get(name)) is not None:
            return out

        for base in cls.__mro__:
            if (base != cls and issubclass(base, Entity)
                    and (out := base._get_attr_doc(name)) is not None):
                return out

        return None

    @classmethod
    def matching_columns(cls) -> cs.Selector:
        return cs.starts_with(getattr(cls, 'PREFIX'))


class SubEntity[Child: Entity]:
    entity_type: type[Child]

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        cls.entity_type = get_type_args_of_base(cls, SubEntity)[0]


type PolarsLazyFrame[T: Entity] = pl.LazyFrame
type PolarsExpr[T] = pl.Expr


class Field[FieldName: LiteralString, FieldType: pl.DataType](str):
    _field_type: pl.DataType | type[pl.DataType] | None = None
    _field_docs: str | None = None
    _field_docs_overridable: bool = False

    @property
    def field_name(self) -> str:
        return str(self)

    @property
    def field_type(self) -> pl.DataType | type[pl.DataType] | None:
        return self._field_type

    @property
    def field_docs(self) -> str | None:
        return self._field_docs

    def __new__(
        cls,
        field_name: FieldName,
        field_type: FieldType | type[FieldType] | None = None,
        field_docs: str | None = None,
        field_docs_overridable: bool = False
    ):
        field = str.__new__(cls, field_name)
        if field_type is not None:
            field._field_type = field_type
        if field_docs is not None:
            field._field_docs = field_docs
        if field_docs_overridable != field._field_docs_overridable:
            field._field_docs_overridable = field_docs_overridable
        return field

    def __set_name__(self, owner: type, name: str):
        if ((self._field_docs is None or self._field_docs_overridable) and issubclass(owner, Entity)):
            self._field_docs = owner._get_attr_doc(name)

        self._field_docs_overridable = False

    def alias[NewName: LiteralString](self, new_name: NewName, *, help: str | None = None) -> 'Field[NewName, FieldType]':
        return Field(new_name, self.field_type, help if help is not None else self.field_docs, help is not None)

    def cast[NewType: pl.DataType](self, new_type: NewType | type[NewType], *, help: str | None = None) -> 'Field[FieldName, NewType]':
        return Field(self.field_name, new_type, help if help is not None else self.field_docs, help is not None)

    def with_help(self, help: str | None) -> Self:
        return Field(self.field_name, self.field_type, help if help is not None else self.field_docs, help is not None)

    def list(self) -> 'Field[FieldName, pl.List]':
        return Field(self.field_name, pl.List(self.field_type), self.field_docs, True)

    def __call__(self) -> pl.Expr:
        return pl.col(self.field_name)

    def as_expr(self) -> pl.Expr:
        return pl.col(self.field_name)

    def as_struct_field(self) -> pl.Field:
        return pl.Field(self.field_name, self.field_type)

    def struct_field(self, expr: pl.Expr | None = None) -> pl.Expr:
        expr = pl.element() if expr is None else expr
        return expr.struct.field(self.field_name)


def field[FieldName: LiteralString, FieldType: pl.DataType](
    field_name: FieldName,
    field_type: FieldType | type[FieldType] | None = None,
    help: str | None = None,
) -> Field[FieldName, FieldType]:
    return Field(field_name, field_type, help)
