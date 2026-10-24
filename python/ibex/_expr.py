# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Immutable expressions for the initial Polars-compatible API subset."""

from dataclasses import dataclass
from datetime import date, datetime
import builtins


@dataclass(frozen=True, eq=False)
class Expr:
    _op: str
    _args: tuple

    def __bool__(self):
        raise TypeError("an expression has no truth value; use '&' or '|' instead of 'and' or 'or'")

    def _binary(self, op, other):
        return Expr(op, (self, _literal_or_expr(other)))

    def __add__(self, other): return self._binary("+", other)
    def __sub__(self, other): return self._binary("-", other)
    def __mul__(self, other): return self._binary("*", other)
    def __truediv__(self, other): return self._binary("/", other)
    def __eq__(self, other): return self._binary("==", other)  # type: ignore[override]
    def __ne__(self, other): return self._binary("!=", other)  # type: ignore[override]
    def __lt__(self, other): return self._binary("<", other)
    def __le__(self, other): return self._binary("<=", other)
    def __gt__(self, other): return self._binary(">", other)
    def __ge__(self, other): return self._binary(">=", other)
    def __and__(self, other): return self._binary("&&", other)
    def __or__(self, other): return self._binary("||", other)
    def __radd__(self, other): return _literal_or_expr(other)._binary("+", self)
    def __rsub__(self, other): return _literal_or_expr(other)._binary("-", self)
    def __rmul__(self, other): return _literal_or_expr(other)._binary("*", self)
    def __rtruediv__(self, other): return _literal_or_expr(other)._binary("/", self)
    def __rand__(self, other): return _literal_or_expr(other)._binary("&&", self)
    def __ror__(self, other): return _literal_or_expr(other)._binary("||", self)
    def __invert__(self): return Expr("!", (self,))
    def __neg__(self): return lit(0) - self

    def alias(self, name: str):
        _identifier(name)
        return Expr("alias", (self, name))

    def sum(self):
        if self._aggregate:
            raise NotImplementedError("nested reductions are not supported yet")
        return Expr("sum", (self,))

    def count(self):
        if self._aggregate:
            raise NotImplementedError("nested reductions are not supported yet")
        return Expr("count", (self,))

    def len(self):
        if self._aggregate:
            raise NotImplementedError("nested reductions are not supported yet")
        return Expr("column_len", (self,))

    def mean(self):
        if self._aggregate:
            raise NotImplementedError("nested reductions are not supported yet")
        return Expr("mean", (self,))

    def first(self, *, ignore_nulls=False):
        return self._edge("first", ignore_nulls)

    def last(self, *, ignore_nulls=False):
        return self._edge("last", ignore_nulls)

    def _edge(self, name, ignore_nulls):
        if self._aggregate:
            raise NotImplementedError("nested reductions are not supported yet")
        if type(ignore_nulls) is not bool:
            raise TypeError("ignore_nulls must be a bool")
        return Expr(name + ("_non_null" if ignore_nulls else ""), (self,))

    def is_between(self, lower_bound, upper_bound, closed="both"):
        if closed not in ("both", "left", "right", "none"):
            raise ValueError("closed must be 'both', 'left', 'right', or 'none'")
        lower = _into_expr(lower_bound, allow_literal=True)
        upper = _into_expr(upper_bound, allow_literal=True)
        return ((self >= lower) if closed in ("both", "left") else (self > lower)) & (
            (self <= upper) if closed in ("both", "right") else (self < upper)
        )

    @property
    def _aggregate(self):
        if self._op == "broadcast_length":
            return False
        return self._op in ("sum", "mean", "min", "max", "count", "column_len", "first", "last", "first_non_null", "last_non_null", "len") or any(
            arg._aggregate for arg in self._args if isinstance(arg, Expr)
        )

    @property
    def _row_reference(self):
        if self._op in ("repeat", "repeat_null", "broadcast_length"):
            return True
        if self._op in ("sum", "mean", "min", "max", "count", "column_len", "first", "last", "first_non_null", "last_non_null", "len"):
            return False
        return self._op == "col" or any(
            arg._row_reference for arg in self._args if isinstance(arg, Expr)
        )

    @property
    def _name(self):
        if self._op == "alias":
            return self._args[1]
        if self._op == "col":
            return self._args[0]
        if self._op == "len":
            return "len"
        for arg in self._args:
            if isinstance(arg, Expr):
                return arg._name
        return "literal"


def _identifier(name):
    if not isinstance(name, str) or not name:
        raise TypeError("column names must be non-empty strings")
    if any(char in name for char in ("`", "\n", "\r", "\0")) or "${" in name:
        raise NotImplementedError("this prototype cannot represent that column name safely")
    return "`" + name + "`"


def col(name: str) -> Expr:
    _identifier(name)
    return Expr("col", (name,))


def lit(value) -> Expr:
    if type(value) not in (bool, int, float, str, date, datetime):
        raise NotImplementedError(f"literal type {type(value).__name__} is not supported yet")
    if type(value) is int and not -(2**63) <= value < 2**63:
        raise OverflowError("integer literals must fit Int64")
    return Expr("lit", (value,))


def _literal_or_expr(value):
    return value if isinstance(value, Expr) else lit(value)


def _into_expr(value, *, allow_literal=False):
    if isinstance(value, str):
        return col(value)
    if isinstance(value, Expr):
        return value
    if allow_literal:
        return lit(value)
    raise TypeError("expected a column name or an Ibex expression")


def _expressions(values, named):
    if builtins.len(values) == 1 and isinstance(values[0], (list, tuple)):
        values = values[0]
    result = tuple(_into_expr(value) for value in values) + tuple(
        _into_expr(value).alias(name) for name, value in named.items()
    )
    if not result:
        raise NotImplementedError("empty expression lists are not supported yet")
    names = [expr._name for expr in result]
    if builtins.len(set(names)) != builtins.len(names):
        raise ValueError("duplicate output column names")
    return result


def sum(*names: str) -> Expr:
    if builtins.len(names) != 1:
        raise NotImplementedError("sum currently accepts exactly one column name")
    return col(names[0]).sum()


def mean(*names: str) -> Expr:
    if builtins.len(names) != 1:
        raise NotImplementedError("mean currently accepts exactly one column name")
    return col(names[0]).mean()


def len() -> Expr:
    """Count rows, including rows containing null values."""
    return Expr("len", ())
