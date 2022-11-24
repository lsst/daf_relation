# This file is part of daf_relation.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

from __future__ import annotations

__all__ = (
    "ColumnExpression",
    "ColumnLiteral",
    "ColumnReference",
    "ColumnFunction",
    "PredicateFunction",
)

import dataclasses
from abc import ABC, abstractmethod
from collections.abc import Iterable, Set
from typing import TYPE_CHECKING, Any

from lsst.utils.classes import cached_getter

from .._exceptions import RelationalAlgebraError
from ._predicate import Predicate
from ._tag import ColumnTag

if TYPE_CHECKING:
    from .._engine import Engine


class ColumnExpression(ABC):
    """An abstract base class and factory for scalar, non-boolean column
    expressions.

    `ColumnExpression` inheritance is closed to the types already provided by
    this package, but considerable custom behavior can still be provided via
    the `ColumnFunction` class and an `Engine` that knows how to interpret its
    `~ColumnFunction.name` value.  These concrete types can all be constructed
    via factory methods on `ColumnExpression` itself, so the derived types
    themselves only need to be referenced when writing `match` expressions that
    process an expression tree.  See
    :ref:`lsst.daf.relation-overview-extensibility` for rationale and details.
    """

    def __init_subclass__(cls) -> None:
        assert cls.__name__ in {
            "ColumnLiteral",
            "ColumnReference",
            "ColumnFunction",
        }, "ColumnExpression inheritance is closed to predefined types in daf_relation."

    dtype: type | None
    """The Python type this expression evaluates to (`type` or `None`).

    Interpretation of this attribute is up to the `Engine` or other algorithms
    that operate on the expression tree; it is ignored by all code in the
    `lsst.daf.relation` package.
    """

    @property
    @abstractmethod
    def columns_required(self) -> Set[ColumnTag]:
        """Columns required by this expression
        (`~collections.abc.Set` [ `ColumnTag` ]).

        This includes columns required by expressions nested within this one.
        """
        raise NotImplementedError()

    @abstractmethod
    def is_supported_by(self, engine: Engine) -> bool:
        """Test whether the given engine is capable of evaluating this
        expression.

        Parameters
        ----------
        engine : `Engine`
            Engine to test.

        Returns
        -------
        supported : `bool`
            Whether the engine supports this expression and all expressions
            nested within it.
        """
        raise NotImplementedError()

    @classmethod
    def literal(cls, value: Any, dtype: type | None = None) -> ColumnLiteral:
        """Construct an expression backed by a regular Python object.

        Parameters
        ----------
        value
            Value for the expression.
        dtype : `type` or `None`, optional
            The Python type this expression evaluates to (`type` or `None`).

        Returns
        -------
        literal : `ColumnLiteral`
            A column expression backed by the given value.
        """
        return ColumnLiteral(value, dtype)

    @classmethod
    def reference(cls, tag: ColumnTag, dtype: type | None = None) -> ColumnReference:
        """Construct an expression that refers to a column in a relation.

        Parameters
        ----------
        tag : `ColumnTag`
            Identifier for the column to reference.
        dtype : `type` or `None`, optional
            The Python type this expression evaluates to (`type` or `None`).

        Returns
        -------
        reference : `ColumnReference`
            A column expression that refers the given relation column.
        """
        return ColumnReference(tag, dtype)

    def method(
        self,
        name: str,
        *args: ColumnExpression,
        dtype: type | None = None,
        supporting_engine_types: Iterable[type[Engine]] | None = None,
    ) -> ColumnFunction:
        """Construct an expression that represents a method call with
        expression arguments.

        Parameters
        ----------
        name : `str`
            Name of the method, to be interpreted by the `Engine` or other
            algorithm.
        *args : `ColumnExpression`
            Expressions to pass as arguments to the method (after ``self``).
        dtype : `type` or `None`, optional
            The Python type this expression evaluates to (`type` or `None`).
        supporting_engine_types : `~collections.abc.Iterable` [ `type` ], \
                optional
            If provided, the set of `Engine` types that are expected to support
            this expression.  If `None` (default), all engines are assumed to
            support it.

        Returns
        -------
        function : `ColumnFunction`
            Column expression that represents this function call.

        Notes
        -----
        `ColumnExpression` cannot actually force an engine to interpret the
        given name as the name of a method rather than something else; calling
        this method like this::

            a.method("name", b)

        is exactly equivalent to::

            ColumnExpression.function("name", a, b)

        The provided `iteration` and `sql` engines both interpret these names
        as method names if and only if they are not first found in the
        built-in `operator` module.
        """
        return self.function(
            name,
            self,
            *args,
            dtype=dtype,
            supporting_engine_types=(
                tuple(supporting_engine_types) if supporting_engine_types is not None else None
            ),
        )

    @classmethod
    def function(
        cls,
        name: str,
        *args: ColumnExpression,
        dtype: type | None = None,
        supporting_engine_types: Iterable[type[Engine]] | None = None,
    ) -> ColumnFunction:
        """Construct an expression that represents a function call with
        expression arguments.

        Parameters
        ----------
        name : `str`
            Name of the method, to be interpreted by the `Engine` or other
            algorithm.
        *args : `ColumnExpression`
            Expressions to pass as arguments to the method (not including
            ``self``; this is a `classmethod`, so it never has access to
            ``self``).
        dtype : `type` or `None`, optional
            The Python type this expression evaluates to (`type` or `None`).
        supporting_engine_types : `~collections.abc.Iterable` [ `type` ], \
                optional
            If provided, the set of `Engine` types that are expected to support
            this expression.  If `None` (default), all engines are assumed to
            support it.

        Returns
        -------
        function : `ColumnFunction`
            Column expression that represents this function call.
        """
        return ColumnFunction(
            name,
            args,
            dtype,
            supporting_engine_types=(
                tuple(supporting_engine_types) if supporting_engine_types is not None else None
            ),
        )

    def eq(self, other: ColumnExpression) -> PredicateFunction:
        """Construct a boolean equality-comparison expression.

        Parameters
        ----------
        other : `ColumnExpression`
            Expression whose value will be compared to that of ``self``.

        Returns
        -------
        comparison : `Predicate`
            Boolean column expression.
        """
        return self.predicate_method("__eq__", other)

    def ne(self, other: ColumnExpression) -> PredicateFunction:
        """Construct a boolean inequality-comparison expression.

        Parameters
        ----------
        other : `ColumnExpression`
            Expression whose value will be compared to that of ``self``.

        Returns
        -------
        comparison : `Predicate`
            Boolean column expression.
        """
        return self.predicate_method("__ne__", other)

    def lt(self, other: ColumnExpression) -> PredicateFunction:
        """Construct a boolean less-than-comparison expression.

        Parameters
        ----------
        other : `ColumnExpression`
            Expression whose value will be compared to that of ``self``.

        Returns
        -------
        comparison : `Predicate`
            Boolean column expression.
        """
        return self.predicate_method("__lt__", other)

    def gt(self, other: ColumnExpression) -> PredicateFunction:
        """Construct a boolean greater-than-comparison expression.

        Parameters
        ----------
        other : `ColumnExpression`
            Expression whose value will be compared to that of ``self``.

        Returns
        -------
        comparison : `Predicate`
            Boolean column expression.
        """
        return self.predicate_method("__gt__", other)

    def le(self, other: ColumnExpression) -> PredicateFunction:
        """Construct a boolean less-or-equal-comparison expression.

        Parameters
        ----------
        other : `ColumnExpression`
            Expression whose value will be compared to that of ``self``.

        Returns
        -------
        comparison : `Predicate`
            Boolean column expression.
        """
        return self.predicate_method("__le__", other)

    def ge(self, other: ColumnExpression) -> PredicateFunction:
        """Construct a boolean greater-or-equal-comparison expression.

        Parameters
        ----------
        other : `ColumnExpression`
            Expression whose value will be compared to that of ``self``.

        Returns
        -------
        comparison : `Predicate`
            Boolean column expression.
        """
        return self.predicate_method("__ge__", other)

    def predicate_method(
        self,
        name: str,
        *args: ColumnExpression,
        supporting_engine_types: Set[type[Engine]] | None = None,
    ) -> PredicateFunction:
        """Construct an expression that represents a method call with
        expression arguments and a boolean result.

        Parameters
        ----------
        name : `str`
            Name of the method, to be interpreted by the `Engine` or other
            algorithm.
        *args : `ColumnExpression`
            Expressions to pass as arguments to the method (after ``self``).
        dtype : `type` or `None`, optional
            The Python type this expression evaluates to (`type` or `None`).
        supporting_engine_types : `~collections.abc.Iterable` [ `type` ] \
                optional
            If provided, the set of `Engine` types that are expected to support
            this expression.  If `None` (default), all engines are assumed to
            support it.

        Returns
        -------
        function : `PredicateFunction`
            Boolean column expression that represents this function call.

        Notes
        -----
        `ColumnExpression` cannot actually force an engine to interpret the
        given name as the name of a method rather than something else; calling
        this method like this::

            a.predicate_method("name", b)

        is exactly equivalent to::

            ColumnExpression.predicate_function("name", a, b)

        The provided `iteration` and `sql` engines both interpret these names
        as method names if and only if they are not first found in the
        built-in `operator` module.
        """
        return self.predicate_function(
            name,
            self,
            *args,
            supporting_engine_types=(
                tuple(supporting_engine_types) if supporting_engine_types is not None else None
            ),
        )

    @classmethod
    def predicate_function(
        cls,
        name: str,
        *args: ColumnExpression,
        supporting_engine_types: Iterable[type[Engine]] | None = None,
    ) -> PredicateFunction:
        """Construct an expression that represents a function call with
        expression arguments and a boolean result.

        Parameters
        ----------
        name : `str`
            Name of the method, to be interpreted by the `Engine` or other
            algorithm.
        *args : `ColumnExpression`
            Expressions to pass as arguments to the method (not including
            ``self``; this is a `classmethod`, so it never has access to
            ``self``).
        dtype : `type` or `None`, optional
            The Python type this expression evaluates to (`type` or `None`).
        supporting_engine_types : `~collections.abc.Iterable` [ `type` ], \
                optional
            If provided, the set of `Engine` types that are expected to support
            this expression.  If `None` (default), all engines are assumed to
            support it.

        Returns
        -------
        function : `PredicateFunction`
            Boolean column expression that represents this function call.
        """
        return PredicateFunction(
            name,
            args,
            supporting_engine_types=(
                tuple(supporting_engine_types) if supporting_engine_types is not None else None
            ),
        )


@dataclasses.dataclass(frozen=True)
class ColumnLiteral(ColumnExpression):
    """A concrete column expression backed by a regular Python value."""

    value: Any
    """Python value for the expression."""

    dtype: type | None
    """The Python type this expression evaluates to (`type` or `None`)."""

    @property
    def columns_required(self) -> Set[ColumnTag]:
        # Docstring inherited
        return frozenset()

    def __str__(self) -> str:
        return repr(self.value)

    def is_supported_by(self, engine: Engine) -> bool:
        # Docstring inherited
        return True


@dataclasses.dataclass(frozen=True)
class ColumnReference(ColumnExpression):
    """A concrete column expression that refers to a relation column."""

    tag: ColumnTag
    """Identifier for the column this expression refers to (`ColumnTag`)."""

    dtype: type | None
    """The Python type this expression evaluates to (`type` or `None`)."""

    @property
    def columns_required(self) -> Set[ColumnTag]:
        # Docstring inherited
        return {self.tag}

    def __str__(self) -> str:
        return str(self.tag)

    def is_supported_by(self, engine: Engine) -> bool:
        # Docstring inherited
        return True


_OPERATOR_STRINGS = {
    "__eq__": "=",
    "__ne__": "≠",
    "__lt__": "<",
    "__le__": "≤",
    "__gt__": ">",
    "__ge__": "≥",
}


@dataclasses.dataclass(frozen=True)
class ColumnFunction(ColumnExpression):
    """A concrete column expression that represents calling a named function
    with column expression arguments.
    """

    name: str
    """Name of the function to apply (`str`).

    Interpretation of this name is entirely up to the `Engine` or other
    relation-processing algorithm.
    """

    args: tuple[ColumnExpression, ...]
    """Column expressions to pass as arguments to the function
    (`tuple` [ `ColumnExpression`, ... ]).
    """

    dtype: type | None
    """The Python type this expression evaluates to (`type` or `None`)."""

    supporting_engine_types: tuple[type[Engine], ...] | None = dataclasses.field(compare=False)
    """The set of `Engine` types that are expected to support this expression
    (`tuple` [ `type` [ `Engine` ], ... ]).
    """

    def __post_init__(self) -> None:
        if not self.args:
            raise RelationalAlgebraError(f"No arguments for function {self.name}.")

    @property
    @cached_getter
    def columns_required(self) -> Set[ColumnTag]:
        # Docstring inherited.
        result: set[ColumnTag] = set()
        for arg in self.args:
            result.update(arg.columns_required)
        return result

    def __str__(self) -> str:
        return f"{self.name}({', '.join(str(a) for a in self.args)})"

    def is_supported_by(self, engine: Engine) -> bool:
        # Docstring inherited.
        return (
            self.supporting_engine_types is None or isinstance(engine, self.supporting_engine_types)
        ) and all(arg.is_supported_by(engine) for arg in self.args)


@dataclasses.dataclass(frozen=True)
class PredicateFunction(Predicate):
    """A concrete boolean expression that represents calling an named function
    with column expression arguments.
    """

    name: str
    """Name of the function to apply (`str`).

    Interpretation of this name is entirely up to the `Engine` or other
    relation-processing algorithm.
    """

    args: tuple[ColumnExpression, ...]
    """Column expressions to pass as arguments to the function
    (`tuple` [ `ColumnExpression`, ... ]).
    """

    supporting_engine_types: tuple[type[Engine], ...] | None = dataclasses.field(compare=False)
    """The set of `Engine` types that are expected to support this expression
    (`tuple` [ `type` [ `Engine` ], ... ]).
    """

    def __post_init__(self) -> None:
        if not self.args:
            raise RelationalAlgebraError(f"No arguments for predicate function {self.name}.")

    @property
    @cached_getter
    def columns_required(self) -> Set[ColumnTag]:
        # Docstring inherited.
        result: set[ColumnTag] = set()
        for arg in self.args:
            result.update(arg.columns_required)
        return result

    def __str__(self) -> str:
        if (op_str := _OPERATOR_STRINGS.get(self.name)) is not None:
            return f"{self.args[0]}{op_str}{self.args[1]}"
        else:
            return f"{self.name}({', '.join(str(a) for a in self.args)})"

    def is_supported_by(self, engine: Engine) -> bool:
        # Docstring inherited.
        return (
            self.supporting_engine_types is None or isinstance(engine, self.supporting_engine_types)
        ) and all(arg.is_supported_by(engine) for arg in self.args)

    def as_trivial(self) -> None:
        # Docstring inherited.
        return None
