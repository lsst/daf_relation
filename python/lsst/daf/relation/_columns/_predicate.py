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
    "flatten_logical_and",
    "LogicalNot",
    "LogicalAnd",
    "LogicalOr",
    "Predicate",
    "PredicateLiteral",
    "PredicateReference",
)

import dataclasses
from abc import ABC, abstractmethod
from collections.abc import Set
from typing import TYPE_CHECKING, ClassVar, Literal

from lsst.utils.classes import cached_getter

from ._tag import ColumnTag

if TYPE_CHECKING:
    from .._engine import Engine


class Predicate(ABC):
    """An abstract base class and factory for boolean column expressions.

    `Predicate` inheritance is closed to the types already provided by this
    package, but considerable custom behavior can still be provided via the
    `PredicateFunction` class and an `Engine` that knows how to interpret its
    `~PredicateFunction.name` value.  These concrete types can all be
    constructed via factory methods on `Predicate` itself, `ColumnExpression`,
    or `ColumnContainer`, so the derived types themselves only need to be
    referenced when writing `match` expressions that process an expression
    tree.  See :ref:`lsst.daf.relation-overview-extensibility` for rationale
    and details.
    """

    def __init_subclass__(cls) -> None:
        assert cls.__name__ in {
            "PredicateLiteral",
            "PredicateReference",
            "PredicateFunction",
            "ColumnInContainer",
            "LogicalNot",
            "LogicalAnd",
            "LogicalOr",
        }, "Predicate inheritance is closed to predefined types in daf_relation."

    dtype: ClassVar[type[bool]] = bool
    """The Python type this expression evaluates to (`type` [ `bool` ]).
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
    def literal(cls, value: bool) -> PredicateLiteral:
        """Construct a boolean expression that is a constant `True` or `False`.

        Parameters
        ----------
        value : `bool`
            Value for the expression.

        Returns
        -------
        literal : `PredicateLiteral`
            A boolean column expression set to the given value.
        """
        return PredicateLiteral(value)

    @classmethod
    def reference(cls, tag: ColumnTag) -> PredicateReference:
        """Construct an expression that refers to a boolean column in a
        relation.

        Parameters
        ----------
        tag : `ColumnTag`
            Identifier for the column to reference.

        Returns
        -------
        reference : `PredicateReference`
            A column expression that refers the given relation column.
        """
        return PredicateReference(tag)

    def logical_not(self) -> LogicalNot:
        """Return a boolean expression that is the logical NOT of this one.

        Returns
        -------
        logical_not : `Predicate`
            Logical NOT expression.
        """
        return LogicalNot(self)

    def logical_and(*operands: Predicate) -> Predicate:
        """Return a boolean expression that is the logical AND of the given
        ones.

        Parameters
        ----------
        *operands : `Predicate`
            Existing boolean expressions to AND together.

        Returns
        -------
        logical_and : `Predicate`
            Logical AND expression.  If no operands are provided, a
            `PredicateLiteral` for `True` is returned.  If one operand is
            provided, it is returned directly.
        """
        if not operands:
            return Predicate.literal(True)
        elif len(operands) == 1:
            return operands[0]
        return LogicalAnd(operands)

    def logical_or(*operands: Predicate) -> Predicate:
        """Return a boolean expression that is the logical OR of the given
        ones.

        Parameters
        ----------
        *operands : `Predicate`
            Existing boolean expressions to OR together.

        Returns
        -------
        logical_and : `Predicate`
            Logical OR expression.  If no operands are provided, a
            `PredicateLiteral` for `False` is returned.  If one operand is
            provided, it is returned directly.
        """
        if not operands:
            return Predicate.literal(False)
        elif len(operands) == 1:
            return operands[0]
        return LogicalOr(operands)

    @abstractmethod
    def as_trivial(self) -> bool | None:
        """Attempt to simplify this expression into a constant boolean.

        Returns
        -------
        trivial : `bool` or `None`
            If `True` or `False`, the expression always evaluates to exactly
            that constant value.  If `None`, the expression is nontrivial (or
            at least could not easily be simplified into a trivial expression).
        """
        return None


@dataclasses.dataclass(frozen=True)
class PredicateLiteral(Predicate):
    """A concrete boolean column expression that is a constant `True` or
    `False`.
    """

    value: bool
    """Constant value for the expression (`bool`)."""

    @property
    def columns_required(self) -> Set[ColumnTag]:
        # Docstring inherited.
        return frozenset()

    def __str__(self) -> str:
        return str(self.value)

    def is_supported_by(self, engine: Engine) -> bool:
        # Docstring inherited.
        return True

    def as_trivial(self) -> bool:
        # Docstring inherited.
        return self.value


@dataclasses.dataclass(frozen=True)
class PredicateReference(Predicate):
    """A concrete boolean column expression that refers to a boolean relation
    column.
    """

    tag: ColumnTag
    """Identifier for the column this expression refers to (`ColumnTag`)."""

    @property
    def columns_required(self) -> Set[ColumnTag]:
        # Docstring inherited.
        return {self.tag}

    def __str__(self) -> str:
        return str(self.tag)

    def is_supported_by(self, engine: Engine) -> bool:
        # Docstring inherited.
        return True

    def as_trivial(self) -> None:
        # Docstring inherited.
        return None


@dataclasses.dataclass(frozen=True)
class LogicalNot(Predicate):
    """A concrete boolean column expression that inverts its operand."""

    operand: Predicate
    """Boolean expression to invert (`Predicate`).
    """

    @property
    def columns_required(self) -> Set[ColumnTag]:
        # Docstring inherited.
        return self.operand.columns_required

    def __str__(self) -> str:
        return f"not ({self.operand!s})"

    def is_supported_by(self, engine: Engine) -> bool:
        # Docstring inherited.
        return self.operand.is_supported_by(engine)

    def as_trivial(self) -> bool | None:
        # Docstring inherited.
        if (operand_as_trivial := self.operand.as_trivial()) is None:
            return None
        return not operand_as_trivial


@dataclasses.dataclass(frozen=True)
class LogicalAnd(Predicate):
    """A concrete boolean column expression that ANDs its operands."""

    operands: tuple[Predicate, ...]
    """Boolean expressions to combine (`tuple` [ `Predicate`, ... ])."""

    @property
    @cached_getter
    def columns_required(self) -> Set[ColumnTag]:
        # Docstring inherited.
        result: set[ColumnTag] = set()
        for operand in self.operands:
            result.update(operand.columns_required)
        return result

    def __str__(self) -> str:
        return " and ".join(
            str(operand) if not isinstance(operand, LogicalOr) else f"({operand!s})"
            for operand in self.operands
        )

    def is_supported_by(self, engine: Engine) -> bool:
        # Docstring inherited.
        return all(operand.is_supported_by(engine) for operand in self.operands)

    def as_trivial(self) -> bool | None:
        # Docstring inherited.
        result: bool | None = True
        for operand in self.operands:
            if (operand_as_trivial := operand.as_trivial()) is False:
                return False
            elif operand_as_trivial is None:
                result = None
        return result


@dataclasses.dataclass(frozen=True)
class LogicalOr(Predicate):
    """A concrete boolean column expression that ORs its operands."""

    operands: tuple[Predicate, ...]
    """Boolean expressions to combine (`tuple` [ `Predicate`, ... ])."""

    def __str__(self) -> str:
        return " or ".join(str(operand) for operand in self.operands)

    @property
    @cached_getter
    def columns_required(self) -> Set[ColumnTag]:
        # Docstring inherited.
        result: set[ColumnTag] = set()
        for operand in self.operands:
            result.update(operand.columns_required)
        return result

    def is_supported_by(self, engine: Engine) -> bool:
        # Docstring inherited.
        return all(operand.is_supported_by(engine) for operand in self.operands)

    def as_trivial(self) -> bool | None:
        # Docstring inherited.
        result: bool | None = False
        for operand in self.operands:
            if (operand_as_trivial := operand.as_trivial()) is True:
                return True
            elif operand_as_trivial is None:
                result = None
        return result


def flatten_logical_and(predicate: Predicate) -> list[Predicate] | Literal[False]:
    """Flatten all logical AND operations in predicate into a `list`.

    Parameters
    ----------
    predicate : `Predicate`
        Original expression to flatten.

    Returns
    -------
    flat : `list` [ `Predicate` ] or `False`
        A list of predicates that could be combined with AND to reproduce the
        original expression, or `False` if the predicate is
        `trivially false <is_trivial>`.

    Notes
    -----
    This algorithm is not guaranteed to descend into nested OR or NOT
    operations, but it does descend into nested AND operations.
    """
    match predicate:
        case LogicalAnd(operands=operands):
            result: list[Predicate] = []
            for operand in operands:
                if (nested_result := flatten_logical_and(operand)) is False:
                    return False
                result.extend(nested_result)
            return result
        case PredicateLiteral(value=value):
            if value:
                return []
            else:
                return False
    return [predicate]
