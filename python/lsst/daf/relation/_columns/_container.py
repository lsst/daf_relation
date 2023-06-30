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
    "ColumnContainer",
    "ColumnRangeLiteral",
    "ColumnExpressionSequence",
    "ColumnInContainer",
)

import dataclasses
from abc import ABC, abstractmethod
from collections.abc import Sequence, Set
from typing import TYPE_CHECKING

from lsst.utils.classes import cached_getter

from ._predicate import Predicate
from ._tag import ColumnTag

if TYPE_CHECKING:
    from .._engine import Engine
    from ._expression import ColumnExpression


class ColumnContainer(ABC):
    """A abstract base class and factory for expressions that represent
    containers of multiple column values.

    `ColumnContainer` inheritance is closed to the types already provided by
    this package.  These concrete types can all be constructed via factory
    methods on `ColumnContainer` itself, so the derived types themselves only
    need to be referenced when writing `match` expressions that process an
    expression tree.  See :ref:`lsst.daf.relation-overview-extensibility` for
    rationale and details.
    """

    def __init_subclass__(cls) -> None:
        assert cls.__name__ in {
            "ColumnRangeLiteral",
            "ColumnExpressionSequence",
        }, "ColumnContainer inheritance is closed to predefined types in daf_relation."

    dtype: type | None
    """The Python type of the elements in the container (`type` or `None`).

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

    def contains(self, item: ColumnExpression) -> ColumnInContainer:
        """Construct a boolean column expression that tests whether a scalar
        expression is present in this container expression.

        Parameters
        ----------
        item : `ColumnExpression`
            Item expression to test.

        Returns
        -------
        contains : `ColumnInContainer`
            Boolean column expression that tests for membership in the
            container.
        """
        return ColumnInContainer(item, self)

    @classmethod
    def range_literal(cls, r: range) -> ColumnRangeLiteral:
        """Construct a container expression from a range of indices.

        Parameters
        ----------
        r : `range`
            Range object.

        Returns
        -------
        container : `ColumnRangeLiteral`
            Container expression object representing the range.
        """
        return ColumnRangeLiteral(r)

    @classmethod
    def sequence(
        cls, items: Sequence[ColumnExpression], dtype: type | None = None
    ) -> ColumnExpressionSequence:
        """Construct a container expression from a sequence of item
        expressions.

        Parameters
        ----------
        items : `~collections.abc.Sequence` [ `ColumnExpression` ]
            Sequence of item expressions.
        dtype : `type`, optional
            The Python type of the elements in the container.

        Returns
        -------
        container : `ColumnExpressionSequence`
            Container expression object backed by the given items.
        """
        return ColumnExpressionSequence(items, dtype)


@dataclasses.dataclass(frozen=True)
class ColumnRangeLiteral(ColumnContainer):
    """A container expression backed by a range of integer indices."""

    value: range
    """Range value that backs the expression (`range`).
    """

    dtype: type[int] = int
    """The Python type of the elements in the container (`type` [ `int` ] )."""

    @property
    def columns_required(self) -> Set[ColumnTag]:
        # Docstring inherited.
        return frozenset()

    def __str__(self) -> str:
        return f"[{self.value.start}:{self.value.stop}:{self.value.step}]"

    def is_supported_by(self, engine: Engine) -> bool:
        # Docstring inherited.
        return True


@dataclasses.dataclass(frozen=True)
class ColumnExpressionSequence(ColumnContainer):
    """A container expression backed by a sequence of scalar column
    expressions.
    """

    items: Sequence[ColumnExpression]
    """Sequence of item expressions
    (`~collections.abc.Sequence` [ `ColumnExpression` ]).
    """

    dtype: type | None
    """The Python type of the elements in the container (`type` or `None`)."""

    @property
    @cached_getter
    def columns_required(self) -> Set[ColumnTag]:
        # Docstring inherited.
        result: set[ColumnTag] = set()
        for item in self.items:
            result.update(item.columns_required)
        return result

    def __str__(self) -> str:
        return f"[{', '.join(str(i) for i in self.items)}]"

    def is_supported_by(self, engine: Engine) -> bool:
        # Docstring inherited.
        return all(item.is_supported_by(engine) for item in self.items)


@dataclasses.dataclass(frozen=True)
class ColumnInContainer(Predicate):
    """A boolean column expression that tests whether a scalar column
    expression is present in a container expression.
    """

    item: ColumnExpression
    """Item to be tested (`ColumnExpression`)."""

    container: ColumnContainer
    """Container to be tested (`ColumnContainer`)."""

    @property
    @cached_getter
    def columns_required(self) -> Set[ColumnTag]:
        # Docstring inherited.
        return self.item.columns_required | self.container.columns_required

    def __str__(self) -> str:
        return f"{self.item}∈{self.container}"

    def is_supported_by(self, engine: Engine) -> bool:
        # Docstring inherited.
        return self.item.is_supported_by(engine) and self.container.is_supported_by(engine)

    def as_trivial(self) -> None:
        # Docstring inherited.
        return None
