# This file is part of daf_relation.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
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
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from __future__ import annotations

__all__ = (
    "UnaryOperationRelation",
    "BinaryOperationRelation",
)

import dataclasses
from collections.abc import Set
from typing import TYPE_CHECKING, Any, Generic, TypeVar, final

from ._leaf_relation import LeafRelation
from ._relation import BaseRelation, Relation

if TYPE_CHECKING:
    from ._binary_operation import BinaryOperation
    from ._columns import ColumnTag
    from ._engine import Engine
    from ._unary_operation import UnaryOperation


_U = TypeVar("_U", bound="UnaryOperation")
_B = TypeVar("_B", bound="BinaryOperation")


@final
@dataclasses.dataclass(frozen=True)
class UnaryOperationRelation(BaseRelation, Generic[_U]):
    """A concrete `Relation` that represents the action of a `UnaryOperation`
    on a target `Relation`.

    `UnaryOperationRelation` instances must only be constructed via calls to
    `UnaryOperation.apply` or `Relation` convenience methods.  Direct calls to
    the constructor are not guaranteed to satisfy invariants imposed by the
    operations classes.
    """

    operation: _U
    """The unary operation whose action this relation represents
    (`UnaryOperation`).
    """

    target: Relation
    """The target relation the operation acts upon (`Relation`).
    """

    columns: Set[ColumnTag] = dataclasses.field(repr=False, compare=False)
    """The columns in this relation (`~collections.abc.Set` [ `ColumnTag` ] ).
    """

    payload: Any = dataclasses.field(repr=False, compare=False, default=None)
    """The engine-specific contents of the relation.

    This will always be `None` for most operations, with `Materialization`
    operations a notable exception (and the only exception in
    `lsst.daf.relation` itself).
    """

    is_locked: bool = dataclasses.field(repr=False, compare=False, default=False)
    """Whether this relation and those upstream of it should be considered
    fixed by tree-manipulation algorithms (`bool`).

    Most operation-based relations default to unlocked but can be explicitly
    locked when created, indicating that algorithms can consider inserting new
    operations into the subtree, either as a way to intentionally change the
    tree's behavior or to reorder operations in a way consistent with
    commutation relations to aid in the tree's evaluation.
    """

    @property
    def engine(self) -> Engine:
        """The engine that is responsible for interpreting this relation
        (`Engine`).
        """
        return self.operation.applied_engine(self.target)

    @property
    def min_rows(self) -> int:
        """The minimum number of rows this relation might have (`int`)."""
        return self.operation.applied_min_rows(self.target)

    @property
    def max_rows(self) -> int | None:
        """The maximum number of rows this relation might have (`int` or
        `None`).

        This is `None` for relations whose size is not bounded from above.
        """
        return self.operation.applied_max_rows(self.target)

    def __str__(self) -> str:
        return f"{self.operation!s}({self.target!s})"


@final
@dataclasses.dataclass(frozen=True)
class BinaryOperationRelation(BaseRelation, Generic[_B]):
    """A concrete `Relation` that represents the action of a `BinaryOperation`
    on a pair of target `Relation` objects.

    `BinaryOperationRelation` instances must only be constructed via calls to
    `BinaryOperation.apply` or `Relation` convenience methods.  Direct calls to
    the constructor are not guaranteed to satisfy invariants imposed by the
    operations classes.
    """

    operation: _B
    """The binary operation whose action this relation represents
    (`BinaryOperation`).
    """

    lhs: Relation
    """One target relation the operation acts upon (`Relation`).
    """

    rhs: Relation
    """The other target relation the operation acts upon (`Relation`).
    """

    columns: Set[ColumnTag] = dataclasses.field(repr=False, compare=False)
    """The columns in this relation (`~collections.abc.Set` [ `ColumnTag` ] ).
    """

    is_locked: bool = dataclasses.field(repr=False, compare=False, default=False)
    """Whether this relation and those upstream of it should be considered
    fixed by tree-manipulation algorithms (`bool`).

    Most operation-based relations default to unlocked but can be explicitly
    locked when created, indicating that algorithms can consider inserting new
    operations into the subtree, either as a way to intentionally change the
    tree's behavior or to reorder operations in a way consistent with
    commutation relations to aid in the tree's evaluation.
    """

    @property
    def engine(self) -> Engine:
        """The engine that is responsible for interpreting this relation
        (`Engine`).
        """
        return self.operation.applied_engine(self.lhs, self.rhs)

    @property
    def payload(self) -> None:
        """The engine-specific contents of the relation.

        This is always `None` for binary operation relations.
        """
        return None

    @property
    def min_rows(self) -> int:
        """The minimum number of rows this relation might have (`int`)."""
        return self.operation.applied_min_rows(self.lhs, self.rhs)

    @property
    def max_rows(self) -> int | None:
        """The maximum number of rows this relation might have (`int` or
        `None`).

        This is `None` for relations whose size is not bounded from above.
        """
        return self.operation.applied_max_rows(self.lhs, self.rhs)

    def __str__(self) -> str:
        lhs_str = f"({self.lhs!s})"
        match self.lhs:
            case LeafRelation():
                lhs_str = str(self.lhs)
            case BinaryOperationRelation(operation=lhs_operation):
                if type(lhs_operation) is type(self.operation):  # noqa: E721
                    lhs_str = str(self.lhs)
        rhs_str = f"({self.rhs!s})"
        match self.rhs:
            case LeafRelation():
                rhs_str = str(self.rhs)
            case BinaryOperationRelation(operation=rhs_operation):
                if type(rhs_operation) is type(self.operation):  # noqa: E721
                    rhs_str = str(self.rhs)
        return f"{lhs_str} {self.operation!s} {rhs_str}"
