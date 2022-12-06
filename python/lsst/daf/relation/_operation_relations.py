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
from typing import TYPE_CHECKING, Literal, final

from ._leaf_relation import LeafRelation
from ._relation import BaseRelation, Relation

if TYPE_CHECKING:
    from ._binary_operation import BinaryOperation
    from ._columns import ColumnTag
    from ._engine import Engine
    from ._unary_operation import UnaryOperation


@dataclasses.dataclass(frozen=True)
class UnaryOperationRelation(BaseRelation):
    """A concrete `Relation` that represents the action of a `UnaryOperation`
    on a target `Relation`.

    `UnaryOperationRelation` instances must only be constructed via calls to
    `UnaryOperation.apply` or `Relation` convenience methods.  Direct calls to
    the constructor are not guaranteed to satisfy invariants imposed by the
    operations classes.
    """

    operation: UnaryOperation
    """The unary operation whose action this relation represents
    (`UnaryOperation`).
    """

    target: Relation
    """The target relation the operation acts upon (`Relation`).
    """

    columns: Set[ColumnTag] = dataclasses.field(repr=False, compare=False)
    """The columns in this relation (`~collections.abc.Set` [ `ColumnTag` ] ).
    """

    @property
    def payload(self) -> None:
        """The engine-specific contents of the relation.

        This is always `None` for binary operation relations.
        """
        return None

    @property
    def is_locked(self) -> Literal[False]:
        """Whether this relation and those upstream of it should be considered
        fixed by tree-manipulation algorithms (`bool`).
        """
        return False

    @property
    def engine(self) -> Engine:
        """The engine that is responsible for interpreting this relation
        (`Engine`).
        """
        return self.target.engine

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

    def reapply(self, target: Relation) -> Relation:
        """Reapply this relation's operation with a possibly-new target.

        Parameters
        ----------
        target : `Relation`
            Possibly-new target.

        Returns
        -------
        relation : `Relation`
            Relation that applies `operation` to the new target.  Will be
            ``self`` if ``target is self.target`` (this avoidance of an
            unnecessary new `UnaryOperationRelation` instance is the main
            reason for this convenience method's existence).
        """
        if target is self.target:
            return self
        else:
            return self.operation.apply(target)


@final
@dataclasses.dataclass(frozen=True)
class BinaryOperationRelation(BaseRelation):
    """A concrete `Relation` that represents the action of a `BinaryOperation`
    on a pair of target `Relation` objects.

    `BinaryOperationRelation` instances must only be constructed via calls to
    `BinaryOperation.apply` or `Relation` convenience methods.  Direct calls to
    the constructor are not guaranteed to satisfy invariants imposed by the
    operations classes.
    """

    operation: BinaryOperation
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

    @property
    def is_locked(self) -> Literal[False]:
        """Whether this relation and those upstream of it should be considered
        fixed by tree-manipulation algorithms (`bool`).
        """
        return False

    @property
    def engine(self) -> Engine:
        """The engine that is responsible for interpreting this relation
        (`Engine`).
        """
        return self.lhs.engine

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

    def reapply(self, lhs: Relation, rhs: Relation) -> Relation:
        """Reapply this relation's operation with possibly-new targets.

        Parameters
        ----------
        lhs : `Relation`
            One possibly-new target.
        rhs : `Relation`
            The other possibly-new target.

        Returns
        -------
        relation : `Relation`
            Relation that applies `operation` to the new targets.  Will be
            ``self`` if ``lhs is self.lhs and rhs is self.rhs`` (this avoidance
            of an unnecessary new `BinaryOperationRelation` instance is the
            main reason for this convenience method's existence).
        """
        if lhs is self.lhs and rhs is self.rhs:
            return self
        else:
            return self.operation.apply(lhs, rhs)
