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

__all__ = ("BinaryOperation",)

from abc import ABC, abstractmethod
from collections.abc import Set
from typing import TYPE_CHECKING

from ._columns import ColumnTag
from ._relation import Relation

if TYPE_CHECKING:
    from ._engine import Engine


class BinaryOperation(ABC):
    """An abstract base class for operations that act on a pair of relations.

    Notes
    -----
    A `BinaryOperation` represents the operation itself; the combination of an
    operation and the "lhs" and "rhs" relations it acts on to form a new
    relation is represented by the `BinaryOperationRelation` class, which
    should always be performed via a call to the `apply` method (or something
    that calls it, like the convenience methods on the `Relation` class).  In
    many cases, applying a `BinaryOperation` doesn't return something involving
    the original operation, because of some combination of defaulted-parameter
    population and simplification, and there are even some `BinaryOperation`
    classes that should never actually appear in a `BinaryOperationRelation`.

    `BinaryOperation` cannot be subclassed by external code.

    All concrete `BinaryOperation` types are frozen, equality-comparable
    `dataclasses`.  They also provide a very concise `str` representation (in
    addition to the dataclass-provided `repr`) suitable for summarizing an
    entire relation tree.

    See Also
    --------
    :ref:`lsst.daf.relation-overview-operations`
    """

    def __init_subclass__(cls) -> None:
        assert cls.__name__ in {
            "Join",
            "Chain",
        }, "BinaryOperation inheritance is closed to predefined types in daf_relation."

    @abstractmethod
    def apply(self, lhs: Relation, rhs: Relation, *, lock: bool) -> Relation:
        """Create a new relation that represents the action of this operation
        on a pair of existing relations.

        Parameters
        ----------
        lhs : `Relation`
            On relation the operation will act on.
        rhs : `Relation`
            The other relation the operation will act on.
        lock : `bool`, optional
            Set `Relation.is_locked` on the result to this value.

        Returns
        -------
        new_relation : `Relation`
            Relation that includes this operation.  This may be ``self`` if the
            operation is a no-op, and it may not be a `BinaryOperationRelation`
            holding this operation (or even a similar one) if the operation was
            inserted earlier in the tree via commutation relations.

        Raises
        ------
        ColumnError
            Raised if the operation could not be applied due to problems with
            the target relations' columns.
        EngineError
            Raised if the operation could not be applied due to problems with
            the target relations' engine(s).
        RowOrderError
            Raised if ``lhs`` or ``rhs`` is unnecessarily ordered; see
            `Relation.expect_unordered`.

        Notes
        -----
        Most concrete implementations support additional optional keyword
        arguments that provide more control over where the operation is
        inserted; see operation subclass documentation for details.
        """
        from ._operation_relations import BinaryOperationRelation

        return BinaryOperationRelation(
            operation=self,
            lhs=lhs,
            rhs=rhs,
            columns=self.applied_columns(lhs, rhs),
            is_locked=lock,
        )

    def applied_engine(self, lhs: Relation, rhs: Relation) -> Engine:
        """Return the engine of the relation that results from applying this
        operation to the given targets.

        Parameters
        ----------
        lhs : `Relation`
            On relation the operation will act on.
        rhs : `Relation`
            The other relation the operation will act on.

        Returns
        -------
        engine : `Engine`
            Engine a new relation would have.
        """
        return lhs.engine

    @abstractmethod
    def applied_columns(self, lhs: Relation, rhs: Relation) -> Set[ColumnTag]:
        """Return the columns of the relation that results from applying this
        operation to the given targets.

        Parameters
        ----------
        lhs : `Relation`
            On relation the operation will act on.
        rhs : `Relation`
            The other relation the operation will act on.

        Returns
        -------
        columns : `~collections.abc.Set` [ `ColumnTag` ]
            Columns the new relation would have.
        """
        raise NotImplementedError()

    @abstractmethod
    def applied_min_rows(self, lhs: Relation, rhs: Relation) -> int:
        """Return the minimum number of rows of the relation that results from
        applying this operation to the given targets.

        Parameters
        ----------
        lhs : `Relation`
            On relation the operation will act on.
        rhs : `Relation`
            The other relation the operation will act on.

        Returns
        -------
        min_rows : `int`
            Minimum number of rows the new relation would have.
        """
        raise NotImplementedError()

    @abstractmethod
    def applied_max_rows(self, lhs: Relation, rhs: Relation) -> int | None:
        """Return the maximum number of rows of the relation that results from
        applying this operation to the given target.

        Parameters
        ----------
        lhs : `Relation`
            On relation the operation will act on.
        rhs : `Relation`
            The other relation the operation will act on.

        Returns
        -------
        max_rows : `int` or `None`
            Maximum number of rows the new relation would have.
        """
        raise NotImplementedError()
