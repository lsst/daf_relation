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

__all__ = ("BinaryOperation", "IgnoreOne")

import dataclasses
from abc import ABC, abstractmethod
from collections.abc import Set
from typing import final

from ._columns import ColumnTag
from ._relation import Relation


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
            "IgnoreOne",
        }, "BinaryOperation inheritance is closed to predefined types in daf_relation."

    @final
    def apply(self, lhs: Relation, rhs: Relation) -> Relation:
        """Create a new relation that represents the action of this operation
        on a pair of existing relations.

        Parameters
        ----------
        lhs : `Relation`
            One relation the operation will act on.
        rhs : `Relation`
            The other relation the operation will act on.

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
        """
        operation = self._begin_apply(lhs, rhs)
        return lhs.engine.append_binary(operation, lhs, rhs)

    def _begin_apply(self, lhs: Relation, rhs: Relation) -> BinaryOperation:
        """A customization hook for the beginning of operation application.

        Parameters
        ----------
        lhs : `Relation`
            One relation the operation should act on.
        rhs : `Relation`
            The other relation the operation should act on.

        Returns
        -------
        operation : `BinaryOperation`
            The operation to actually apply.  The default implementation
            returns ``self``.

        Notes
        -----
        This method provides an opportunity for operations to establish any
        invariants that must be satisfied only when the operation is part of
        a relation.
        """  # noqa: D401
        return self

    def _finish_apply(self, lhs: Relation, rhs: Relation) -> Relation:
        """A customization hook for the end of operation application.

        Parameters
        ----------
        target : `Relation`
            Relation the operation will act upon directly.

        Returns
        -------
        applied : `Relation`
            Result of applying this operation to the given target.  Usually -
            but not always - a `UnaryOperationRelation` that holds ``self`` and
            ``target``.

        Notes
        -----
        This method provides an opportunity for operations to change the kind
        of relation produced (the default implementation constructs a
        `BinaryOperationRelation`).
        """  # noqa: D401
        from ._operation_relations import BinaryOperationRelation

        return BinaryOperationRelation(
            operation=self,
            lhs=lhs,
            rhs=rhs,
            columns=self.applied_columns(lhs, rhs),
        )

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


@dataclasses.dataclass
class IgnoreOne(BinaryOperation):
    """A binary operation that passes through one of its operands and ignores
    the other.
    """

    ignore_lhs: bool
    """Whether the ignored operand is the left-hand-side one (`bool`).
    """

    def applied_columns(self, lhs: Relation, rhs: Relation) -> Set[ColumnTag]:
        # Docstring inherited.
        return self._finish_apply(lhs, rhs).columns

    def applied_min_rows(self, lhs: Relation, rhs: Relation) -> int:
        # Docstring inherited.
        return self._finish_apply(lhs, rhs).min_rows

    def applied_max_rows(self, lhs: Relation, rhs: Relation) -> int | None:
        # Docstring inherited.
        return self._finish_apply(lhs, rhs).max_rows

    def _finish_apply(self, lhs: Relation, rhs: Relation) -> Relation:
        # Docstring inherited.
        if self.ignore_lhs:
            return rhs
        else:
            return lhs
