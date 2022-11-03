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

__all__ = ("UnaryOperation", "Marker", "RowFilter", "Reordering", "Identity")

from abc import ABC, abstractmethod
from collections.abc import Set
from typing import TYPE_CHECKING, Literal, final

from ._columns import ColumnTag
from ._relation import Relation

if TYPE_CHECKING:
    from ._engine import Engine


class UnaryOperation(ABC):
    """An abstract base class for operations that act on a single relation.

    Notes
    -----
    A `UnaryOperation` represents the operation itself; the combination of an
    operation and the "target" relation it acts on to form a new relation is
    represented by the `UnaryOperationRelation` class, which should always be
    performed via a call to the `apply` method (or something that calls it,
    like the convenience methods on the `Relation` class).  In many cases,
    applying a `UnaryOperation` doesn't return something involving the original
    operation, because of some combination of defaulted-parameter population
    and simplification, and there are even some `UnaryOperation` classes that
    should never actually appear in a `UnaryOperationRelation`.

    `UnaryOperation` cannot be subclassed directly by external code, but it has
    three more restricted subclasses that can be: `Marker`, `RowFilter`, and
    `Reordering`.

    All concrete `UnaryOpeation` types are frozen, equality-comparable
    `dataclasses`.  They also provide a very concise `str` representation (in
    addition to the dataclass-provided `repr`) suitable for summarizing an
    entire relation tree.

    See Also
    --------
    :ref:`lsst.daf.relation-overview-operations`
    """

    def __init_subclass__(cls) -> None:
        assert (
            cls.__name__
            in {
                "Calculation",
                "Deduplication",
                "Identity",
                "Marker",
                "PartialJoin",
                "Projection",
                "RowFilter",
                "Reordering",
            }
            or cls.__base__ is not UnaryOperation
        ), (
            "UnaryOperation inheritance is closed to predefined types in daf_relation, "
            "except for subclasses of RowFilter, Marker, and Reordering."
        )

    @property
    def columns_required(self) -> Set[ColumnTag]:
        """The columns the target relation must have in order for this
        operation to be applied to it (`~collections.abc.Set` [ `ColumnTag` ]
        ).
        """
        return frozenset()

    @abstractmethod
    def __str__(self) -> str:
        raise NotImplementedError()

    @property
    @abstractmethod
    def is_empty_invariant(self) -> bool:
        """Whether this operation can remove all rows from its target relation
        (`bool`).
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def is_count_invariant(self) -> bool:
        """Whether this operation can change the number of rows in its target
        relation (`bool`).

        The number of rows here includes duplicates - removing duplicates is
        not considered a count-invariant operation.
        """
        raise NotImplementedError()

    @property
    def is_order_dependent(self) -> bool:
        """Whether this operation depends on the order of the rows in its
        target relation (`bool`).
        """
        return False

    @property
    def is_count_dependent(self) -> bool:
        """Whether this operation depends on the number of rows in its target
        relation (`bool`).
        """
        return False

    @abstractmethod
    def apply(self, target: Relation, *, lock: bool = False) -> Relation:
        """Create a new relation that represents the action of this operation
        on an existing relation.

        Parameters
        ----------
        target : `Relation`
            Relation the operation will act on.
        lock : `bool`, optional
            Set `Relation.is_locked` on the result to this value.

        Returns
        -------
        new_relation : `Relation`
            Relation that includes this operation.  This may be ``self`` if the
            operation is a no-op, and it may not be a `UnaryOperationRelation`
            holding this operation (or even a similar one) if the operation was
            inserted earlier in the tree via commutation relations.

        Raises
        ------
        ColumnError
            Raised if the operation could not be applied due to problems with
            the target relation's columns.
        EngineError
            Raised if the operation could not be applied due to problems with
            the target relation's engine.
        RowOrderError
            Raised if ``target`` is unnecessarily ordered; see
            `Relation.expect_unordered`.

        Notes
        -----
        Most concrete implementations support additional optional keyword
        arguments that provide more control over where the operation is
        inserted; see operation subclass documentation for details.
        """
        from ._operation_relations import UnaryOperationRelation

        return UnaryOperationRelation(
            operation=self,
            target=target,
            columns=self.applied_columns(target),
            is_locked=lock,
        )

    def applied_engine(self, target: Relation) -> Engine:
        """Return the engine of the relation that results from applying this
        operation to the given target.

        Parameters
        ----------
        target : `Relation`
            Relation the operation will act on.

        Returns
        -------
        engine : `Engine`
            Engine a new relation would have.
        """
        return target.engine

    def applied_columns(self, target: Relation) -> Set[ColumnTag]:
        """Return the columns of the relation that results from applying this
        operation to the given target.

        Parameters
        ----------
        target : `Relation`
            Relation the operation will act on.

        Returns
        -------
        columns : `~collections.abc.Set` [ `ColumnTag` ]
            Columns the new relation would have.
        """
        return target.columns

    @abstractmethod
    def applied_min_rows(self, target: Relation) -> int:
        """Return the minimum number of rows of the relation that results from
        applying this operation to the given target.

        Parameters
        ----------
        target : `Relation`
            Relation the operation will act on.

        Returns
        -------
        min_rows : `int`
            Minimum number of rows the new relation would have.
        """
        raise NotImplementedError()

    def applied_max_rows(self, target: Relation) -> int | None:
        """Return the maximum number of rows of the relation that results from
        applying this operation to the given target.

        Parameters
        ----------
        target : `Relation`
            Relation the operation will act on.

        Returns
        -------
        max_rows : `int` or `None`
            Maximum number of rows the new relation would have.
        """
        return target.max_rows


class RowFilter(UnaryOperation):
    """An extensible `UnaryOperation` subclass for operations that only remove
    rows from their target.
    """

    @final
    @property
    def is_count_invariant(self, engine: Engine | None = None) -> Literal[False]:
        # Docstring inherited.
        return False

    @property
    @abstractmethod
    def is_order_dependent(self) -> bool:
        # Docstring inherited.
        raise NotImplementedError()

    @final
    def applied_engine(self, target: Relation) -> Engine:
        # Docstring inherited.
        return target.engine

    @final
    def applied_columns(self, target: Relation) -> Set[ColumnTag]:
        # Docstring inherited.
        return target.columns

    def applied_min_rows(self, target: Relation) -> int:
        # Docstring inherited.
        if target.min_rows == 0 or self.is_count_invariant:
            return target.min_rows
        elif self.is_empty_invariant:
            return 1
        else:
            return 0


class Marker(UnaryOperation):
    """An extensible `UnaryOperation` subclass for operations that do not
    change the rows or columns of their target at all.
    """

    @final
    @property
    def is_count_invariant(self) -> Literal[True]:
        # Docstring inherited.
        return True

    @final
    @property
    def is_empty_invariant(self) -> Literal[True]:
        # Docstring inherited.
        return True

    @final
    def applied_columns(self, target: Relation) -> Set[ColumnTag]:
        # Docstring inherited.
        return target.columns

    @final
    def applied_min_rows(self, target: Relation) -> int:
        # Docstring inherited.
        return target.min_rows

    @final
    def applied_max_rows(self, target: Relation) -> int | None:
        # Docstring inherited.
        return target.max_rows


@final
class Identity(Marker):
    """A concrete unary operation that does nothing.

    `Identity` operations never appear in relation trees; their `apply` method
    always just returns the target relation.
    """

    def __str__(self) -> str:
        return "identity"

    def apply(self, target: Relation, *, lock: bool = False) -> Relation:
        # Docstring inherited.
        return target


class Reordering(UnaryOperation):
    """An extensible `UnaryOperation` subclass for operations that only reorder
    rows.
    """

    @final
    @property
    def is_count_invariant(self) -> Literal[True]:
        # Docstring inherited.
        return True

    @final
    @property
    def is_empty_invariant(self) -> Literal[True]:
        # Docstring inherited.
        return True

    @final
    def applied_engine(self, target: Relation) -> Engine:
        # Docstring inherited.
        return target.engine

    @final
    def applied_columns(self, target: Relation) -> Set[ColumnTag]:
        # Docstring inherited.
        return target.columns

    @final
    def applied_min_rows(self, target: Relation) -> int:
        # Docstring inherited.
        return target.min_rows

    @final
    def applied_max_rows(self, target: Relation) -> int | None:
        # Docstring inherited.
        return target.max_rows
