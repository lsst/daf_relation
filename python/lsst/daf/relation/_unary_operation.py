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

__all__ = ("UnaryOperation", "RowFilter", "Reordering", "Identity", "UnaryCommutator")

import dataclasses
from abc import ABC, abstractmethod
from collections.abc import Set
from typing import TYPE_CHECKING, Literal, final

from ._columns import ColumnTag
from ._exceptions import EngineError
from ._relation import Relation

if TYPE_CHECKING:
    from ._engine import Engine
    from ._operation_relations import UnaryOperationRelation


class UnaryOperation(ABC):
    """An abstract base class for operations that act on a single relation.

    Notes
    -----
    A `UnaryOperation` represents the operation itself; the combination of an
    operation and the "target" relation it acts on to form a new relation is
    represented by the `UnaryOperationRelation` class.  That combination should
    always be created via a call to the `apply` method (or something that calls
    it, like the convenience methods on the `Relation` class).  In some cases,
    applying a `UnaryOperation` doesn't return something involving the original
    operation, because of some combination of defaulted-parameter population
    and simplification, and there are even some `UnaryOperation` classes that
    should never actually appear in a `UnaryOperationRelation`.

    `UnaryOperation` cannot be subclassed directly by external code, but it has
    two more restricted subclasses that can be:`RowFilter` and `Reordering`.

    All concrete `UnaryOperation` types are frozen, equality-comparable
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
                "PartialJoin",
                "Projection",
                "RowFilter",
                "Reordering",
            }
            or cls.__base__ is not UnaryOperation
        ), (
            "UnaryOperation inheritance is closed to predefined types in daf_relation, "
            "except for subclasses of RowFilter and Reordering."
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

    def is_supported_by(self, engine: Engine) -> bool:
        """Whether this operation is supported by the given engine (`bool`)."""
        return True

    @final
    def apply(
        self,
        target: Relation,
        *,
        preferred_engine: Engine | None = None,
        backtrack: bool = True,
        transfer: bool = False,
        require_preferred_engine: bool = False,
    ) -> Relation:
        """Create a new relation that represents the action of this operation
        on an existing relation.

        Parameters
        ----------
        target : `Relation`
            Relation the operation will act on.
        preferred_engine : `Engine`, optional
            Engine that the operation would ideally be performed in.  If this
            is not equal to ``target.engine``, the ``backtrack``, ``transfer``,
            and ``require_preferred_engine`` arguments control the behavior.
            Some operations may supply their own preferred engine default, such
            as the "fixed" operand's own engine in a `PartialJoin`.
        backtrack : `bool`, optional
            If `True` (default) and the current engine is not the preferred
            engine, attempt to insert this operation before a transfer upstream
            of the current relation, as long as this can be done without
            breaking up any locked relations or changing the resulting relation
            content.
        transfer : `bool`, optional
            If `True` (`False` is default) and the current engine is not the
            preferred engine, insert a new `Transfer` to the preferred engine
            before this operation.  If ``backtrack`` is also true, the transfer
            is added only if the backtrack attempt fails.
        require_preferred_engine : `bool`, optional
            If `True` (`False` is default) and the current engine is not the
            preferred engine, raise `EngineError`.  If ``backtrack`` is also
            true, the exception is only raised if the backtrack attempt fails.
            Ignored if ``transfer`` is true.

        Returns
        -------
        new_relation : `Relation`
            Relation that includes this operation.  This may be ``target`` if
            the operation is a no-op, and it may not be a
            `UnaryOperationRelation` holding this operation (or even a similar
            one) if the operation was inserted earlier in the tree via
            commutation relations or if simplification occurred.

        Raises
        ------
        ColumnError
            Raised if the operation could not be applied due to problems with
            the target relation's columns.
        EngineError
            Raised if the operation could not be applied due to problems with
            the target relation's engine.

        Notes
        -----
        Adding operations to relation trees is a potentially complex process in
        order to give both the operation type and the engine to customize the
        opportunity to enforce their own invariants.  This `~typing.final`
        method provides the bulk of the high-level implementation, and is
        called by the `Relation` class's convenience methods with essentially
        no additional logic.  The overall sequence is as follows:

        - `apply` starts by delegating to `_begin_apply`, which
          allows operation classes to replace the operation object itself,
          perform initial checks, and set the preferred engine.
        - `apply` then performs the ``preferred_engine`` logic indicated by the
          ``backtrack`` ``transfer``, and ``require_preferred_engine`` options,
          delegating backtracking to `Engine.backtrack_unary`.
        - `Engine.backtrack_unary` will typically call back to `commute` to
          determine how and whether to move the new operation upstream of
          existing ones.
        - If backtracking is not used or is not fully successful, `apply` then
          delegates to `Engine.append_unary` to add the operation to the root
          of the relation tree.
        - The `Engine` methods are expected to delegate back to
          `_finish_apply` when they have identified the location in the tree
          where the new operation should be inserted.
        - `_finish_apply` is responsible for actually constructing the
          `UnaryOperationRelation` when appropriate.  The default
          implementation of `_finish_apply` also calls `simplify` to see if it
          is possible to merge the new operation with those immediately
          upstream of it or elide it entirely.
        """
        operation, preferred_engine = self._begin_apply(target, preferred_engine)
        done = False
        result = target
        if preferred_engine != target.engine:
            if backtrack:
                result, done, messages = target.engine.backtrack_unary(operation, target, preferred_engine)
            else:
                messages = ("backtracking insertion not requested by caller",)
            if not done:
                if transfer:
                    result = result.transferred_to(preferred_engine)
                elif require_preferred_engine:
                    raise EngineError(
                        f"No way to apply {operation} to {target} "
                        f"with required engine '{preferred_engine}': {'; '.join(messages)}."
                    )
        if not done:
            result = result.engine.append_unary(operation, result)
        return result

    def _begin_apply(
        self, target: Relation, preferred_engine: Engine | None
    ) -> tuple[UnaryOperation, Engine]:
        """A customization hook for the beginning of operation application.

        Parameters
        ----------
        target : `Relation`
            Relation the operation should act on, at least conceptually.  Later
            logic may actually apply the operation upstream of this relation,
            but only when the result of doing so would be equivalent to
            applying it here.
        preferred_engine : `Engine` or `None`
            Preferred engine passed to `apply`.

        Returns
        -------
        operation : `UnaryOperation`
            The operation to actually apply.  The default implementation
            returns ``self``.
        preferred_engine : `Engine`
            The engine to actually prefer.  The default implementation returns
            the given ``preferred_engine`` if it is not `None`, and
            ``target.engine`` if it is `None`.

        Notes
        -----
        This method provides an opportunity for operations to establish any
        invariants that must be satisfied only when the operation is part of
        a relation.  Implementations can also return an `Identity` instance
        when they can determine that the operation will do nothing when applied
        to the given target.
        """  # noqa: D401
        return self, preferred_engine if preferred_engine is not None else target.engine

    def _finish_apply(self, target: Relation) -> Relation:
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
        This method provides an opportunity for operations to perform final
        simplification at the point of insertion (which the default
        implementation does, via calls to `simplify`) and change the kind of
        relation produced (the default implementation constructs a
        `UnaryOperationRelation`).
        """  # noqa: D401
        from ._operation_relations import UnaryOperationRelation

        match target:
            case UnaryOperationRelation():
                if simplified := self.simplify(target.operation):
                    if simplified is target.operation:
                        return target
                    else:
                        return simplified._finish_apply(target.target)

        if not self.is_supported_by(target.engine):
            raise EngineError(f"Operation {self} is not supported by engine {target.engine}.")

        return UnaryOperationRelation(
            operation=self,
            target=target,
            columns=self.applied_columns(target),
        )

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

    def commute(self, current: UnaryOperationRelation) -> UnaryCommutator:
        """Describe whether and how this operation can be moved upstream of an
        existing one without changing the content of the resulting relation.

        Parameters
        ----------
        current : `UnaryOperationRelation`
            A unary operation relation that is the current logical target of
            ``self``.

        Returns
        -------
        commutator : `UnaryCommutator`
            A struct that either provides a version of ``current.operation``
            that can be applied to ``current.target`` after a possibly-modified
            version of ``self``, or an explanation of why this is impossible.

        Notes
        -----
        The `commute` implementations for the provided concrete
        `UnaryOperation` types assume that all unary operations preserve row
        order.  If this is not the case in an engine, that engine should not
        implement `Engine.backtrack_unary` or take this into account itself
        when determining whether operations commute.
        """
        return UnaryCommutator(
            first=None,
            second=current.operation,
            done=False,
            messages=(f"{self} does not commute with anything",),
        )

    def simplify(self, upstream: UnaryOperation) -> UnaryOperation | None:
        """Return a simplified combination of this operation with another.

        Parameters
        ----------
        upstream : `UnaryOperation`
            Operation that acts immediately prior to ``self``.

        Returns
        -------
        simplified : `UnaryOperation`
            Operation that combines the action of ``upstream`` followed by
            ``self``, or `None` if no such combination is possible.
        """
        return None


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


@final
class Identity(UnaryOperation):
    """A concrete unary operation that does nothing.

    `Identity` operations never appear in relation trees; their `apply` method
    always just returns the target relation.
    """

    def __str__(self) -> str:
        return "identity"

    def _finish_apply(self, target: Relation) -> Relation:
        # Docstring inherited.
        return target

    @property
    def is_count_invariant(self) -> Literal[True]:
        # Docstring inherited.
        return True

    @property
    def is_empty_invariant(self) -> Literal[True]:
        # Docstring inherited.
        return True

    def applied_columns(self, target: Relation) -> Set[ColumnTag]:
        # Docstring inherited.
        return target.columns

    def applied_min_rows(self, target: Relation) -> int:
        # Docstring inherited.
        return target.min_rows

    def applied_max_rows(self, target: Relation) -> int | None:
        # Docstring inherited.
        return target.max_rows

    def commute(self, current: UnaryOperationRelation) -> UnaryCommutator:
        return UnaryCommutator(first=self, second=current.operation)

    def simplify(self, current: UnaryOperation) -> UnaryOperation:
        return current


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


@dataclasses.dataclass
class UnaryCommutator:
    """A struct for the return value of `UnaryOperation.commute`."""

    first: UnaryOperation | None
    """The first operation to apply in the commuted sequence (`UnaryOperation`
    or `None`).

    When at least some commutation is possible, this is a possibly-modified
    version of ``current.operation``, where ``current`` is the argument to
    `UnaryOperation.commute`.  When it is `None`, either the commutation failed
    or the original operation will simplify away entirely (as indicated by
    ``done``).
    """

    second: UnaryOperation
    """The second operation to apply in the commuted sequence
    (`UnaryOperation`).

    When commutation is successful, this is usually ``self`` or a modification
    thereof.  When commutation is unsuccessful, this should be exactly
    ``current.operation``, where ``current`` is the argument to
    `UnaryOperation.commute`.
    """

    done: bool = True
    """Whether the commutation was fully successful (`bool`).

    When `False`, the original downstream relation (``self`` in call to
    `commute`) must still be applied after `first` (if not `None`) and
    `second`.  While `first` is usually `None` in this case, `Projection`
    operations (and possibily some extension operations) can be partially
    commuted
    """

    messages: tuple[str, ...] = dataclasses.field(default_factory=tuple)
    """Messages that describe why commutation either failed or only
    partially succeeded.
    """
