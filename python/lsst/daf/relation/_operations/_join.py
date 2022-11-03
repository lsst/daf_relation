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

__all__ = ("Join", "PartialJoin")

import dataclasses
from collections.abc import Set
from typing import TYPE_CHECKING, final

from .._binary_operation import BinaryOperation
from .._columns import ColumnTag, Predicate
from .._exceptions import ColumnError, EngineError
from .._operation_relations import BinaryOperationRelation, UnaryOperationRelation
from .._unary_operation import UnaryOperation

if TYPE_CHECKING:
    from .._engine import Engine
    from .._relation import Relation


@final
@dataclasses.dataclass(frozen=True)
class Join(BinaryOperation):
    """A natural join operation.

    A natural join combines two relations by matching rows with the same values
    in their common columns (and satisfying an optional column expression, via
    a `Predicate`), producing a new relation whose columns are the union of the
    columns of its operands.  This is equivalent to [``INNER``] ``JOIN`` in
    SQL.
    """

    predicate: Predicate = dataclasses.field(default_factory=lambda: Predicate.literal(True))
    """A boolean expression that must evaluate to true for any matched rows
    (`Predicate`).

    This does not include the equality constraint on `common_columns`.
    """

    min_columns: Set[ColumnTag] = dataclasses.field(default=frozenset())
    """The minimal set of columns that should be used in the equality
    constraint on `common_columns` (`~collections.abc.Set` [ `ColumnTag` ]).

    If the relations this operation is applied to have common columsn that are
    not a superset of this set, `ColumnError` will be raised by `apply`.

    This is guaranteed to be equal to `max_columns` on any `Join` instance
    attached to a `BinaryOperationRelation` by `apply`.
    """

    max_columns: Set[ColumnTag] | None = dataclasses.field(default=None)
    """The maximal set of columns that should be used in the equality
    constraint on `common_columns` (`~collections.abc.Set` [ `ColumnTag` ] or
    ``None``).

    If the relations this operation is applied to have more columns in common
    than this set, they will not be included in the equality constraint.

    This is guaranteed to be equal to `min_columns` on any `Join` instance
    attached to a `BinaryOperationRelation` by `apply`.
    """

    @property
    def common_columns(self) -> Set[ColumnTag]:
        """The common columns between relations that will be used as an
        equality constraint (`~collections.abc.Set` [ `ColumnTag` ]).

        This attribute is not available on `Join` instances for which
        `min_columns` is not the same as `max_columns`.  It is always available
        on any `Join` instance attached to a `BinaryOperationRelation` by
        `apply`.
        """
        if self.max_columns == self.min_columns:
            return self.min_columns
        else:
            raise ColumnError(f"Common columns for join {self} have not been resolved.")

    def __str__(self) -> str:
        return "⋈"

    def apply(
        self,
        lhs: Relation,
        rhs: Relation,
        *,
        lock: bool = False,
        strip_ordering: bool = False,
    ) -> Relation:
        """Apply this join operation to a pair of relations, creating a new
        relation.

        `Relation.join` is a convenience method that should
        be preferred to constructing and applying a `Join` directly.

        Parameters
        ----------
        lhs : `Relation`
            One relation to join.
        rhs : `Relation`
            The other relation to join to ``lhs``.
        lock : `bool`, optional
            Set `is_locked` on the returned relation to this value.
        strip_ordering : `bool`, optional
            If `True`, remove upstream operations that impose row ordering when
            the application of this operation makes that ordering unnecessary;
            if `False` (default) raise `RowOrderError` instead (see
            `Relation.expect_unordered`).

        Returns
        -------
        relation : `Relation`
            New relation that joins ``lhs`` to ``rhs``.  May be ``lhs`` or
            ``rhs`` if the other is a `join identity <is_join_identity>`.

        Raises
        ------
        ColumnError
            Raised if the `predicate` requires columns not present in either
            ``lhs`` or ``rhs``.
        EngineError
            Raised if ``lhs.engine != rhs.engine``
        RowOrderError
            Raised if ``lhs`` or ``rhs`` is unnecessarily ordered; see
            `Relation.expect_unordered`.

        Notes
        -----
        `PartialJoin` can be used to provide backtrack/transfer behavior to
        deal with the case where the operands may not have the same engine.
        """
        if lhs.engine != rhs.engine:
            raise EngineError(f"Mismatched join engines: {lhs.engine} != {rhs.engine}.")
        if not self.predicate.is_supported_by(lhs.engine):
            raise EngineError(f"Join predicate {self.predicate} does not support engine {lhs.engine}.")
        if not self.predicate.columns_required <= self.applied_columns(lhs, rhs):
            raise ColumnError(
                f"Missing columns {set(self.predicate.columns_required - self.applied_columns(lhs, rhs))} "
                f"for join between {lhs!r} and {rhs!r} with predicate {self.predicate}."
            )
        if self.max_columns != self.min_columns:
            common_columns = self.applied_common_columns(lhs, rhs)
            operation = dataclasses.replace(self, min_columns=common_columns, max_columns=common_columns)
        else:
            if not lhs.columns >= self.common_columns:
                raise ColumnError(
                    f"Missing columns {set(self.common_columns - lhs.columns)} "
                    f"for left-hand side of join between {lhs!r} and {rhs!r}."
                )
            if not lhs.columns >= self.common_columns:
                raise ColumnError(
                    f"Missing columns {set(self.common_columns - rhs.columns)} "
                    f"for right-hand side of join between {lhs!r} and {rhs!r}."
                )
            operation = self
        lhs = lhs.expect_unordered(
            None if strip_ordering else f"Join to {rhs} will not preserve order in {lhs}."
        )
        rhs = rhs.expect_unordered(
            None if strip_ordering else f"Join to {lhs} will not preserve order in {rhs}."
        )
        # These simplifications for identity relations intentionally happen
        # after all of the previous checks to avoid silently ignoring logic
        # bugs.
        if lhs.is_join_identity:
            return rhs
        if rhs.is_join_identity:
            return lhs
        return BinaryOperation.apply(operation, lhs, rhs, lock=lock)

    def applied_columns(self, lhs: Relation, rhs: Relation) -> Set[ColumnTag]:
        # Docstring inherited.
        return lhs.columns | rhs.columns

    def applied_min_rows(self, lhs: Relation, rhs: Relation) -> int:
        # Docstring inherited.
        return 0

    def applied_max_rows(self, lhs: Relation, rhs: Relation) -> int | None:
        # Docstring inherited.
        if lhs.max_rows == 0 or rhs.max_rows == 0:
            return 0
        if lhs.max_rows is None or rhs.max_rows is None:
            return None
        else:
            return lhs.max_rows * rhs.max_rows

    def applied_common_columns(self, lhs: Relation, rhs: Relation) -> Set[ColumnTag]:
        """Compute the actual common columns for a `Join` given its targets.

        Parameters
        ----------
        lhs : `Relation`
            One relation to join.
        rhs : `Relation`
            The other relation to join to ``lhs``.

        Returns
        -------
        common_columns : `~collections.abc.Set` [ `ColumnTag` ]
            Columns that are included in all of ``lhs.columns`` and
            ``rhs.columns`` and `max_columns`, checked to be a superset of
            `min_columns`.

        Raises
        ------
        ColumnError
            Raised if the result would not be a superset of `min_columns`.
        """
        # Docstring inherited.
        if self.max_columns != self.min_columns:
            common_columns = {tag for tag in lhs.columns & rhs.columns if tag.is_key}
            if self.max_columns is not None:
                common_columns &= self.max_columns
            if not (common_columns >= self.min_columns):
                raise ColumnError(
                    f"Common columns {common_columns} for join between {lhs} and {rhs} are not a superset "
                    f"of the minimum columns {self.min_columns}."
                )
            return common_columns
        else:
            return self.min_columns

    def partial(self, fix: Relation, is_lhs: bool = False, strip_ordering: bool = False) -> PartialJoin:
        """Return a `UnaryOperation` that represents this join with one operand
        already provided and held fixed.

        Parameters
        ----------
        fix : `Relation`
            Relation to include in the returned unary operation.
        is_lhs : `bool`, optional
            Whether ``fix`` should be considered the ``lhs`` or ``rhs`` side of
            the join (`Join` side is *usually* irrelevant, but `Engine`
            implementations are permitted to make additional guarantees about
            row order or duplicates based on them).
        strip_ordering : `bool`, optional
            If `True`, remove upstream operations that impose row ordering when
            the application of this operation makes that ordering unnecessary;
            if `False` (default) raise `RowOrderError` instead (see
            `Relation.expect_unordered`).

        Returns
        -------
        partial_join : `PartialJoin`
            Unary operation representing a join to a fixed relation.

        Raises
        ------
        ColumnError
            Raised if the given predicate requires columns not present in
            ``lhs`` or ``rhs``.
        RowOrderError
            Raised if ``lhs`` or ``rhs`` is unnecessarily ordered; see
            `Relation.expect_unordered`.

        Notes
        -----
        This method and the class it returns are called "partial" in the spirit
        of `functools.partial`: a callable formed by holding some arguments to
        another callable fixed.
        """
        if not (self.min_columns <= fix.columns):
            raise ColumnError(
                f"Missing columns {set(self.min_columns - fix.columns)} for partial join to {fix}."
            )
        fix = fix.expect_unordered(
            None if strip_ordering else f"Join will not preserve order when applied to {fix}."
        )
        return PartialJoin(self, fix, is_lhs)


@final
@dataclasses.dataclass(frozen=True)
class PartialJoin(UnaryOperation):
    """A `UnaryOperation` that represents this join with one operand already
    provided and held fixed.

    Notes
    -----
    This class and the `Join.partial` used to construct it are called "partial"
    in the spirit of `functools.partial`: a callable formed by holding some
    arguments to another callable fixed.

    `PartialJoin` instances never appear in relation trees; the `apply` method
    will return a `BinaryOperationRelation` with a `Join` operation instead of
    a `UnaryOperationRelation` with a `PartialJoin` (or one of the operands, if
    the other is a `join identity relation <Relation.is_join_identity>`).
    """

    binary: Join
    """The join operation (`Join`) to be applied.
    """

    fixed: Relation
    """The target relation already included in the operation (`Relation`).
    """

    fixed_is_lhs: bool
    """Whether `fixed` should be considered the ``lhs`` or ``rhs`` side of
    the join.

    `Join` side is *usually* irrelevant, but `Engine` implementations are
    permitted to make additional guarantees about row order or duplicates based
    on them.
    """

    @property
    def columns_required(self) -> Set[ColumnTag]:
        # Docstring inherited.
        result = set(self.binary.predicate.columns_required)
        result.difference_update(self.fixed.columns)
        result.update(self.binary.min_columns)
        return result

    @property
    def is_empty_invariant(self) -> bool:
        # Docstring inherited.
        return False

    @property
    def is_count_invariant(self) -> bool:
        # Docstring inherited.
        return False

    def __str__(self) -> str:
        return f"{self.binary!s}[{self.fixed!s}]"

    def apply(
        self,
        target: Relation,
        *,
        backtrack: bool = True,
        transfer: bool = False,
        strip_ordering: bool = False,
        lock: bool = False,
    ) -> Relation:
        """Apply this join operation to a relation, creating a new relation.

        `Relation.join` is a convenience method that should be preferred to
        constructing and applying a `Partial Join` directly.

        Parameters
        ----------
        target : `Relation`
            Relation to join to `fixed`.
        backtrack : `bool`, optional
            If `True` (default) and ``self.engine != rhs.engine``, attempt to
            insert this join before a transfer upstream of ``self``, as long as
            this can be done without breaking up any locked relations or
            changing the resulting relation content.
        transfer : `bool`, optional
            If `True` (`False` is default) and ``self.engine != rhs.engine``,
            insert a new `Transfer` before the `Join`.  If ``backtrack`` is
            also true, the transfer is added only if the backtrack attempt
            fails.
        lock : `bool`, optional
            Set `~Relation.is_locked` on the returned relation to this value.
        strip_ordering : `bool`, optional
            If `True`, remove upstream operations that impose row ordering when
            the application of this operation makes that ordering unnecessary;
            if `False` (default) raise `RowOrderError` instead (see
            `Relation.expect_unordered`).

        Returns
        -------
        relation : `Relation`
            New relation that joins `fixed` to ``target``.  May be `fixed` or
            ``target`` if the other is a `join identity <is_join_identity>`.

        Raises
        ------
        ColumnError
            Raised if ``binary.predicate`` requires columns not present in
            `fixed` or ``target``.
        EngineError
            Raised if it was impossible to insert this operation in
            ``fixed.engine`` via backtracks or transfers on ``target``, or if
            the predicate was not supported by the engine.
        RowOrderError
            Raised if `fixed` or ``target`` is unnecessarily ordered; see
            `Relation.expect_unordered`.

        Notes
        -----
        This method will only backtrack uptream of ``target`` or apply a
        transfer to ``target``, not `fixed`, but it will strip ordering
        operations from both operands if ``strip_ordering=True``.
        """
        if self.binary.max_columns != self.binary.min_columns:
            common_columns = self.binary.applied_common_columns(self.fixed, target)
            replacement = dataclasses.replace(
                self,
                binary=dataclasses.replace(
                    self.binary, min_columns=common_columns, max_columns=common_columns
                ),
            )
            return replacement.apply(target, backtrack=backtrack, transfer=transfer)
        if not self.columns_required <= target.columns:
            raise ColumnError(
                f"Join {self} to relation {target} needs columns "
                f"{set(self.columns_required) - target.columns}."
            )
        if target.engine != self.fixed.engine:
            if backtrack and (result := self._insert_recursive(target, lock)):
                return result
            elif transfer:
                from ._transfer import Transfer

                target = Transfer(self.fixed.engine).apply(target)
            else:
                raise EngineError(
                    f"No way to apply join between {self.fixed} and {target} in engine {self.fixed.engine}."
                )
        if self.fixed_is_lhs:
            return self.binary.apply(self.fixed, target, lock=lock, strip_ordering=strip_ordering)
        else:
            return self.binary.apply(target, self.fixed, lock=lock, strip_ordering=strip_ordering)

    def _insert_recursive(self, target: Relation, lock: bool) -> Relation | None:
        """Recursive implementation for `apply`.

        See that method's documentation for details.
        """
        assert self.binary.common_columns is not None, "Guaranteed by apply()."
        from ._chain import Chain
        from ._deduplication import Deduplication
        from ._projection import Projection

        if target.is_locked:
            return None
        match target:
            case UnaryOperationRelation(operation=operation, target=next_target):
                match operation:
                    case Deduplication():
                        # A Join only commutes past Deduplication if the fixed
                        # relation has unique rows, which is not something we
                        # can check right now.
                        return None
                    case Projection():
                        # In order for projection(join(target)) to be
                        # equivalent to join(projection(target)), the new outer
                        # projection has to include the columns added by the
                        # join.  Note that because we require common_columns to
                        # be explicit at this point, the projection cannot
                        # change them.
                        operation = Projection(self.applied_columns(target))
                if operation.is_order_dependent:
                    return None
                if operation.is_count_dependent:
                    return None
                if not self.columns_required <= next_target.columns:
                    return None
                # If other checks above are satisfied, a PartialJoin commutes
                # through:
                # - Calculations;
                # - Marker subclasses;
                # - RowFilter subclasses;
                # - Reordering subclasses;
                # while Identity and PartialJoin never actually appear in
                # UnaryOperationRelations, as their apply() methods guarantee.
                if next_target.engine == self.fixed.engine:
                    return operation.apply(self.apply(next_target, lock=lock))
                if new_target := self._insert_recursive(next_target, lock):
                    return operation.apply(new_target)
            case BinaryOperationRelation(operation=operation, lhs=next_lhs, rhs=next_rhs):
                match operation:
                    case Join():

                        def _try_branch(branch: Relation) -> Relation | None:
                            if branch.columns >= self.columns_required:
                                return self._insert_recursive(branch, lock)
                            else:
                                return None

                        if new_join_lhs := _try_branch(next_lhs):
                            del _try_branch  # helps garbage collector a lot
                            return operation.apply(new_join_lhs, next_rhs)
                        if new_join_rhs := _try_branch(next_rhs):
                            del _try_branch  # helps garbage collector a lot
                            return operation.apply(next_lhs, new_join_rhs)
                        del _try_branch  # helps garbage collector a lot

                    case Chain():
                        new_union_lhs = self._insert_recursive(next_lhs, lock)
                        new_union_rhs = self._insert_recursive(next_rhs, lock)
                        if new_union_lhs and new_union_rhs:
                            return operation.apply(new_union_lhs, new_union_rhs)
        return None

    def applied_engine(self, target: Relation) -> Engine:
        # Docstring inherited.
        if self.fixed_is_lhs:
            return self.binary.applied_engine(self.fixed, target)
        else:
            return self.binary.applied_engine(target, self.fixed)

    def applied_columns(self, target: Relation) -> Set[ColumnTag]:
        # Docstring inherited.
        if self.fixed_is_lhs:
            return self.binary.applied_columns(self.fixed, target)
        else:
            return self.binary.applied_columns(target, self.fixed)

    def applied_min_rows(self, target: Relation) -> int:
        # Docstring inherited.
        if self.fixed_is_lhs:
            return self.binary.applied_min_rows(self.fixed, target)
        else:
            return self.binary.applied_min_rows(target, self.fixed)

    def applied_max_rows(self, target: Relation) -> int | None:
        # Docstring inherited.
        if self.fixed_is_lhs:
            return self.binary.applied_max_rows(self.fixed, target)
        else:
            return self.binary.applied_max_rows(target, self.fixed)
