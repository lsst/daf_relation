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

__all__ = ("Deduplication",)

import dataclasses
from collections.abc import Sequence
from typing import TYPE_CHECKING, Literal, final

from .._columns import ColumnTag
from .._exceptions import ColumnError, EngineError
from .._operation_relations import BinaryOperationRelation, UnaryOperationRelation
from .._unary_operation import UnaryOperation

if TYPE_CHECKING:
    from .._engine import Engine
    from .._relation import Relation


@final
@dataclasses.dataclass(frozen=True)
class Deduplication(UnaryOperation):
    """A relation operation that removes duplicate rows."""

    unique_key: Sequence[ColumnTag] | None = None
    """Columns that are sufficient for uniqueness on their own
    (`~collections.abc.Sequence` [ `ColumnTag` ] or `None`).

    The `apply` method guarantees that this is never `None` when the operation
    is attached to a `UnaryOperationRelation`, by creating a new operation with
    `unique_key` set to all columns for which `ColumnTag.is_key` is `True`.
    """

    @property
    def is_count_invariant(self) -> Literal[False]:
        # Docstring inherited.
        return False

    @property
    def is_empty_invariant(self) -> Literal[True]:
        # Docstring inherited.
        return True

    def __str__(self) -> str:
        return "deduplication"

    def apply(
        self,
        target: Relation,
        *,
        preferred_engine: Engine | None = None,
        backtrack: bool = True,
        transfer: bool = False,
        require_preferred_engine: bool = False,
        lock: bool = False,
        strip_ordering: bool = False,
    ) -> Relation:
        """Return a new relation that applies this operation.

        `Relation.without_duplicates` is a convenience method that should be
        preferred to constructing and applying a `Deduplication` directly.

        Parameters
        ----------
        target : `Relation`
            Relation the operation will act upon.
        preferred_engine : `Engine`, optional
            Engine that the operation would ideally be performed in.  If this
            is not equal to ``target.engine``, the ``backtrack``, ``transfer``,
            and ``require_preferred_engine`` arguments control the behavior.
        backtrack : `bool`, optional
            If `True` (default) and the current engine is not the preferred
            engine, attempt to insert this deduplication before a transfer
            upstream of the current relation, as long as this can be done
            without breaking up any locked relations or changing the resulting
            relation content.
        transfer : `bool`, optional
            If `True` (`False` is default) and the current engine is not the
            preferred engine, insert a new `Transfer` before the
            `Deduplication`.  If ``backtrack`` is also true, the transfer is
            added only if the backtrack attempt fails.
        require_preferred_engine : `bool`, optional
            If `True` (`False` is default) and the current engine is not the
            preferred engine, raise `EngineError`.  If ``backtrack`` is also
            true, the exception is only raised if the backtrack attempt fails.
            Ignored if ``transfer`` is true.
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
            Relation with no duplicate rows.  This may be ``target`` if it can
            be determined that there is no duplication already, but this is not
            guaranteed.

        Raises
        ------
        ColumnError
            Raised if `unique_key` is `None` and no columns have
            `ColumnTag.is_key` set to `True`, or if `unique_key` is given but
            is not a subset of ``target.columns``.
        EngineError
            Raised if ``require_preferred_engine=True`` and it was impossible
            to insert this operation in the preferred engine.
        RowOrderError
            Raised if ``target`` is unnecessarily ordered; see
            `expect_unordered`.
        """
        if not target.engine.preserves_order(self):
            target = target.expect_unordered(
                None
                if strip_ordering
                else (
                    f"Deduplication in engine {target.engine} will not preserve order "
                    f"when applied to {target}."
                )
            )
        if self.unique_key is None:
            unique_key = self._applied_unique_key(target)
            return Deduplication(unique_key).apply(
                target,
                preferred_engine=preferred_engine,
                backtrack=backtrack,
                transfer=transfer,
                require_preferred_engine=require_preferred_engine,
                lock=lock,
                strip_ordering=strip_ordering,
            )
        if preferred_engine is not None and preferred_engine != target.engine:
            if backtrack and (result := self._insert_recursive(target, preferred_engine, lock)):
                return result
            elif transfer:
                from ._transfer import Transfer

                target = Transfer(preferred_engine).apply(target)
            elif require_preferred_engine:
                raise EngineError(f"No way to remove duplicates from {target} in engine {preferred_engine}.")
        return super().apply(target)

    def _insert_recursive(self, target: Relation, preferred_engine: Engine, lock: bool) -> Relation | None:
        """Recursive implementation for `apply`.

        See that method's documentation for details.
        """
        from ._join import Join

        if target.is_locked:
            return None
        match target:
            case UnaryOperationRelation(operation=operation, target=next_target):
                if isinstance(operation, Deduplication):
                    # No need for a new deduplication, because there already
                    # is one here.
                    return target
                if operation.is_count_dependent:
                    return None
                if operation.is_order_dependent and not next_target.engine.preserves_order(operation):
                    return None
                # Deduplication does not commute through Projection, but this a
                # bit more defensive to guard against what a Projection does
                # rather than isinstance(Projection).
                if not target.columns >= next_target.columns:
                    return None
                # If other checks above are satisfied, a Deduplication commutes
                # through:
                # - Calculations (assumes calculated columns are
                #   deterministic);
                # - Marker subclasses;
                # - RowFilter subclasses;
                # - Reordering subclasses;
                # while Identity and PartialJoin never actually appear in
                # UnaryOperationRelations, as their apply() methods guarantee.
                if next_target.engine == preferred_engine:
                    return operation.apply(self.apply(next_target, lock=lock))
                if new_target := self._insert_recursive(next_target, preferred_engine, lock):
                    return operation.apply(new_target)
            case BinaryOperationRelation(operation=operation, lhs=next_lhs, rhs=next_rhs):
                match operation:
                    case Join():
                        new_join_lhs = self._insert_recursive(next_lhs, preferred_engine, lock)
                        new_join_rhs = self._insert_recursive(next_rhs, preferred_engine, lock)
                        if new_join_lhs and new_join_rhs:
                            return operation.apply(new_join_rhs, new_join_rhs)
        return None

    def _applied_unique_key(self, target: Relation) -> Sequence[ColumnTag]:
        """Compute a `unique_key` from a target relation's columns.

        Parameters
        ----------
        target : `Relation`
            Relation this operation will act upon.

        Returns
        -------
        unique_key : `~collections.abc.Sequence` [ `ColumnTag` ]
            Sequence of columns from ``target.columns`` that should be unique
            on their own.
        """
        if self.unique_key is None:
            unique_key = tuple(c for c in target.columns if c.is_key)
            if not unique_key:
                raise ColumnError(f"No key columns in relation {target} for deduplication.")
            return unique_key
        elif not set(unique_key) <= target.columns:
            raise ColumnError(f"Unique key columns {unique_key} are not a subset of {target.columns}.")
        return self.unique_key

    def applied_min_rows(self, target: Relation) -> int:
        # Docstring inherited.
        return 1
