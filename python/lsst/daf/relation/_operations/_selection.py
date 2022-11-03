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

__all__ = ("Selection",)

import dataclasses
from collections.abc import Set
from typing import TYPE_CHECKING, final

from .._columns import ColumnTag, Predicate, flatten_logical_and
from .._exceptions import ColumnError, EngineError
from .._operation_relations import BinaryOperationRelation, UnaryOperationRelation
from .._unary_operation import RowFilter

if TYPE_CHECKING:
    from .._engine import Engine
    from .._relation import Relation


@final
@dataclasses.dataclass(frozen=True)
class Selection(RowFilter):
    """A relation operation that filters rows according to a boolean column
    expression.
    """

    predicate: Predicate
    """Boolean column expression that evaluates to `True` for rows to be
    kept and `False` for rows to be filtered out (`Predicate`).
    """

    def __post_init__(self) -> None:
        # Simplify-out nested ANDs and literal True/False values.
        if (and_sequence := flatten_logical_and(self.predicate)) is not False:
            object.__setattr__(self, "predicate", Predicate.logical_and(*and_sequence))

    @property
    def columns_required(self) -> Set[ColumnTag]:
        # Docstring inherited.
        return self.predicate.columns_required

    @property
    def is_empty_invariant(self) -> bool:
        # Docstring inherited.
        return False

    @property
    def is_order_dependent(self) -> bool:
        # Docstring inherited.
        return False

    def __str__(self) -> str:
        return f"σ[{self.predicate}]"

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
        """Return a new relation that applies this selection to an existing
        relation.

        `Relation.with_rows_satisfying` is a convenience method that should
        be preferred to constructing and applying a `Selection` directly.


        Parameters
        ----------
        target : `Relation`
            Relation this operation will act upon.
        preferred_engine : `Engine`, optional
            Engine that the operation would ideally be performed in.  If this
            is not equal to ``self.engine``, the ``backtrack``, ``transfer``,
            and ``require_preferred_engine`` arguments control the behavior.
        backtrack : `bool`, optional
            If `True` (default) and the current engine is not the preferred
            engine, attempt to insert this selection before a transfer
            upstream of the current relation, as long as this can be done
            without breaking up any locked relations or changing the resulting
            relation content.
        transfer : `bool`, optional
            If `True` (`False` is default) and the current engine is not the
            preferred engine, insert a new `Transfer` before the
            `Selection`.  If ``backtrack`` is also true, the transfer is
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
            New relation with only the rows that satisfy `predicate`.
            May be ``target`` if `predicate` is
            `trivially True <Predicate.as_trivial>`.

        Raises
        ------
        ColumnError
            Raised if ``predicate.columns_required`` is not a subset of
            ``self.columns``.
        EngineError
            Raised if ``require_preferred_engine=True`` and it was impossible
            to insert this operation in the preferred engine, or if the
            expression was not supported by the engine.
        RowOrderError
            Raised if ``self`` is unnecessarily ordered; see
            `Relation.expect_unordered`.
        """
        if self.predicate.as_trivial() is True:
            return target
        # We don't simplify the trivially-false predicate case, in keeping with
        # our policy of leaving doomed relations in place for diagnostics
        # to report on later.
        if not self.predicate.columns_required <= target.columns:
            raise ColumnError(
                f"Predicate {self.predicate} for target relation {target} needs "
                f"columns {self.predicate.columns_required - target.columns}."
            )
        if not target.engine.preserves_order(self):
            target = target.expect_unordered(
                None
                if strip_ordering
                else f"Selection in engine {target.engine} will not preserve order when applied to {target}."
            )
        if preferred_engine is not None and preferred_engine != target.engine:
            if backtrack and (result := self._insert_recursive(target, preferred_engine, lock)):
                return result
            elif transfer:
                from ._transfer import Transfer

                target = Transfer(preferred_engine).apply(target)
            elif require_preferred_engine:
                raise EngineError(
                    f"No way to apply selection with predicate {self.predicate} "
                    f"with required engine {preferred_engine}."
                )
        if not self.predicate.is_supported_by(target.engine):
            raise EngineError(f"Predicate {self.predicate} does not support engine {target.engine}.")
        match target:
            case UnaryOperationRelation(operation=Selection(predicate=other_predicate), target=nested_target):
                return Selection(predicate=other_predicate.logical_and(self.predicate)).apply(nested_target)
        return super().apply(target, lock=lock)

    def _insert_recursive(self, target: Relation, preferred_engine: Engine, lock: bool) -> Relation | None:
        """Recursive implementation for `apply`.

        See that method's documentation for details.
        """
        from ._chain import Chain
        from ._join import Join

        if target.is_locked:
            return None
        match target:
            case UnaryOperationRelation(operation=operation, target=next_target):
                if operation.is_count_dependent:
                    return None
                if operation.is_order_dependent and not next_target.engine.preserves_order(self):
                    return None
                if not self.columns_required <= next_target.columns:
                    return None
                # If other checks above are satisfied, a Selection commutes
                # through:
                # - Calculations;
                # - Deduplications;
                # - Projections;
                # - Marker subclasses;
                # - other RowFilter subclasses;
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

                        def _try_branch(branch: Relation) -> Relation:
                            if branch.columns >= self.columns_required:
                                return self._insert_recursive(branch, preferred_engine, lock) or branch
                            else:
                                return branch

                        # We can try to insert the selection into either branch
                        # or both branches - applying it everywhere it is valid
                        # as early as possible is usually desirable.
                        new_join_lhs = _try_branch(next_lhs)
                        new_join_rhs = _try_branch(next_rhs)
                        del _try_branch  # helps garbage collector a lot
                        if new_join_lhs is not next_lhs or new_join_rhs is not next_rhs:
                            return operation.apply(new_join_lhs, new_join_rhs)
                    case Chain():
                        new_union_lhs = self._insert_recursive(next_lhs, preferred_engine, lock)
                        new_union_rhs = self._insert_recursive(next_rhs, preferred_engine, lock)
                        if new_union_lhs and new_union_rhs:
                            return operation.apply(new_union_lhs, new_union_rhs)
        return None

    def applied_min_rows(self, target: Relation) -> int:
        # Docstring inherited.
        return 0
