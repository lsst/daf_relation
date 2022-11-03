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

__all__ = ("Projection",)

import dataclasses
from collections.abc import Set
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
class Projection(UnaryOperation):
    """A unary operation that removes one or more columns.

    Notes
    -----
    This is the only operation permitted to introduce duplication among rows
    (as opposed to just propagating duplicates).
    """

    columns: Set[ColumnTag]
    """The columns to be kept (`~collections.abc.Set` [ `ColumnTag` ]).
    """

    @property
    def columns_required(self) -> Set[ColumnTag]:
        # Docstring inherited.
        return self.columns

    @property
    def is_empty_invariant(self) -> Literal[True]:
        # Docstring inherited.
        return True

    @property
    def is_count_invariant(self) -> Literal[True]:
        # Docstring inherited.
        return True

    def __str__(self) -> str:
        return f"Π[{', '.join(str(tag) for tag in self.columns)}]"

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
        """Return a new relation that applies this projection.

        `Relation.with_only_columns` is a convenience method that should
        be preferred to constructing and applying a `Projection` directly.

        Parameters
        ----------
        target : `Relation`
            Relation the calculation will act upon.
        preferred_engine : `Engine`, optional
            Engine that the operation would ideally be performed in.  If this
            is not equal to ``self.engine``, the ``backtrack``, ``transfer``,
            and ``require_preferred_engine`` arguments control the behavior.
        backtrack : `bool`, optional
            If `True` (default) and the current engine is not the preferred
            engine, attempt to insert this projection before a transfer
            upstream of the current relation, as long as this can be done
            without breaking up any locked relations or changing the resulting
            relation content.
        transfer : `bool`, optional
            If `True` (`False` is default) and the current engine is not the
            preferred engine, insert a new `Transfer` before the
            `Projection`.  If ``backtrack`` is also true, the transfer is
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
            `expect_unordered`).

        Returns
        -------
        relation : `Relation`
            New relation with only the given columns.  Will be ``self`` if
            ``columns == self.columns``.

        Raises
        ------
        ColumnError
            Raised if `columns` is not a subset of ``target.columns``.
        EngineError
            Raised if ``require_preferred_engine=True`` and it was impossible
            to insert this operation in the preferred engine.
        RowOrderError
            Raised if ``self`` is unnecessarily ordered; see
            `Relation.expect_unordered`.
        """
        if self.columns == target.columns:
            return target
        if not target.engine.preserves_order(self):
            target = target.expect_unordered(
                None
                if strip_ordering
                else f"Projection in engine {target.engine} will not preserve order when applied to {target}."
            )
        if not self.columns <= target.columns:
            raise ColumnError(
                f"Cannot project column(s) {set(self.columns) - target.columns} "
                f"that are not present in the target relation {target}."
            )
        if preferred_engine is not None and preferred_engine != target.engine:
            if backtrack:
                target = self._insert_recursive(target, preferred_engine, lock)
                if target.columns == self.columns:
                    return target
            if transfer:
                from ._transfer import Transfer

                target = Transfer(preferred_engine).apply(target)
            elif require_preferred_engine:
                raise EngineError(
                    f"No way to apply projection to columns {set(self.columns)} "
                    f"with required engine {preferred_engine}."
                )
        from ._calculation import Calculation

        # See similar checks in _insert_recursive for explanations; need to do
        # these here, too, for when that's never called.
        match target:
            case UnaryOperationRelation(operation=Projection(), target=nested_target):
                return self.apply(nested_target)
            case UnaryOperationRelation(
                operation=Calculation(tag=tag), target=nested_target
            ) if tag not in self.columns:
                return self.apply(nested_target)
        return super().apply(target, lock=lock)

    def _insert_recursive(
        self,
        target: Relation,
        preferred_engine: Engine,
        lock: bool,
    ) -> Relation:
        """Recursive implementation for `apply`.

        See that method's documentation for details.
        """
        from ._calculation import Calculation
        from ._chain import Chain
        from ._join import Join

        if target.is_locked:
            return target
        match target:
            case UnaryOperationRelation(operation=operation, target=next_target):
                recurse_with: Projection = self
                reapply_after: UnaryOperation | None = operation
                match operation:
                    case Projection():
                        # We can just drop any existing Projection as this one
                        # supersedes it; by construction the new one has a
                        # subset of the original's columns.
                        reapply_after = None
                    case Calculation(tag=tag) if tag not in self.columns:
                        # Projection will drop the column added by the
                        # Calculation, so it might as well have never
                        # existed.
                        reapply_after = None
                        recurse_with = self
                if not self.columns >= operation.columns_required:
                    # Can't move the entire projection past this operation;
                    # move what we can, and allow the rest to be handled by
                    # other options back in `apply`.
                    recurse_with = Projection(self.columns | operation.columns_required)
                else:
                    recurse_with = self
                if operation.is_order_dependent and not next_target.engine.preserves_order(self):
                    return target
                if next_target.engine == preferred_engine:
                    return operation.apply(self.apply(next_target, lock=lock))
                new_target = recurse_with._insert_recursive(next_target, preferred_engine, lock)
                if reapply_after is operation and new_target is next_target:
                    return target  # avoid spurious copies by returning original
                if reapply_after is not None:
                    return reapply_after.apply(new_target)
            case BinaryOperationRelation(operation=operation, lhs=next_lhs, rhs=next_rhs):
                match operation:
                    case Join(common_columns=common_columns, predicate=predicate):
                        recurse_columns = self.columns | common_columns | predicate.columns_required

                        def _try_branch(branch: Relation) -> Relation:
                            if recurse_columns >= branch.columns:
                                return branch
                            else:
                                new_projection = Projection(recurse_columns & branch.columns)
                                return new_projection._insert_recursive(branch, preferred_engine, lock)

                        new_join_lhs = _try_branch(next_lhs)
                        new_join_rhs = _try_branch(next_rhs)
                        if new_join_lhs is next_lhs and new_join_rhs is next_rhs:
                            return target
                        else:
                            return operation.apply(new_join_lhs, new_join_rhs)
                    case Chain():
                        new_union_lhs = self._insert_recursive(next_lhs, preferred_engine, lock)
                        new_union_rhs = self._insert_recursive(next_rhs, preferred_engine, lock)
                        if new_union_lhs is next_lhs and new_union_lhs is next_rhs:
                            return target
                        if new_union_lhs.columns != new_union_rhs.columns:
                            new_projection = Projection(new_union_lhs.columns | new_union_rhs.columns)
                            if new_projection.columns == self.columns:
                                # This is the best we can do; each side only
                                # projected away columns the other side kept.
                                return target
                            # Try again with a less-ambitious projection that
                            # should work equally well on both sides.
                            new_union_lhs = self._insert_recursive(next_lhs, preferred_engine, lock)
                            new_union_rhs = self._insert_recursive(next_rhs, preferred_engine, lock)
                        return operation.apply(new_union_lhs, new_union_rhs)
        return target

    def applied_columns(self, target: Relation) -> Set[ColumnTag]:
        # Docstring inherited.
        return self.columns

    def applied_min_rows(self, target: Relation) -> int:
        # Docstring inherited.
        return target.min_rows
