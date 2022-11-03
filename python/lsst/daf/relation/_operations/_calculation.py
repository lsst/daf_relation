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

__all__ = ("Calculation",)

import dataclasses
from collections.abc import Set
from typing import TYPE_CHECKING, Literal, final

from .._columns import ColumnExpression, ColumnTag
from .._exceptions import ColumnError, EngineError
from .._operation_relations import BinaryOperationRelation, UnaryOperationRelation
from .._unary_operation import UnaryOperation

if TYPE_CHECKING:
    from .._engine import Engine
    from .._relation import Relation


@final
@dataclasses.dataclass(frozen=True)
class Calculation(UnaryOperation):
    """A relation operation that adds a new column from an expression involving
    existing columns.

    Notes
    -----
    `Calculation` operations are assumed to be deterministically related to
    existing columns - in particular, a `Deduplication` is assumed to have the
    same effect regardless of whether it is performed before or after a
    `Calculation`.  This means a `Calculation` should not be used to generate
    random numbers or counters, though it does not prohibit additional
    information outside the relation being used.  The expression that backs
    a `Calculation` must depend on at least one existing column, however; it
    also cannot be used to add a constant-valued column to a relation.
    """

    tag: ColumnTag
    """Identifier for the new column (`ColumnTag`).
    """

    expression: ColumnExpression
    """Expression used to populate the new column (`ColumnExpression`).
    """

    def __post_init__(self) -> None:
        if not self.expression.columns_required:
            # It's unlikely anyone would want them, and explicitly prohibiting
            # calculated columns that are constants saves us from having to
            # worry about one-row, zero-column relations hiding behind them,
            # and hence Relation.is_trivial not propagating the way we'd like.
            raise ColumnError(
                f"Calculated column {self.tag} that does not depend on any other columns is not allowed."
            )

    @property
    def columns_required(self) -> Set[ColumnTag]:
        # Docstring inherited.
        return self.expression.columns_required

    @property
    def is_empty_invariant(self) -> Literal[True]:
        # Docstring inherited.
        return True

    @property
    def is_count_invariant(self) -> Literal[True]:
        # Docstring inherited.
        return True

    def __str__(self) -> str:
        return f"+[{self.tag!r}={self.expression!s}]"

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
        """Return a new relation that applies this calculation to an existing
        one.

        `Relation.with_calculated_column` is a convenience method that should
        be preferred to constructing and applying a `Calculation` directly.

        Parameters
        ----------
        target : `Relation`
            Relation the calculation will act upon.
        preferred_engine : `Engine`, optional
            Engine that the operation would ideally be performed in.  If this
            is not equal to ``target.engine``, the ``backtrack``, ``transfer``,
            and ``require_preferred_engine`` arguments control the behavior.
        backtrack : `bool`, optional
            If `True` (default) and the current engine is not the preferred
            engine, attempt to insert this calculation before a transfer
            upstream of the current relation, as long as this can be done
            without breaking up any locked relations or changing the resulting
            relation content.
        transfer : `bool`, optional
            If `True` (`False` is default) and the current engine is not the
            preferred engine, insert a new `Transfer` before the `Calculation`.
            If ``backtrack`` is also true, the transfer is added only if the
            backtrack attempt fails.
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
            Relation that contains the calculated column.

        Raises
        ------
        ColumnError
            Raised if the expression requires columns that are not present in
            ``target.columns``, or if `tag` is already present in
            ``target.columns``.
        EngineError
            Raised if ``require_preferred_engine=True`` and it was impossible
            to insert this operation in the preferred engine, or if the
            expression was not supported by the engine.
        RowOrderError
            Raised if ``target`` is unnecessarily ordered; see
            `Relation.expect_unordered`.
        """
        if not target.engine.preserves_order(self):
            target = target.expect_unordered(
                None
                if strip_ordering
                else (
                    f"Calculation in engine {target.engine} will not "
                    f"preserve order when applied to {target}."
                )
            )
        if not (self.expression.columns_required <= target.columns):
            raise ColumnError(
                f"Cannot calculate column {self.tag} because expression requires "
                f"columns {set(self.expression.columns_required) - target.columns} "
                f"that are not present in the target relation {target}."
            )
        if self.tag in target.columns:
            raise ColumnError(f"Calculated column {self.tag} is already present in {target}.")
        if preferred_engine is not None and preferred_engine != target.engine:
            if backtrack and (result := self._insert_recursive(target, preferred_engine, lock)):
                return result
            elif transfer:
                from ._transfer import Transfer

                target = Transfer(preferred_engine).apply(target)
            elif require_preferred_engine:
                raise EngineError(
                    f"No way to apply calculation of column {self.tag} "
                    f"with required engine {preferred_engine}."
                )
        if not self.expression.is_supported_by(target.engine):
            raise EngineError(f"Column expression {self.expression} does not support engine {target.engine}.")
        return super().apply(target, lock=lock)

    def _insert_recursive(self, target: Relation, preferred_engine: Engine, lock: bool) -> Relation | None:
        """Recursive implementation for `apply`.

        See that method's documentation for details.
        """
        from ._chain import Chain
        from ._join import Join
        from ._projection import Projection

        if target.is_locked:
            return None
        match target:
            case UnaryOperationRelation(operation=operation, target=next_target):
                if isinstance(operation, Projection):
                    # If we commute a calculation before a projection, the
                    # projection also needs to include the calculated column.
                    operation = Projection(operation.columns | {self.tag})
                if not self.columns_required <= next_target.columns:
                    return None
                if operation.is_order_dependent and not next_target.engine.preserves_order(self):
                    return None
                # If other checks above are satisfied, a Calculation also
                # commutes through:
                # - other Calculations;
                # - Deduplications (assumes calculated columns are
                #   deterministic);
                # - Marker subclasses;
                # - RowFilter subclasses;
                # - Reordering subclasses;
                # while Identity and PartialJoin never actually appear in
                # UnaryOperationRelations, as their apply() methods guarantee.
                if next_target.engine == preferred_engine:
                    return operation.apply(self.apply(next_target, lock=lock))
                elif new_target := self._insert_recursive(next_target, preferred_engine, lock):
                    return operation.apply(new_target)
            case BinaryOperationRelation(operation=operation, lhs=next_lhs, rhs=next_rhs):
                match operation:
                    case Join():

                        def _try_branch(branch: Relation) -> Relation | None:
                            if branch.columns >= self.columns_required:
                                return self._insert_recursive(branch, preferred_engine, lock)
                            return None

                        if new_join_lhs := _try_branch(next_lhs):
                            del _try_branch  # helps garbage collector a lot
                            return operation.apply(new_join_lhs, next_rhs)
                        if new_join_rhs := _try_branch(next_rhs):
                            del _try_branch
                            return operation.apply(next_lhs, new_join_rhs)
                        del _try_branch
                    case Chain():
                        new_union_lhs = self._insert_recursive(next_lhs, preferred_engine, lock)
                        new_union_rhs = self._insert_recursive(next_rhs, preferred_engine, lock)
                        if new_union_lhs and new_union_rhs:
                            return operation.apply(new_union_lhs, new_union_rhs)
        return None

    def applied_columns(self, target: Relation) -> Set[ColumnTag]:
        # Docstring inherited.
        result = set(target.columns)
        result.add(self.tag)
        return result

    def applied_min_rows(self, target: Relation) -> int:
        # Docstring inherited.
        return target.min_rows
