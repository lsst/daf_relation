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
    "SortTerm",
    "Sort",
)

import dataclasses
from collections.abc import Sequence, Set
from typing import TYPE_CHECKING, final

from .._columns import ColumnTag
from .._exceptions import ColumnError, EngineError
from .._operation_relations import UnaryOperationRelation
from .._unary_operation import Reordering

if TYPE_CHECKING:
    from .._columns import ColumnExpression
    from .._engine import Engine
    from .._relation import Relation


@dataclasses.dataclass
class SortTerm:

    expression: ColumnExpression
    ascending: bool = True

    def __str__(self) -> str:
        return f"{'' if self.ascending else '-'}{self.expression}"


@final
@dataclasses.dataclass(frozen=True)
class Sort(Reordering):
    """A relation operation that orders rows according to a sequence of
    column expressions.
    """

    terms: Sequence[SortTerm]
    """Criteria for sorting rows (`Sequence` [ `SortTerm` ])."""

    @property
    def columns_required(self) -> Set[ColumnTag]:
        # Docstring inherited.
        result: set[ColumnTag] = set()
        for term in self.terms:
            result.update(term.expression.columns_required)
        return result

    def __str__(self) -> str:
        return f"sort[{', '.join(str(term) for term in self.terms)}]"

    def apply(
        self,
        target: Relation,
        *,
        preferred_engine: Engine | None = None,
        backtrack: bool = True,
        transfer: bool = False,
        require_preferred_engine: bool = False,
        lock: bool = False,
    ) -> Relation:
        """Return a new relation that applies this sort to an existing
        relation.

        `Relation.sorted` is a convenience method that should
        be preferred to constructing and applying a `Sort` directly.

        Parameters
        ----------
        terms : `~collections.abc.Sequence` [ `SortTerm` ]
            Ordered sequence of column expressions to sort on, with whether to
            apply them in ascending or descending order.
        preferred_engine : `Engine`, optional
            Engine that the operation would ideally be performed in.  If this
            is not equal to ``self.engine``, the ``backtrack``, ``transfer``,
            and ``require_preferred_engine`` arguments control the behavior.
        backtrack : `bool`, optional
            If `True` (default) and the current engine is not the preferred
            engine, attempt to insert this sort before a transfer upstream of
            the current relation, as long as this can be done without breaking
            up any locked relations or changing the resulting relation content.
        transfer : `bool`, optional
            If `True` (`False` is default) and the current engine is not the
            preferred engine, insert a new `Transfer` before the `Sort`.  If
            ``backtrack`` is also true, the transfer is added only if the
            backtrack attempt fails.
        require_preferred_engine : `bool`, optional
            If `True` (`False` is default) and the current engine is not the
            preferred engine, raise `EngineError`.  If ``backtrack`` is also
            true, the exception is only raised if the backtrack attempt fails.
            Ignored if ``transfer`` is true.
        lock : `bool`, optional
            Set `~Relation.is_locked` on the returned relation to this value.

        Returns
        -------
        relation : `Relation`
            New relation with sorted rows.  Will be `target` if ``terms`` is
            empty.    If `target` is already a sort operation relation, the
            operations will be merged by concatenating their terms, which may
            result in duplicate sort terms that have no effect.

        Raises
        ------
        ColumnError
            Raised if any column required by a `SortTerm` is not present in
            ``target.columns``.
        EngineError
            Raised if ``require_preferred_engine=True`` and it was impossible
            to insert this operation in the preferred engine, or if a
            `SortTerm` expression was not supported by the engine.
        """
        if not self.terms:
            return target
        if preferred_engine is not None and preferred_engine != target.engine:
            if backtrack and (result := self._insert_recursive(target, preferred_engine, lock)):
                return result
            elif transfer:
                from ._transfer import Transfer

                target = Transfer(preferred_engine).apply(target)
            elif require_preferred_engine:
                raise EngineError(
                    f"No way to perform sort on {self.terms} with required engine {preferred_engine}."
                )
        for term in self.terms:
            if not term.expression.is_supported_by(target.engine):
                raise EngineError(f"Sort term {term} does not support engine {target.engine}.")
            if not term.expression.columns_required <= target.columns:
                raise ColumnError(
                    f"Sort term {term} for target relation {target} needs "
                    f"columns {set(term.expression.columns_required - target.columns)}."
                )
        match target:
            case UnaryOperationRelation(operation=Sort(terms=previous_terms), target=nested_target):
                new_terms = list(self.terms)
                for term in previous_terms:
                    if term not in new_terms:
                        new_terms.append(term)
                return Sort(new_terms).apply(nested_target, lock=lock)
        return super().apply(target, lock=lock)

    def _insert_recursive(self, target: Relation, preferred_engine: Engine, lock: bool) -> Relation | None:
        """Recursive implementation for `apply`.

        See that method's documentation for details.
        """
        if target.is_locked:
            return None
        match target:
            case UnaryOperationRelation(operation=operation, target=next_target):
                if not target.engine.preserves_order(operation):
                    return None
                if operation.is_order_dependent:
                    return None
                if next_target.engine == preferred_engine:
                    return operation.apply(self.apply(next_target, lock=lock))
                if new_target := self._insert_recursive(next_target, preferred_engine, lock=lock):
                    return operation.apply(new_target)
        return None
