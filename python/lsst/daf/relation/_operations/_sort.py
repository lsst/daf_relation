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
from collections.abc import Set
from typing import TYPE_CHECKING, final

from .._columns import ColumnTag
from .._exceptions import ColumnError
from .._operation_relations import UnaryOperationRelation
from .._unary_operation import Identity, Reordering, UnaryCommutator, UnaryOperation

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

    terms: tuple[SortTerm, ...] = ()
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

    def is_supported_by(self, engine: Engine) -> bool:
        # Docstring inherited.
        return all(term.expression.is_supported_by(engine) for term in self.terms)

    def _begin_apply(
        self, target: Relation, preferred_engine: Engine | None
    ) -> tuple[UnaryOperation, Engine]:
        # Docstring inherited.
        if not self.terms:
            return Identity(), target.engine
        for term in self.terms:
            if not term.expression.columns_required <= target.columns:
                raise ColumnError(
                    f"Sort term {term} for target relation {target} needs "
                    f"columns {set(term.expression.columns_required - target.columns)}."
                )
        return super()._begin_apply(target, preferred_engine)

    def _finish_apply(self, target: Relation) -> Relation:
        # Docstring inherited.
        if not self.terms:
            return target
        return super()._finish_apply(target)

    def then(self, next: Sort) -> Sort:
        """Compose this sort with another one.

        Parameters
        ----------
        next : `Sort`
            Sort that acts after ``self``.

        Returns
        -------
        composition : `Sort`
            Sort that is equivalent to ``self`` and ``next`` being applied
            back-to-back.
        """
        new_terms = list(next.terms)
        for term in self.terms:
            if term not in new_terms:
                new_terms.append(term)
        return Sort(tuple(new_terms))

    def commute(self, current: UnaryOperationRelation) -> UnaryCommutator:
        # Docstring inherited.
        if not self.columns_required <= current.target.columns:
            return UnaryCommutator(
                first=None,
                second=current.operation,
                done=False,
                messages=(
                    f"{current.target} is missing columns "
                    f"{set(self.columns_required - current.target.columns)}",
                ),
            )
        if current.operation.is_order_dependent:
            return UnaryCommutator(
                first=None,
                second=current.operation,
                done=False,
                messages=(f"{current.operation} is order-dependent",),
            )
        return UnaryCommutator(self, current.operation)

    def simplify(self, upstream: UnaryOperation) -> UnaryOperation | None:
        # Docstring inherited.
        if not self.terms:
            return upstream
        match upstream:
            case Sort():
                return upstream.then(self)
        return None
