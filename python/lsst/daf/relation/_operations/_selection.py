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
from .._exceptions import ColumnError
from .._operation_relations import UnaryOperationRelation
from .._unary_operation import Identity, RowFilter, UnaryCommutator, UnaryOperation

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

    def is_supported_by(self, engine: Engine) -> bool:
        # Docstring inherited.
        return self.predicate.is_supported_by(engine)

    def _begin_apply(
        self, target: Relation, preferred_engine: Engine | None
    ) -> tuple[UnaryOperation, Engine]:
        # Docstring inherited.
        if self.predicate.as_trivial() is True:
            return Identity(), target.engine
        # We don't simplify the trivially-false predicate case, in keeping with
        # our policy of leaving doomed relations in place for diagnostics
        # to report on later.
        if not self.predicate.columns_required <= target.columns:
            raise ColumnError(
                f"Predicate {self.predicate} for target relation {target} needs "
                f"columns {self.predicate.columns_required - target.columns}."
            )
        return super()._begin_apply(target, preferred_engine)

    def _finish_apply(self, target: Relation) -> Relation:
        # Docstring inherited.
        if self.predicate.as_trivial() is True:
            return target
        else:
            return super()._finish_apply(target)

    def applied_min_rows(self, target: Relation) -> int:
        # Docstring inherited.
        return 0

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
        if current.operation.is_count_dependent:
            return UnaryCommutator(
                first=None,
                second=current.operation,
                done=False,
                messages=(f"{current.operation} is count-dependent",),
            )
        return UnaryCommutator(self, current.operation)

    def simplify(self, upstream: UnaryOperation) -> UnaryOperation | None:
        # Docstring inherited.
        match upstream:
            case Selection(predicate=other_predicate):
                return Selection(predicate=other_predicate.logical_and(self.predicate))
        return None
