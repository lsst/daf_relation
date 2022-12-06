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
from .._exceptions import ColumnError
from .._operation_relations import UnaryOperationRelation
from .._unary_operation import UnaryCommutator, UnaryOperation

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
        return f"+[{self.tag!s}={self.expression!s}]"

    def is_supported_by(self, engine: Engine) -> bool:
        # Docstring inherited.
        return self.expression.is_supported_by(engine)

    def _begin_apply(
        self, target: Relation, preferred_engine: Engine | None
    ) -> tuple[UnaryOperation, Engine]:
        # Docstring inherited.
        if not (self.expression.columns_required <= target.columns):
            raise ColumnError(
                f"Cannot calculate column {self.tag} because expression requires "
                f"columns {set(self.expression.columns_required) - target.columns} "
                f"that are not present in the target relation {target}."
            )
        if self.tag in target.columns:
            raise ColumnError(f"Calculated column {self.tag} is already present in {target}.")
        return super()._begin_apply(target, preferred_engine)

    def applied_columns(self, target: Relation) -> Set[ColumnTag]:
        # Docstring inherited.
        result = set(target.columns)
        result.add(self.tag)
        return result

    def applied_min_rows(self, target: Relation) -> int:
        # Docstring inherited.
        return target.min_rows

    def commute(self, current: UnaryOperationRelation) -> UnaryCommutator:
        # Docstring inherited.
        from ._projection import Projection

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
        # If we commute a calculation before a projection, the
        # projection also needs to include the calculated column.
        return UnaryCommutator(
            self,
            (
                Projection(current.operation.columns | {self.tag})
                if isinstance(current.operation, Projection)
                else current.operation
            ),
        )
