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
from .._exceptions import ColumnError
from .._operation_relations import UnaryOperationRelation
from .._unary_operation import Identity, UnaryCommutator, UnaryOperation

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

    columns: frozenset[ColumnTag]
    """The columns to be kept (`frozenset` [ `ColumnTag` ]).
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

    def _begin_apply(
        self, target: Relation, preferred_engine: Engine | None
    ) -> tuple[UnaryOperation, Engine]:
        if self.columns == target.columns:
            return Identity(), target.engine
        if not self.columns <= target.columns:
            raise ColumnError(
                f"Cannot project column(s) {set(self.columns) - target.columns} "
                f"that are not present in the target relation {target}."
            )
        return super()._begin_apply(target, preferred_engine)

    def _finish_apply(self, target: Relation) -> Relation:
        if self.columns == target.columns:
            return target
        return super()._finish_apply(target)

    def applied_columns(self, target: Relation) -> Set[ColumnTag]:
        # Docstring inherited.
        return self.columns

    def applied_min_rows(self, target: Relation) -> int:
        # Docstring inherited.
        return target.min_rows

    def commute(self, current: UnaryOperationRelation) -> UnaryCommutator:
        # Docstring inherited.
        from ._calculation import Calculation

        commuted_columns: frozenset[ColumnTag] = self.columns
        match current.operation:
            case Projection():
                # We can just drop any existing Projection as this one
                # supersedes it; by construction the new one has a
                # subset of the original's columns.
                return UnaryCommutator(first=self, second=Identity())
            case Calculation(tag=tag):
                if tag not in self.columns:
                    # Projection will drop the column added by the
                    # Calculation, so it might as well have never
                    # existed.
                    return UnaryCommutator(first=self, second=Identity())
                else:
                    commuted_columns -= {tag}
        if not commuted_columns >= current.operation.columns_required:
            # Can't move the entire projection past this operation;
            # move what we can, and return the full Projection as the
            # "remainder".
            return UnaryCommutator(
                first=Projection(commuted_columns | current.operation.columns_required),
                second=current.operation,
                done=False,
                messages=(
                    f"{current.operation} requires columns "
                    f"{set(current.operation.columns_required - self.columns)}",
                ),
            )
        return UnaryCommutator(Projection(commuted_columns), current.operation)

    def simplify(self, upstream: UnaryOperation) -> UnaryOperation | None:
        # Docstring inherited.
        from ._calculation import Calculation

        # See similar checks in commute for explanations.
        match upstream:
            case Projection():
                return self
            case Calculation(tag=tag) if tag not in self.columns:
                return self
        return None
