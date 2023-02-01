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
from typing import TYPE_CHECKING, Literal, final

from .._operation_relations import UnaryOperationRelation
from .._unary_operation import UnaryCommutator, UnaryOperation

if TYPE_CHECKING:
    from .._relation import Relation


@final
@dataclasses.dataclass(frozen=True)
class Deduplication(UnaryOperation):
    """A relation operation that removes duplicate rows."""

    @property
    def is_count_invariant(self) -> Literal[False]:
        # Docstring inherited.
        return False

    @property
    def is_empty_invariant(self) -> Literal[True]:
        # Docstring inherited.
        return True

    def __str__(self) -> str:
        return "deduplicate"

    def applied_min_rows(self, target: Relation) -> int:
        # Docstring inherited.
        return 1

    def commute(self, current: UnaryOperationRelation) -> UnaryCommutator:
        # Docstring inherited.
        # Deduplication does not commute through Projection, but this is a bit
        # more defensive to guard against what a Projection does that's
        # problematic, rather than just checking isinstance(Projection).
        if not current.columns >= current.target.columns:
            return UnaryCommutator(
                first=None,
                second=current.operation,
                done=False,
                messages=(
                    "deduplication columns would change from "
                    f"{set(current.columns)} to {set(current.target.columns)}",
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
