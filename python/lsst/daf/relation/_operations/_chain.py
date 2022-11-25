# This file is part of daf_relation.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the relations of the GNU General Public License as published by
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

__all__ = ("Chain",)

import dataclasses
from collections.abc import Set
from typing import TYPE_CHECKING, final

from .._binary_operation import BinaryOperation
from .._columns import ColumnTag
from .._exceptions import ColumnError, EngineError

if TYPE_CHECKING:
    from .._relation import Relation


@final
@dataclasses.dataclass(frozen=True)
class Chain(BinaryOperation):
    """A relation operation that concatenates the rows of a pair of relations
    with the same columns."""

    def __str__(self) -> str:
        return "∪"

    def _begin_apply(self, lhs: Relation, rhs: Relation) -> BinaryOperation:
        # Docstring inherited.
        if lhs.engine != rhs.engine:
            raise EngineError(f"Mismatched chain engines: {lhs.engine} != {rhs.engine}.")
        if lhs.columns != rhs.columns:
            raise ColumnError(f"Mismatched chain columns: {set(lhs.columns)} != {set(rhs.columns)}.")
        return self

    def applied_columns(self, lhs: Relation, rhs: Relation) -> Set[ColumnTag]:
        # Docstring inherited.
        return lhs.columns

    def applied_min_rows(self, lhs: Relation, rhs: Relation) -> int:
        # Docstring inherited.
        return lhs.min_rows + rhs.min_rows

    def applied_max_rows(self, lhs: Relation, rhs: Relation) -> int | None:
        # Docstring inherited.
        return None if lhs.max_rows is None or rhs.max_rows is None else lhs.max_rows + rhs.max_rows
