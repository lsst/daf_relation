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

    def apply(
        self, lhs: Relation, rhs: Relation, *, lock: bool = False, strip_ordering: bool = False
    ) -> Relation:
        """Return a new relation that applies this operation to a pair of
        existing relations.

        `Relation.chain` is a convenience method that should be preferred to
        constructing and applying a `Chain` directly.

        Parameters
        ----------
        lhs : `Relation`
            One relation to chain.
        rhs : `Relation`
            Other relation to chain to ``lhs``.  Must have the same columns
            and engine as ``lhs``.
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
            New relation with all rows from both relations.  If the engine
            `preserves order <Engine.preserves_order>` for chains, all rows
            from ``lhs`` will appear before all rows from ``rhs``, in their
            original order.  This method never returns an operand directly,
            even if the other has ``max_rows==0``, as it is assumed that even
            relations with no rows are useful to preserve in the tree for
            `diagnostics <Diagnostics>`.

        Raises
        ------
        ColumnError
            Raised if the two relations do not have the same columns.
        EngineError
            Raised if the two relations do not have the same engine.
        RowOrderError
            Raised if ``lhs`` or ``rhs`` is unnecessarily ordered; see
            `Relation.expect_unordered`.
        """
        if not lhs.engine.preserves_order(self):
            lhs = lhs.expect_unordered(
                None
                if strip_ordering
                else f"Chain in engine {lhs.engine} will not preserve order when applied to {lhs}."
            )
        if not rhs.engine.preserves_order(self):
            rhs = rhs.expect_unordered(
                None
                if strip_ordering
                else f"Chain in engine {rhs.engine} will not preserve order when applied to {rhs}."
            )
        if lhs.engine != rhs.engine:
            raise EngineError(f"Mismatched union engines: {lhs.engine} != {rhs.engine}.")
        if lhs.columns != rhs.columns:
            raise ColumnError(f"Mismatched union columns: {set(lhs.columns)} != {set(rhs.columns)}.")
        return super().apply(lhs, rhs, lock=lock)

    def applied_columns(self, lhs: Relation, rhs: Relation) -> Set[ColumnTag]:
        # Docstring inherited.
        return lhs.columns

    def applied_min_rows(self, lhs: Relation, rhs: Relation) -> int:
        # Docstring inherited.
        return lhs.min_rows + rhs.min_rows

    def applied_max_rows(self, lhs: Relation, rhs: Relation) -> int | None:
        # Docstring inherited.
        return None if lhs.max_rows is None or rhs.max_rows is None else lhs.max_rows + rhs.max_rows
