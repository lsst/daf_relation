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

__all__ = ("Slice",)

import dataclasses
from collections.abc import Set
from typing import TYPE_CHECKING, Literal, final

from .._columns import ColumnTag
from .._operation_relations import UnaryOperationRelation
from .._unary_operation import RowFilter

if TYPE_CHECKING:
    from .._relation import Relation


@final
@dataclasses.dataclass(frozen=True)
class Slice(RowFilter):
    """A relation relation that filters rows that are outside a range of
    positional indices.
    """

    start: int = 0
    """First index to include the output relation (`int`).
    """

    stop: int | None = None
    """One past the last index to include in the output relation
    (`int` or `None`).
    """

    @property
    def limit(self) -> int | None:
        """The maximum number of rows to include (`int` or `None`)."""
        return None if self.stop is None else self.stop - self.start

    @property
    def columns_required(self) -> Set[ColumnTag]:
        # Docstring inherited.
        return frozenset()

    @property
    def is_empty_invariant(self) -> Literal[False]:
        # Docstring inherited.
        return False

    @property
    def is_order_dependent(self) -> Literal[True]:
        # Docstring inherited.
        return True

    @property
    def is_count_dependent(self) -> bool:
        # Docstring inherited.
        return True

    def __str__(self) -> str:
        return f"slice[{self.start}:{self.stop}]"

    def apply(self, target: Relation, lock: bool = False) -> Relation:
        """Return a new relation that applies this slice to an existing
        relation.

        Relation indexing with a `slice` object constructs and applies a
        `Slice` object, and should be preferred to calling this method
        directly.

        Parameters
        ----------
        target : `Relation`
            Relation this operation will act upon.
        lock : `bool`, optional
            Set `~Relation.is_locked` on the returned relation to this value.

        Returns
        -------
        relation : `Relation`
            New relation with only the rows between `start` and `stop`.  May be
            ``target`` if ``start=0`` and ``stop=None``.  If ``target`` is
            already a slice operation relation, the operations will be merged.
        """
        if not self.start and self.stop is None:
            return target
        match target:
            case UnaryOperationRelation(
                operation=Slice(start=previous_start, stop=previous_stop), target=nested_target
            ):
                new_start = previous_start + self.start
                if previous_stop is None:
                    if self.stop is None:
                        new_stop = None
                    else:
                        new_stop = self.stop + previous_start
                else:
                    if self.stop is None:
                        new_stop = previous_stop
                    else:
                        new_stop = min(previous_stop, self.stop + previous_start)
                return Slice(new_start, new_stop).apply(nested_target, lock=lock)
        return super().apply(target, lock=lock)

    def applied_min_rows(self, target: Relation) -> int:
        # Docstring inherited.
        if self.stop is not None:
            return min(self.stop - self.start, target.min_rows)
        else:
            return target.min_rows
