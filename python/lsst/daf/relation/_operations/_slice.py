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
from .._unary_operation import Identity, RowFilter, UnaryCommutator, UnaryOperation

if TYPE_CHECKING:
    from .._engine import Engine
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

    def __post_init__(self) -> None:
        if self.start < 0:
            raise ValueError(f"Slice start {self.start} is negative.")
        if self.stop is not None and self.stop < self.start:
            raise ValueError(f"Slice stop {self.stop} is less than its start {self.start}.")

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

    def _begin_apply(
        self, target: Relation, preferred_engine: Engine | None
    ) -> tuple[UnaryOperation, Engine]:
        # Docstring inherited.
        if not self.start and self.stop is None:
            return Identity(), target.engine
        return super()._begin_apply(target, preferred_engine)

    def _finish_apply(self, target: Relation) -> Relation:
        # Docstring inherited.
        if not self.start and self.stop is None:
            return target
        return super()._finish_apply(target)

    def then(self, next: Slice) -> Slice:
        """Compose this slice with another one.

        Parameters
        ----------
        next : `Slice`
            Slice that acts after ``self``.

        Returns
        -------
        composition : `Slice`
            Slice that is equivalent to ``self`` and ``next`` being applied
            back-to-back.
        """
        new_start = self.start + next.start
        if self.stop is None:
            if next.stop is None:
                new_stop = None
            else:
                new_stop = next.stop + self.start
        else:
            if next.stop is None:
                new_stop = self.stop
            else:
                new_stop = min(self.stop, next.stop + self.start)
        return Slice(new_start, new_stop)

    def applied_min_rows(self, target: Relation) -> int:
        # Docstring inherited.
        if self.stop is not None:
            stop = min(self.stop, target.min_rows)
        else:
            stop = target.min_rows
        return max(stop - self.start, 0)

    def applied_max_rows(self, target: Relation) -> int | None:
        # Docstring inherited.
        if self.stop is not None:
            if target.max_rows is not None:
                stop = min(self.stop, target.max_rows)
            else:
                stop = self.stop
        else:
            if target.max_rows is not None:
                stop = target.max_rows
            else:
                return None
        return max(stop - self.start, 0)

    def commute(self, current: UnaryOperationRelation) -> UnaryCommutator:
        # Docstring inherited.
        from ._calculation import Calculation
        from ._projection import Projection

        match current.operation:
            case Projection() | Calculation():
                return UnaryCommutator(first=self, second=current.operation)
            case _:
                return UnaryCommutator(
                    first=None,
                    second=current.operation,
                    done=False,
                    messages=(
                        f"Slice only commutes with Projection and Calculation, not {current.operation}",
                    ),
                )

    def simplify(self, upstream: UnaryOperation) -> UnaryOperation | None:
        # Docstring inherited.
        if not self.start and self.stop is None:
            return upstream
        match upstream:
            case Slice():
                return upstream.then(self)
        return None
