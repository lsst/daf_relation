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

__all__ = ("Transfer",)

import dataclasses
from typing import TYPE_CHECKING, final

from .._engine import Engine
from .._unary_operation import Marker

if TYPE_CHECKING:
    from .._relation import Relation


@final
@dataclasses.dataclass(frozen=True)
class Transfer(Marker):
    """A `Marker` operation that representings moving relation content from
    one engine to another.

    A single `Engine` cannot generally process a relation tree that contains
    transfers.  The `Processor` class provides a framework for handling these
    trees.
    """

    destination: Engine
    """Engine the relation content will be transferred to (`Engine`).
    """

    def __str__(self) -> str:
        return f"→[{self.destination}]"

    def apply(self, target: Relation, lock: bool = False, strip_ordering: bool = False) -> Relation:
        """Return a new relation that applies this transfer to an existing
        relation.

        `Relation.transferred_to` is a convenience method that should
        be preferred to constructing and applying a `Transfer` directly.

        Parameters
        ----------
        target : `Relation`
            Relation this operation will act upon.
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
            New relation in the given engine.  Will be `target` if
            ``target.engine == destination``.

        Raises
        ------
        RowOrderError
            Raised if `target` is unnecessarily ordered; see
            `Relation.expect_unordered`.
        """
        if target.engine == self.destination:
            return target
        if not target.engine.preserves_order(self):
            target = target.expect_unordered(
                None
                if strip_ordering
                else (
                    f"Transfer from engine {target.engine} to {self.destination} will not preserve "
                    f"order when applied to {target}."
                )
            )
        return super().apply(target, lock=lock)

    def applied_engine(self, target: Relation) -> Engine:
        # Docstring inherited.
        return self.destination
