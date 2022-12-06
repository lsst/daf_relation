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

__all__ = ("Materialization",)

import dataclasses
from typing import TYPE_CHECKING, Literal, final

from ._leaf_relation import LeafRelation
from ._marker_relation import MarkerRelation

if TYPE_CHECKING:
    from ._relation import Relation


@final
@dataclasses.dataclass(frozen=True, kw_only=True)
class Materialization(MarkerRelation):
    """A marker operation that indicates that the upstream tree should be
    evaluated only once, with the results saved and reused for subsequent
    processing.

    Materialization is the only provided operation for which
    `UnaryOperationRelation.is_locked` defaults to `True`.

    Also unlike most operations, the `~Relation.payload` value for a
    `Materialization` if frequently not `None`, as this is where
    engine-specific state is cached for future reuse.
    """

    name: str
    """Name to use for the cached payload within the engine (`str`)."""

    def __str__(self) -> str:
        return f"materialize[{self.name!r}]({self.target})"

    @property
    def is_locked(self) -> Literal[True]:
        # Docstring inherited.
        return True

    @classmethod
    def simplify(cls, target: Relation) -> bool:
        match target:
            case Materialization():
                return True
            case LeafRelation():
                return True
            case MarkerRelation(target=new_target):
                if target.engine == new_target.engine:
                    return cls.simplify(new_target)
        return False
