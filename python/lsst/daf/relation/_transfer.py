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

from ._engine import Engine
from ._marker_relation import MarkerRelation

if TYPE_CHECKING:
    from ._relation import Relation


@final
@dataclasses.dataclass(frozen=True, kw_only=True)
class Transfer(MarkerRelation):
    """A `MarkerRelation` operation that represents moving relation content
    from one engine to another.

    A single `Engine` cannot generally process a relation tree that contains
    transfers.  The `Processor` class provides a framework for handling these
    trees.
    """

    destination: Engine
    """Engine the target relation content will be transferred to (`Engine`).
    """

    @property
    def engine(self) -> Engine:
        # Docstring inherited.
        return self.destination

    def __str__(self) -> str:
        return f"→[{self.destination}]({self.target})"

    @classmethod
    def simplify(cls, target: Relation, destination: Engine) -> Relation | None:
        if target.is_locked:
            return None
        match target:
            case Transfer(target=new_target):
                if destination == new_target.engine:
                    return new_target
                else:
                    return cls.simplify(new_target, destination)
            case MarkerRelation(target=new_target):
                return cls.simplify(new_target, destination)
        return None
