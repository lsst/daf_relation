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
from typing import TYPE_CHECKING, final

from .._leaf_relation import LeafRelation
from .._operation_relations import UnaryOperationRelation
from .._unary_operation import Marker

if TYPE_CHECKING:
    from .._relation import Relation


@final
@dataclasses.dataclass(frozen=True)
class Materialization(Marker):
    """A marker operation that indicates that the upstream tree should be
    evaluated only once, with the results saved and reused for subsequent
    processing.

    Materialization is the only provided operation for which
    `UnaryOperationRelation.is_locked` defaults to `True`.

    Also unlike most operations, the `~Relation.payload` value for a
    `Materialization` if frequently not `None`, as this is where
    engine-specific state is cached for future reuse.
    """

    name: str | None = None
    """Name to use for the cached payload within the engine (`str` or `None`).

    If `None` provided, a name will be created via a call to
    `Engine.get_relation_name` in `apply`.
    """

    def __str__(self) -> str:
        return f"materialize[{self.name!r}]" if self.name else "materialize"

    def apply(
        self,
        target: Relation,
        *,
        name_prefix: str = "materialization",
        lock: bool = True,
        strip_ordering: bool = False,
    ) -> Relation:
        """Apply this operation to the given target relation, indicating that
        its payload should be cached.

        Parameters
        ----------
        target : `Relation`
            Relation the operation will act upon.
        name_prefix : `str`, optional
            Prefix to pass to `Engine.get_relation_name`; ignored if ``name``
            is provided.
        lock : `bool`, optional
            Set `~Relation.is_locked` on the returned relation to this value.
            Unlike most operations, `Materialization` relations are locked by
            default, since they reflect user intent to mark a specific tree as
            cacheable.
        strip_ordering : `bool`, optional
            If `True`, remove upstream operations that impose row ordering when
            the application of this operation makes that ordering unnecessary;
            if `False` (default) raise `RowOrderError` instead (see
            `expect_unordered`).

        Returns
        -------
        relation : `Relation`
            New relation that marks its upstream tree for caching.  May be
            ``self`` if it is already a `LeafRelation` or another
            materialization (in which case the given name or name prefix will
            be ignored).

        Raises
        ------
        RowOrderError
            Raised if ``self`` is unnecessarily ordered; see
            `Relation.expect_unordered`.

        See Also
        --------
        Processor.materialize
        """
        match target:
            case LeafRelation() as leaf:
                return leaf
            case UnaryOperationRelation(operation=Materialization()) as existing:
                return existing
        if not target.engine.preserves_order(self):
            target = target.expect_unordered(
                None
                if strip_ordering
                else (
                    f"Materialization in engine {target.engine} will not preserve "
                    f"order when applied to {target}."
                )
            )
        if self.name is None:
            name = target.engine.get_relation_name(name_prefix)
            operation = dataclasses.replace(self, name=name)
        else:
            name = self.name
            operation = self
        return Marker.apply(operation, target, lock=lock)
