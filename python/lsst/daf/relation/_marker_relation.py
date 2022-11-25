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

__all__ = ("MarkerRelation",)

import dataclasses
from collections.abc import Set
from typing import TYPE_CHECKING, Any, final

from ._relation import BaseRelation, Relation

if TYPE_CHECKING:
    from ._columns import ColumnTag
    from ._engine import Engine


@dataclasses.dataclass(frozen=True)
class MarkerRelation(BaseRelation):
    """An extensible relation base class that provides additional information
    about another relation without changing its row-and-column content.

    As with all other relation types, `MarkerRelation` subclasses should be
    frozen dataclasses.  Since `MarkerRelation` is itself a dataclass, it is
    not formally an abstract base class, but it should be considered one
    conceptually.
    """

    target: Relation
    """The target relation the marker wraps (`Relation`).
    """

    payload: Any = dataclasses.field(repr=False, compare=False, default=None)
    """The engine-specific contents of the relation."""

    @final
    @property
    def columns(self) -> Set[ColumnTag]:
        """The columns in this relation
        (`~collections.abc.Set` [ `ColumnTag` ] ).
        """
        return self.target.columns

    @property
    def engine(self) -> Engine:
        """The engine that is responsible for interpreting this relation
        (`Engine`).
        """
        return self.target.engine

    @property
    def is_locked(self) -> bool:
        """Whether this relation and those upstream of it should be considered
        fixed by tree-manipulation algorithms (`bool`).
        """
        return False

    @final
    @property
    def min_rows(self) -> int:
        """The minimum number of rows this relation might have (`int`)."""
        return self.target.min_rows

    @final
    @property
    def max_rows(self) -> int | None:
        """The maximum number of rows this relation might have (`int` or
        `None`).

        This is `None` for relations whose size is not bounded from above.
        """
        return self.target.max_rows

    def attach_payload(self: Relation, payload: Any) -> None:
        """Attach an engine-specific `payload` to this relation.

        This method may be called exactly once on a `MarkerRelation` instance
        that was not initialized with a `payload`, despite the fact that
        `Relation` objects are otherwise considered immutable.

        Parameters
        ----------
        payload
            Engine-specific content to attach.

        Raises
        ------
        TypeError
            Raised if this relation already has a payload, or if this marker
            subclass can never have a payload.  `TypeError` is used here for
            consistency with other attempts to assign to an attribute of an
            immutable object.
        """
        if self.payload is None:
            object.__setattr__(self, "payload", payload)
        else:
            raise TypeError(
                f"Cannot attach payload {payload} to relation {self} with existing payload "
                f"{self.payload}; relation payloads are write-once."
            )

    def reapply(self, target: Relation, payload: Any | None = None) -> MarkerRelation:
        """Mark a new target relation, returning a new instance of the same
        type.

        Parameters
        ----------
        target : `Relation`
            New relation to mark.
        payload, optional
            Payload to attach to the new relation.

        Returns
        -------
        relation : `MarkerRelation`
            A new relation with the given target.

        Notes
        -----
        This method is primarily intended for use by operations that "unroll"
        a relation tree to perform some modification upstream and then "replay"
        the operations and markers that were downstream.  `MarkerRelation`
        implementations with state that depends on the target will need to
        override this method to update that state accordingly.
        """
        if target is self.target and payload is self.payload:
            return self
        return dataclasses.replace(self, target=target, payload=payload)
