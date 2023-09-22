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

__all__ = ("LeafRelation",)

import dataclasses
from collections.abc import Sequence, Set
from typing import TYPE_CHECKING, Any, Literal, final

from ._relation import BaseRelation

if TYPE_CHECKING:
    from ._columns import ColumnTag
    from ._engine import Engine


@final
@dataclasses.dataclass(frozen=True)
class LeafRelation(BaseRelation):
    """A `Relation` class that represents direct storage of rows, rather than
    an operation on some other relation.
    """

    engine: Engine = dataclasses.field(repr=False, compare=True)
    """The engine that is responsible for interpreting this relation
    (`Engine`).
    """

    columns: frozenset[ColumnTag] = dataclasses.field(repr=False, compare=True)
    """The columns in this relation (`~collections.abc.Set` [ `ColumnTag` ] ).
    """

    payload: Any = dataclasses.field(repr=False, compare=False)
    """The engine-specific contents of the relation."""

    name: str = dataclasses.field(repr=True, compare=True, default="")
    """Name used to identify and reconstruct this relation (`str`)."""

    name_prefix: dataclasses.InitVar[str] = "leaf"
    """Prefix used when calling `Engine.get_relation_name` when `name` is not
    provided (`str`).
    """

    messages: Sequence[str] = dataclasses.field(repr=False, compare=False, default=())
    """Messages for use when processing the relation with the `Diagnostics`
    class or similar algorithms (`~collections.abc.Sequence` [ `str` ]).

    This is typically used to explain why a leaf relation has no rows when
    ``max_rows==0``; see `make_doomed`.
    """

    parameters: Any = dataclasses.field(repr=True, compare=True, default=None)
    """Extra data used to uniquely identify and/or reconstruct this relation.
    """

    min_rows: int = dataclasses.field(repr=False, compare=False, default=0)
    """The minimum number of rows this relation might have (`int`)."""

    max_rows: int | None = dataclasses.field(repr=False, compare=False, default=None)
    """The maximum number of rows this relation might have (`int` or `None`).
    """

    @property
    def is_locked(self) -> Literal[True]:
        """Whether this relation and those upstream of it should be considered
        fixed by tree-manipulation algorithms (`bool`).

        See `Relation.is_locked`.
        """
        return True

    def __post_init__(self, name_prefix: str = "leaf") -> None:
        if not self.name:
            object.__setattr__(self, "name", self.engine.get_relation_name(name_prefix))
        if self.max_rows is not None and self.max_rows < self.min_rows:
            raise ValueError(f"max_rows ({self.max_rows}) < min_rows ({self.min_rows})")

    @classmethod
    def make_doomed(
        cls, engine: Engine, columns: Set[ColumnTag], messages: Sequence[str], name: str = "0"
    ) -> LeafRelation:
        """Construct a leaf relation with no rows and one or more messages
        explaining why.

        Parameters
        ----------
        engine : `Engine`
            The engine that is responsible for interpreting this relation.
        columns : `~collections.abc.Set` [ `ColumnTag` ]
            The columns in this relation.
        messages : `~collections.abc.Sequence` [ `str` ]
            One or more messages explaining why the relation has no rows.
        name : `str`, optional
            Name used to identify and reconstruct this relation.

        Returns
        -------
        relation : `LeafRelation`
            Doomed leaf relation.
        """
        return LeafRelation(
            engine=engine,
            columns=frozenset(columns),
            min_rows=0,
            max_rows=0,
            payload=engine.get_doomed_payload(columns),
            name=name,
            messages=messages,
        )

    @classmethod
    def make_join_identity(cls, engine: Engine, name: str = "I") -> LeafRelation:
        """Construct a leaf relation with no columns and exactly one row.

        Parameters
        ----------
        engine : `Engine`
            The engine that is responsible for interpreting this relation.
        name : `str`, optional
            Name used to identify and reconstruct this relation.

        Returns
        -------
        relation : `LeafRelation`
            Leaf relation with no columns and one row.
        """
        return LeafRelation(
            engine=engine,
            columns=frozenset(),
            min_rows=1,
            max_rows=1,
            payload=engine.get_join_identity_payload(),
            name=name,
        )

    def __str__(self) -> str:
        return self.name if self.parameters is None else f"{self.name}({self.parameters})"
