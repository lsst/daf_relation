# This file is part of daf_relation.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
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
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

from __future__ import annotations

__all__ = ("Payload",)

import dataclasses
from typing import Generic, TypeVar

import sqlalchemy

from .._columns import ColumnTag

_L = TypeVar("_L")


@dataclasses.dataclass(eq=False)
class Payload(Generic[_L]):
    """A struct that represents a SQL table or simple ``SELECT`` query via
    SQLAlchemy objects.
    """

    from_clause: sqlalchemy.sql.FromClause
    """SQLAlchemy representation of the FROM clause or table
    (`sqlalchemy.sql.FromClause`).
    """

    where: list[sqlalchemy.sql.ColumnElement] = dataclasses.field(default_factory=list)
    """SQLAlchemy representation of the WHERE clause, as a sequence of
    boolean expressions to be combined with ``AND``
    (`Sequence` [ `sqlalchemy.sql.ColumnElement` ]).
    """

    columns_available: dict[ColumnTag, _L] = dataclasses.field(default_factory=dict)
    """Mapping from `.ColumnTag` to logical column for the columns available
    from the FROM clause (`dict`).
    """

    def copy(self) -> Payload[_L]:
        """Return a copy of this struct that can safely be modified.

        This method takes care to copy mutable attributes while leaving
        immutable objects alone.
        """
        return dataclasses.replace(
            self, where=list(self.where), columns_available=dict(self.columns_available)
        )
