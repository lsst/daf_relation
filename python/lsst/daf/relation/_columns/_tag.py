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

__all__ = ("ColumnTag",)

from collections.abc import Hashable
from typing import Protocol


class ColumnTag(Hashable, Protocol):
    """An interface for objects that represent columns in a relation.

    See :ref:`lsst.daf.relation-overview-column_tags` for details.
    """

    @property
    def qualified_name(self) -> str:
        """A string that is just as unique as this `ColumnTag` is, for use in
        engines that require a string for column names (`str`).

        This does not need to be limited to avoid special characters; we assume
        it can and will be appropriately quoted when necessary.
        """
        ...

    @property
    def is_key(self) -> bool:
        """Whether this column can be used for deduplication and join
        equality-constraints (`bool`).

        Columns that are not keys are assumed to always be accompanied in
        relations by related key columns that should be used instead.
        """
        ...
