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

__all__ = ("Engine", "GenericConcreteEngine")

import dataclasses
import operator
import uuid
from abc import abstractmethod
from collections.abc import Hashable, Set
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from ._columns import ColumnTag

if TYPE_CHECKING:
    from ._operations import Chain
    from ._unary_operation import UnaryOperation


_F = TypeVar("_F")


class Engine(Hashable):
    """An abstract interface for the systems that hold relation data and know
    how to process relation trees.

    Most of any concrete engine's interface is not defined by the base class,
    because different engines can represent the content (or "payload") of a
    relation in very different ways.
    """

    @abstractmethod
    def get_relation_name(self, prefix: str = "leaf") -> str:
        """Return a name suitable for a new relation in this engine.

        Parameters
        ----------
        prefix : `str`, optional
            Prefix to include in the returned name.

        Returns
        -------
        name : `str`
            Name for the relation; guaranteed to be unique over all of the
            relations in this engine.
        """
        raise NotImplementedError()

    @abstractmethod
    def preserves_order(self, operation: UnaryOperation | Chain) -> bool:
        """Report whether an operation preserves order in this engine.

        Parameters
        ----------
        operation : `UnaryOperation` or `Chain`
            Operation to check; in most (but not all) cases the answer depends
            only on the relation type.  `Join` operations never preserve order
            and cannot be checked.

        Returns
        -------
        preserves_order : `bool`
            Whether this operation preserves the order of its target relation
            or relations when it acts in this engine.

        Notes
        -----
        `Reordering` operations are never considered to preserve order.

        The answer for `Transfer` operations depends on both the target engine
        and the transfer's own "destination" engine; implementations should
        delegate to ``operation.destination.preserves_order(operation)`` if and
        only if ``destination != self`` (to avoid infinite recursion), unless
        transfers from this engine never preserve order.
        """
        raise NotImplementedError()

    def get_join_identity_payload(self) -> Any:
        """Return a `~Relation.payload` for a leaf relation that is the
        `join identity <Relation.is_join_identity>`.

        Returns
        -------
        payload
            The engine-specific content for this relation.
        """
        return None

    def get_doomed_payload(self, columns: Set[ColumnTag]) -> Any:
        """Return a `~Relation.payload` for a leaf relation that has no rows.

        Parameters
        ----------
        columns : `~collections.abc.Set` [ `ColumnTag` ]
            The columns the relation should have.

        Returns
        -------
        payload
            The engine-specific content for this relation.
        """
        return None


@dataclasses.dataclass(repr=False, eq=False, kw_only=True)
class GenericConcreteEngine(Engine, Generic[_F]):
    """An implementation-focused base class for `Engine` objects

    This class provides common functionality for the provided `iteration` and
    `sql` engines.  It may be used in external engine implementations as well.
    """

    name: str
    """Name of the engine; primarily used for display purposes (`str`).
    """

    functions: dict[str, _F] = dataclasses.field(default_factory=dict)
    """A mapping of engine-specific callables that are used to satisfy
    `ColumnFunction` and `PredicateFunction` name lookups.
    """

    relation_name_counter: int = 0
    """An integer counter used to generate relation names (`int`).
    """

    def __str__(self) -> str:
        return self.name

    def __hash__(self) -> int:
        return id(self)

    def __eq__(self, other: Any) -> bool:
        return self is other

    def get_relation_name(self, prefix: str = "leaf") -> str:
        """Return a name suitable for a new relation in this engine.

        Parameters
        ----------
        prefix : `str`, optional
            Prefix to include in the returned name.

        Returns
        -------
        name : `str`
            Name for the relation; guaranteed to be unique over all of the
            relations in this engine.

        Notes
        -----
        This implementation combines the given prefix with both the current
        `relation_name_counter` value and a random hexadecimal suffix.
        """
        name = f"{prefix}_{self.relation_name_counter:04d}_{uuid.uuid4().hex}"
        self.relation_name_counter += 1
        return name

    def get_function(self, name: str) -> _F | None:
        """Return the named column expression function.

        Parameters
        ----------
        name : `str`
            Name of the function, from `ColumnFunction.name` or
            `PredicateFunction.name`

        Returns
        -------
        function
            Engine-specific callable, or `None` if no match was found.

        Notes
        -----
        This implementation first looks for a symbol with this name in the
        built-in `operator` module, to handle the common case (shared by both
        the `iteration` and `sql` engines) where these functions are
        appropriate for the engine due to operator overloading.  When this
        fails, the name is looked up in the `functions` attribute.
        """
        return getattr(operator, name, self.functions.get(name))
