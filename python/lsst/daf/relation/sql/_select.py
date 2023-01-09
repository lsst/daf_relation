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

__all__ = ("Select",)

import dataclasses
from typing import Any

from .._exceptions import EngineError
from .._marker_relation import MarkerRelation
from .._operation_relations import BinaryOperationRelation
from .._operations import Chain, Deduplication, Projection, Slice, Sort
from .._relation import Relation
from .._unary_operation import UnaryOperation


@dataclasses.dataclass(frozen=True, kw_only=True)
class Select(MarkerRelation):
    """A marker operation used by a the SQL engine to group relation trees into
    SELECT statements.

    `Select` objects should not generally be added to relation trees by code
    outside the SQL engine itself, except via the inherited `reapply`
    interface.  Use `Engine.conform` to insert `Select` markers into an
    arbitrary relation tree (while reordering its operations accordingly).

    Notes
    -----
    A conformed SQL relation tree always starts with a `Select` relation,
    immediately followed by any projection, deduplication, sort, and slice (in
    that order) that appear within the corresponding SQL ``SELECT`` statement.
    These operations are held directly by the `Select` itself as attributes,
    and the first upstream relation that isn't one of those operations is held
    as the `skip_to` attribute.  Nested `Select` instances correspond to sets
    of relations that will appear within subqueries.  This practice allows the
    SQL engine to reduce subquery nesting and bring `Sort` operations
    downstream into the outermost ``SELECT`` statement whenever possible, since
    ``ORDER BY`` clauses in subqueries do not propagate to the order of the
    outer statement.

    The SQL engine's relation-processing algorithms typically traverse a tree
    that starts with a `Select` by recursing to `skip_to` rather than `target`,
    since the `Select` object's attributes also fully determine the operations
    between `skip_to` and `target`.  In this pattern, the `apply_skip` and
    `reapply_skip` methods are used to add possibly-modified `Select` markers
    after the upstream `skip_to` tree has been processed.

    In contrast, general relation-processing algorithms that only see the
    `Select` as an opaque `MarkerRelation` recurse via `target` and use
    `reapply` to restore a possibly-modified `Select` marker.

    The operations managed by the `Select` are always added in the same order,
    which is consistent with the order the equivalent ``SELECT`` statement
    would apply them:

    1. `Sort`
    2. `Projection`
    3. `Deduplication`
    4. `Slice`

    Note that the `Projection` needs to follow the `Sort` in order to allow
    the ``ORDER BY`` clause to reference columns that do not appear in the
    ``SELECT`` clause (otherwise these operations would commute with each other
    and the `Deduplication`).
    """

    sort: Sort
    """The sort that will be applied by the ``SELECT`` statement via an
    ``ORDER BY`` clause.

    If `Sort.terms` is empty, no ``ORDER BY`` clause will be added, and this
    `Sort` will not be applied to `skip_to` by `apply_skip` and other methods.
    """

    projection: Projection | None
    """The projection that will be applied by the ``SELECT`` statement.

    If `None` if all columns present in the `skip_to` relation should appear
    in the ``SELECT`` clause.
    """

    deduplication: Deduplication | None
    """The deduplication that will be applied by the ``SELECT`` statement.

    If not `None`, this transforms a ``SELECT`` statement into a
    ``SELECT DISTINCT``, or (if `skip_to` is a `Chain` relation) a
    ``UNION ALL`` into a ``UNION``.
    """

    slice: Slice
    """The slice that will be applied by the ``SELECT`` statement via
    ``OFFSET`` and/or ``LIMIT`` clauses.

    If `Slice.start` is zero, no ``OFFSET`` clause will be added.  If
    `Slice.stop` is `None`, no ``LIMIT`` clause will be added.  If the `Slice`
    does nothing at all, it will not be applied to `skip_to` by `apply_skip`
    and similar methods.
    """

    skip_to: Relation
    """The first relation upstream of this one that is not one of the
    operations is holds.
    """

    is_compound: bool
    """Whether this `Select` represents a SQL ``UNION`` or ``UNION ALL``.

    This is `True` if and only if `skip_to` is a `.Chain` operation relation.
    """

    def __str__(self) -> str:
        return f"select({self.target})"

    @property
    def has_sort(self) -> bool:
        """Whether there is a `Sort` between `skip_to` and `target`.
        (`bool`)"""
        return bool(self.sort.terms)

    @property
    def has_projection(self) -> bool:
        """Whether there is a `Projection` between `skip_to` and `target`
        (`bool`).
        """
        return self.projection is not None

    @property
    def has_deduplication(self) -> bool:
        """Whether there is a `Deduplication` between `skip_to` and `target`
        (`bool`).
        """
        return self.deduplication is not None

    @property
    def has_slice(self) -> bool:
        """Whether there is a `Slice` between `skip_to` and `target`
        (`bool`)."""
        return bool(self.slice.start) or self.slice.stop is not None

    def reapply(self, target: Relation, payload: Any | None = None) -> Select:
        # Docstring inherited.
        if payload is not None:
            raise EngineError("Select marker relations never have a payload.")
        if target is self.target:
            return self
        result = target.engine.conform(target)
        assert isinstance(
            result, Select
        ), "Guaranteed if a SQL engine, and Select should not appear in any other engine."
        return result

    @classmethod
    def apply_skip(
        cls,
        skip_to: Relation,
        sort: Sort | None = None,
        projection: Projection | None = None,
        deduplication: Deduplication | None = None,
        slice: Slice | None = None,
    ) -> Select:
        """Wrap a relation in a `Select` and add all of the operations it
        manages.

        Parameters
        ----------
        skip_to : `Relation`
            The relation to add the `Select` to, after first adding any
            requested operations.  This must not have any of the operation
            types managed by the `Select` class unless they are immediately
            upstream of another existing `Select`.
        sort : `Sort`, optional
            A sort to apply to `skip_to` and add to the new `Select`.
        projection : `Projection`, optional
            A projection to apply to `skip_to` and add to the new `Select`.
        deduplication : `Deduplication`, optional
            A deduplication to apply to `skip_to` and add to the new `Select`.
        slice : `Slice`, optional
            A slice to apply to `skip_to` and add to the new `Select`.

        Returns
        -------
        select : `Select`
            A relation tree terminated by a new `Select`.
        """
        target = skip_to
        if sort is None:
            sort = Sort()
        if slice is None:
            slice = Slice()
        if sort.terms:
            # In the relation tree, we need to apply the Sort before the
            # Projection in case a SortTerm depends on a column that the
            # Projection would drop.
            target = sort._finish_apply(target)
        if projection is not None:
            target = projection._finish_apply(target)
        if deduplication is not None:
            target = deduplication._finish_apply(target)
        if slice.start or slice.limit is not None:
            target = slice._finish_apply(target)
        is_compound = False
        match skip_to:
            case BinaryOperationRelation(operation=Chain()):
                is_compound = True
        return cls(
            target=target,
            projection=projection,
            deduplication=deduplication,
            sort=sort,
            slice=slice,
            skip_to=skip_to,
            is_compound=is_compound,
        )

    def reapply_skip(
        self,
        skip_to: Relation | None = None,
        after: UnaryOperation | None = None,
        **kwargs: Any,
    ) -> Select:
        """Return a modified version of this `Select`.

        Parameters
        ----------
        skip_to : `Relation`, optional
            The relation to add the `Select` to, after first adding any
            requested operations.  This must not have any of the operation
            types managed by the `Select` class unless they are immediately
            upstream of another existing `Select`.  If not provided,
            ``self.skip_to`` is used.
        after : `UnaryOperation`, optional
            A unary operation to apply to ``skip_to`` before the operations
            managed by `Select`.  Must not be one of the operattion types
            managed by `Select`.
        **kwargs
            Operations to include in the `Select`, forwarded to `apply_skip`.
            Default is to apply the same operations already in ``self``, and
            `None` can be passed to drop one of these operations.

        Returns
        -------
        select : `Select`
            Relation tree wrapped in a modified `Select`.
        """
        if skip_to is None:
            skip_to = self.skip_to
        if after is not None:
            skip_to = after._finish_apply(skip_to)
        if kwargs or skip_to is not self.skip_to:
            return Select.apply_skip(
                skip_to,
                projection=kwargs.get("projection", self.projection),
                deduplication=kwargs.get("deduplication", self.deduplication),
                sort=kwargs.get("sort", self.sort),
                slice=kwargs.get("slice", self.slice),
            )
        else:
            return self

    def strip(self) -> tuple[Relation, bool]:
        """Remove the `Select` marker and any preceding `Projection` from a
        relation if it has no other managed operations.

        Returns
        -------
        relation : `Relation`
            Upstream relation with the `Select`.
        removed_projection : `bool`
            Whether a `Projection` operation was also stripped.
        """
        if not self.has_deduplication and not self.has_sort and not self.has_slice:
            return self.skip_to, self.has_projection
        else:
            return self, False
