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
from collections.abc import Hashable, Sequence, Set
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from ._columns import ColumnTag
from ._exceptions import EngineError

if TYPE_CHECKING:
    from ._binary_operation import BinaryOperation
    from ._relation import Relation
    from ._unary_operation import UnaryOperation


_F = TypeVar("_F")


class Engine(Hashable):
    """An abstract interface for the systems that hold relation data and know
    how to process relation trees.

    Notes
    -----
    A key part of any concrete engine's interface is not defined by the base
    class, because different engines can represent the content (or "payload")
    of a relation in very different ways.

    Engines can impose their own invariants on the structure of a relation
    tree, by implementing `conform`.  They can also maintain these invariants
    when new operations are added to the tree by implementing `append_unary`
    and `append_binary`, though any derived implementations of base-class
    methods that accept relation arguments should always conform them.
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

    def conform(self, relation: Relation) -> Relation:
        """Ensure a relation tree satisfies this engine's invariants.

        This can include reordering operations (in a way consistent with their
        commutators) and/or inserting `MarkerRelation` nodes.

        Parameters
        ----------
        relation : `Relation`
            Original relation tree.

        Returns
        -------
        conformed : `Relation`
            Relation tree that satisfies this engine's invariants.

        Notes
        -----
        The default implementation returns the given relation.  Engines with a
        non-trivial `conform` implementation should always call it on any
        relations they are passed, as algorithms that process the relation tree
        are not guaranteed to maintain those invariants themselves.  It is
        recommended to use a custom `MarkerRelation` to indicate trees that
        satisfy invariants, allowing the corresponding `conform` implementation
        to short-circuit quickly.
        """
        return relation

    def materialize(
        self, target: Relation, name: str | None = None, name_prefix: str = "materialization_"
    ) -> Relation:
        """Mark that a target relation's payload should be cached.

        Parameters
        ----------
        target : `Relation`
            Relation to mark.
        name : `str`, optional
            Name to use for the cached payload within the engine.
        name_prefix : `str`, optional
            Prefix to pass to `get_relation_name`; ignored if ``name``
            is provided.

        Returns
        -------
        relation : `Relation`
            New relation that marks its upstream tree for caching, unless
            the materialization was simplified away.

        Notes
        -----
        The base class implementation calls `Materialization.simplify` to avoid
        materializations of leaf relations or other materializations.  Override
        implementations should generally do the same.

        See Also
        --------
        Processor.materialize
        """
        from ._materialization import Materialization

        if Materialization.simplify(target):
            return target
        if name is None:
            name = target.engine.get_relation_name(name_prefix)
        return Materialization(target=target, name=name)

    def transfer(self, target: Relation, payload: Any | None = None) -> Relation:
        """Mark that a relation's payload should be transferred from some other
        engine to this one.

        Parameters
        ----------
        target : Relation
            Relation to transfer.  If ``target.engine == self``, this relation
            will be returned directly and no transfer will be performed.
            Back-to-back transfers from one engine to another and back again
            are also simplified away (via a call to `Transfer.simplify`).
            Sequences of transfers involving more than two engines are not
            simplified.
        payload, optional
            Destination-engine-specific content for the relation to attach to
            the transfer.  Most `Transfer` relations do not have a payload;
            their ability to do so is mostly to support the special relation
            trees returned by the `Processor` class.

        Returns
        -------
        relation : `Relation`
            New relation that marks its upstream tree to be transferred to a
            new engine.

        Notes
        -----
        The default implementation calls `conform` on the target relation using
        the target relation's engine (i.e. not ``self``).  All override
        implementations should do this as well.

        See Also
        --------
        Processor.transfer
        """
        from ._transfer import Transfer

        if simplified := Transfer.simplify(target, self):
            target = simplified
        if target.engine == self:
            if payload is not None:
                raise EngineError("Cannot attach payload to transfer that will be simplified away.")
            return target
        conformed_target = target.engine.conform(target)
        return Transfer(conformed_target, destination=self, payload=payload)

    def make_doomed_relation(
        self, columns: Set[ColumnTag], messages: Sequence[str], name: str = "0"
    ) -> Relation:
        """Construct a leaf relation with no rows and one or more messages
        explaining why.

        Parameters
        ----------
        columns : `~collections.abc.Set` [ `ColumnTag` ]
            The columns in this relation.
        messages : `~collections.abc.Sequence` [ `str` ]
            One or more messages explaining why the relation has no rows.
        name : `str`, optional
            Name used to identify and reconstruct this relation.

        Returns
        -------
        relation : `Relation`
            Doomed relation.

        Notes
        -----
        This is simplify a convenience method that delegates to
        `LeafRelation.make_doomed`.  Derived engines with a nontrivial
        `conform` should override this method to conform the return value.
        """
        from ._leaf_relation import LeafRelation

        return LeafRelation.make_doomed(self, columns, messages, name)

    def make_join_identity_relation(self, name: str = "I") -> Relation:
        """Construct a leaf relation with no columns and exactly one row.

        Parameters
        ----------
        engine : `Engine`
            The engine that is responsible for interpreting this relation.
        name : `str`, optional
            Name used to identify and reconstruct this relation.

        Returns
        -------
        relation : `Relation`
            Relation with no columns and one row.
        """
        from ._leaf_relation import LeafRelation

        return LeafRelation.make_join_identity(self, name)

    def append_unary(self, operation: UnaryOperation, target: Relation) -> Relation:
        """Hook for maintaining the engine's `conform` invariants through
        `UnaryOperation.apply`.

        This method should only be called by `UnaryOperation.apply` and the
        engine's own methods and helper classes.  External code should call
        `UnaryOperation.apply` or a `Relation` factory method instead.

        Parameters
        ----------
        operation : `UnaryOperation`
            Operation to apply; should already be filtered through
            `UnaryOperation._begin_apply`.
        target : `Relation`
            Relation to apply the operation to directly.

        Returns
        -------
        relation : `Relation`
            Relation that includes the given operation acting on ``target``,
            or a simplified equivalent.

        Notes
        -----
        Implementations should delegate back to `UnaryOperation._finish_apply`
        to actually create a `UnaryOperationRelation` and perform final
        simplification and checks.  This is all the default implementation
        does.
        """  # noqa: D401
        return operation._finish_apply(target)

    def append_binary(self, operation: BinaryOperation, lhs: Relation, rhs: Relation) -> Relation:
        """Hook for maintaining the engine's `conform` invariants through
        `BinaryOperation.apply`.

        This method should only be called by `BinaryOperation.apply` and the
        engine's own methods and helper classes.  External code should call
        `BinaryOperation.apply` or a `Relation` factory method instead.

        Parameters
        ----------
        operation : `BinaryOperation`
            Operation to apply; should already be filtered through
            `BinaryOperation._begin_apply`.
        lhs : `Relation`
            One relation to apply the operation to directly.
        rhs : `Relation`
            The other relation to apply the operation to directly.

        Returns
        -------
        relation : `Relation`
            Relation that includes the given operation acting on ``lhs`` and
            ``rhs``, or a simplified equivalent.

        Notes
        -----
        Implementations should delegate back to `UnaryOperation._finish_apply`
        to actually create a `UnaryOperationRelation` and perform final
        simplification and checks.  This is all the default implementation
        does.
        """  # noqa: D401
        return operation._finish_apply(lhs, rhs)

    def backtrack_unary(
        self, operation: UnaryOperation, tree: Relation, preferred: Engine
    ) -> tuple[Relation, bool, tuple[str, ...]]:
        """Attempt to insert a unary operation in another engine upstream of
        this one by via operation commutators.

        Parameters
        ----------
        operation : `UnaryOperation`
            Unary operation to apply.
        tree : `Relation`
            Relation tree the operation logically acts on; any upstream
            insertion of the given operation should be equivalent to applying
            it to the root of this tree.  Caller guarantees that ``tree.engine
            == self``.
        preferred : `Engine`
            Engine in which the operation or its commuted equivalent should be
            performed.

        Returns
        -------
        new_tree : `Relation`
            Possibly-updated relation tree.
        done : `bool`
            If `True`, the operation has been fully inserted upstream in the
            preferred engine.  If `False`, either ``tree`` was returned
            unmodified or only a part of the operation (e.g. a projection whose
            columns are superset of the given projection's) was inserted
            upstream.
        messages : `~collections.abc.Sequence` [ `str` ]
            Messages explaining why backtracking insertion was unsuccessful or
            incomplete.  Should be sentences with no trailing ``.`` and no
            capitalization; they will be joined with semicolons.
        """
        return tree, False, (f"engine {self} does not support backtracking insertion",)


@dataclasses.dataclass(repr=False, eq=False, kw_only=True)
class GenericConcreteEngine(Engine, Generic[_F]):
    """An implementation-focused base class for `Engine` objects.

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
