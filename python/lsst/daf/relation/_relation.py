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

__all__ = (
    "BaseRelation",
    "Relation",
)

import dataclasses
from abc import abstractmethod
from collections.abc import Sequence, Set
from typing import TYPE_CHECKING, Any, Protocol, TypeVar

from ._columns import ColumnTag

if TYPE_CHECKING:
    from ._columns import ColumnExpression, Predicate
    from ._engine import Engine
    from ._operations import SortTerm


class Relation(Protocol):
    """An abstract interface for expression trees on tabular data.

    Notes
    -----
    This ABC is a `typing.Protocol`, which means that classes that implement
    its interface can be recognized as such by static type checkers without
    actually inheriting from it, and in fact all concrete relation types
    inherit only from `BaseRelation` (which provides implementations of many
    `Relation` methods, but does not include the complete interface or inherit
    from `Relation` itself) instead.  This split allows subclasses to implement
    attributes that are defined as properties here as `~dataclasses.dataclass`
    attributes instead of true properties, something `typing.Protocol`
    explicitly permits and recommends that nevertheless works only if the
    protocol is not actually inherited from.

    In almost all cases, users should use `Relation` instead of `BaseRelation`:
    the only exception is when writing an `isinstance` check to see if a type
    is a relation at all, rather than a particular relation subclass.
    `BaseRelation` may become an alias to `Relation` itself in the future if
    `typing.Protocol` inheritance interaction with properties is improved.

    All concrete `Relation` types are frozen, equality-comparable
    `dataclasses`.  They also provide a very concise `str` representation (in
    addition to the dataclass-provided `repr`) suitable for summarizing an
    entire relation tree.

    See Also
    --------
    :ref:`lsst.daf.relation-overview`
    """

    @property
    @abstractmethod
    def columns(self) -> Set[ColumnTag]:
        """The columns in this relation (`~collections.abc.Set` [ `ColumnTag` ]
        ).
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def payload(self) -> Any:
        """The engine-specific contents of the relation.

        This is `None` in the common case that engine-specific contents are to
        be computed on-the-fly.  Relation payloads permit "deferred
        initialization" - while relation objects are otherwise immutable, the
        payload may be set (once) after construction, via `attach_payload`.
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def engine(self) -> Engine:
        """The engine that is responsible for interpreting this relation
        (`Engine`).
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def min_rows(self) -> int:
        """The minimum number of rows this relation might have (`int`)."""
        raise NotImplementedError()

    @property
    @abstractmethod
    def max_rows(self) -> int | None:
        """The maximum number of rows this relation might have (`int` or
        `None`).

        This is `None` for relations whose size is not bounded from above.
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def is_locked(self) -> bool:
        """Whether this relation and those upstream of it should be considered
        fixed by tree-manipulation algorithms (`bool`).
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def is_join_identity(self) -> bool:
        """Whether a `join` to this relation will result in the other relation
        being returned directly (`bool`).

        Join identity relations have exactly one row and no columns.

        See Also
        --------
        LeafRelation.make_join_identity
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def is_trivial(self) -> bool:
        """Whether this relation has no real content (`bool`).

        A trivial relation is either a `join identity <is_join_identity>` with
        no columns and exactly one row, or a relation with an arbitrary number
        of columns and no rows (i.e. ``min_rows==max_rows==0``).
        """
        raise NotImplementedError()

    @abstractmethod
    def attach_payload(self, payload: Any) -> None:
        """Attach an engine-specific ``payload`` to this relation.

        This method may be called exactly once on a `Relation` instance that
        was not initialized with a ``payload``, despite the fact that
        `Relation` objects are otherwise considered immutable.

        Parameters
        ----------
        payload
            Engine-specific content to attach.

        Raises
        ------
        TypeError
            Raised if this relation already has a payload, or can never have a
            payload.  `TypeError` is used here for consistency with other
            attempts to assign to an attribute of an immutable object.
        """
        raise NotImplementedError()

    @abstractmethod
    def with_calculated_column(
        self,
        tag: ColumnTag,
        expression: ColumnExpression,
        *,
        preferred_engine: Engine | None = None,
        backtrack: bool = True,
        transfer: bool = False,
        require_preferred_engine: bool = False,
    ) -> Relation:
        """Return a new relation that adds a calculated column to this one.

        This is a convenience method chat constructs and applies a
        `Calculation` operation.

        Parameters
        ----------
        tag : `ColumnTag`
            Identifier for the new column.
        expression : `ColumnExpression`
            Expression used to populate the new column.
        preferred_engine : `Engine`, optional
            Engine that the operation would ideally be performed in.  If this
            is not equal to ``self.engine``, the ``backtrack``, ``transfer``,
            and ``require_preferred_engine`` arguments control the behavior.
        backtrack : `bool`, optional
            If `True` (default) and the current engine is not the preferred
            engine, attempt to insert this calculation before a transfer
            upstream of the current relation, as long as this can be done
            without breaking up any locked relations or changing the resulting
            relation content.
        transfer : `bool`, optional
            If `True` (`False` is default) and the current engine is not the
            preferred engine, insert a new `Transfer` before the `Calculation`.
            If ``backtrack`` is also true, the transfer is added only if the
            backtrack attempt fails.
        require_preferred_engine : `bool`, optional
            If `True` (`False` is default) and the current engine is not the
            preferred engine, raise `EngineError`.  If ``backtrack`` is also
            true, the exception is only raised if the backtrack attempt fails.
            Ignored if ``transfer`` is true.

        Returns
        -------
        relation : `Relation`
            Relation that contains the calculated column.

        Raises
        ------
        ColumnError
            Raised if the expression requires columns that are not present in
            ``self.columns``, or if ``tag`` is already present in
            ``self.columns``.
        EngineError
            Raised if ``require_preferred_engine=True`` and it was impossible
            to insert this operation in the preferred engine, or if the
            expression was not supported by the engine.
        """
        raise NotImplementedError()

    @abstractmethod
    def chain(self, rhs: Relation) -> Relation:
        """Return a new relation with all rows from this relation and another.

        This is a convenience method that constructs and applies a `Chain`
        operation.

        Parameters
        ----------
        rhs : `Relation`
            Other relation to chain to ``self``.  Must have the same columns
            and engine as ``self``.

        Returns
        -------
        relation : `Relation`
            New relation with all rows from both relations. This method never
            returns an operand directly, even if the other has ``max_rows==0``,
            as it is assumed that even relations with no rows are useful to
            preserve in the tree for `diagnostics <Diagnostics>`.

        Raises
        ------
        ColumnError
            Raised if the two relations do not have the same columns.
        EngineError
            Raised if the two relations do not have the same engine.
        """
        raise NotImplementedError()

    @abstractmethod
    def without_duplicates(
        self,
        *,
        preferred_engine: Engine | None = None,
        backtrack: bool = True,
        transfer: bool = False,
        require_preferred_engine: bool = False,
    ) -> Relation:
        """Return a new relation that removes any duplicate rows from this one.

        This is a convenience method that constructs and applies a
        `Deduplication` operation.

        Parameters
        ----------
        preferred_engine : `Engine`, optional
            Engine that the operation would ideally be performed in.  If this
            is not equal to ``self.engine``, the ``backtrack``, ``transfer``,
            and ``require_preferred_engine`` arguments control the behavior.
        backtrack : `bool`, optional
            If `True` (default) and the current engine is not the preferred
            engine, attempt to insert this deduplication before a transfer
            upstream of the current relation, as long as this can be done
            without breaking up any locked relations or changing the resulting
            relation content.
        transfer : `bool`, optional
            If `True` (`False` is default) and the current engine is not the
            preferred engine, insert a new `Transfer` before the
            `Deduplication`.  If ``backtrack`` is also true, the transfer is
            added only if the backtrack attempt fails.
        require_preferred_engine : `bool`, optional
            If `True` (`False` is default) and the current engine is not the
            preferred engine, raise `EngineError`.  If ``backtrack`` is also
            true, the exception is only raised if the backtrack attempt fails.
            Ignored if ``transfer`` is true.

        Returns
        -------
        relation : `Relation`
            Relation with no duplicate rows.  This may be ``self`` if it can be
            determined that there is no duplication already, but this is not
            guaranteed.

        Raises
        ------
        EngineError
            Raised if ``require_preferred_engine=True`` and it was impossible
            to insert this operation in the preferred engine.
        """
        raise NotImplementedError()

    @abstractmethod
    def join(
        self,
        rhs: Relation,
        predicate: Predicate | None = None,
        *,
        backtrack: bool = True,
        transfer: bool = False,
    ) -> Relation:
        """Return a new relation that joins this one to the given one.

        This is a convenience method that constructs and applies a `Join`
        operation, via `PartialJoin.apply`.

        Parameters
        ----------
        rhs : `Relation`
            Relation to join to ``self``.
        predicate : `Predicate`, optional
            Boolean expression that must evaluate to true in order to join a a
            pair of rows, in addition to an implicit equality constraint on any
            columns in both relations.
        backtrack : `bool`, optional
            If `True` (default) and ``self.engine != rhs.engine``, attempt to
            insert this join before a transfer upstream of ``self``, as long as
            this can be done without breaking up any locked relations or
            changing the resulting relation content.
        transfer : `bool`, optional
            If `True` (`False` is default) and ``self.engine != rhs.engine``,
            insert a new `Transfer` before the `Join`.  If ``backtrack`` is
            also true, the transfer is added only if the backtrack attempt
            fails.

        Returns
        -------
        relation : `Relation`
            New relation that joins ``self`` to ``rhs``.  May be ``self`` or
            ``rhs`` if the other is a `join identity <is_join_identity>`.

        Raises
        ------
        ColumnError
            Raised if the given predicate requires columns not present in
            ``self`` or ``rhs``.
        EngineError
            Raised if it was impossible to insert this operation in
            ``rhs.engine`` via backtracks or transfers on ``self``, or if the
            predicate was not supported by the engine.

        Notes
        -----
        This method does not treat ``self`` and ``rhs`` symmetrically: it
        always considers ``rhs`` fixed, and only backtracks into or considers
        applying transfers to ``self``.
        """
        raise NotImplementedError()

    @abstractmethod
    def materialized(
        self,
        name: str | None = None,
        *,
        name_prefix: str = "materialization",
    ) -> Relation:
        """Return a new relation that indicates that this relation's
        payload should be cached after it is first processed.

        This is a convenience method that constructs and applies a
        `Materialization` operation.

        Parameters
        ----------
        name : `str`, optional
            Name to use for the cached payload within the engine (e.g. the name
            for a temporary table in SQL).  If not provided, a name will be
            created via a call to `Engine.get_relation_name`.
        name_prefix : `str`, optional
            Prefix to pass to `Engine.get_relation_name`; ignored if ``name``
            is provided.  Unlike
            most operations, `Materialization` relations are locked by default,
            since they reflect user intent to mark a specific tree as
            cacheable.

        Returns
        -------
        relation : `Relation`
            New relation that marks its upstream tree for caching.  May be
            ``self`` if it is already a `LeafRelation` or another
            materialization (in which case the given name or name prefix will
            be ignored).

        See Also
        --------
        Processor.materialize
        """
        raise NotImplementedError()

    @abstractmethod
    def with_only_columns(
        self,
        columns: Set[ColumnTag],
        *,
        preferred_engine: Engine | None = None,
        backtrack: bool = True,
        transfer: bool = False,
        require_preferred_engine: bool = False,
    ) -> Relation:
        """Return a new relation whose columns are a subset of this relation's.

        This is a convenience method that constructs and applies a `Projection`
        operation.

        Parameters
        ----------
        columns : `~collections.abc.Set` [ `ColumnTag` ]
            Columns to be propagated to the new relation; must be a subset of
            ``self.columns``.
        preferred_engine : `Engine`, optional
            Engine that the operation would ideally be performed in.  If this
            is not equal to ``self.engine``, the ``backtrack``, ``transfer``,
            and ``require_preferred_engine`` arguments control the behavior.
        backtrack : `bool`, optional
            If `True` (default) and the current engine is not the preferred
            engine, attempt to insert this projection before a transfer
            upstream of the current relation, as long as this can be done
            without breaking up any locked relations or changing the resulting
            relation content.
        transfer : `bool`, optional
            If `True` (`False` is default) and the current engine is not the
            preferred engine, insert a new `Transfer` before the
            `Projection`.  If ``backtrack`` is also true, the transfer is
            added only if the backtrack attempt fails.
        require_preferred_engine : `bool`, optional
            If `True` (`False` is default) and the current engine is not the
            preferred engine, raise `EngineError`.  If ``backtrack`` is also
            true, the exception is only raised if the backtrack attempt fails.
            Ignored if ``transfer`` is true.

        Returns
        -------
        relation : `Relation`
            New relation with only the given columns.  Will be ``self`` if
            ``columns == self.columns``.

        Raises
        ------
        ColumnError
            Raised if ``columns`` is not a subset of ``self.columns``.
        EngineError
            Raised if ``require_preferred_engine=True`` and it was impossible
            to insert this operation in the preferred engine.
        """
        raise NotImplementedError()

    @abstractmethod
    def with_rows_satisfying(
        self,
        predicate: Predicate,
        *,
        preferred_engine: Engine | None = None,
        backtrack: bool = True,
        transfer: bool = False,
        require_preferred_engine: bool = False,
    ) -> Relation:
        """Return a new relation that filters out rows via a boolean
        expression.

        This is a convenience method that constructions and applies a
        `Selection` operation.

        Parameters
        ----------
        predicate : `Predicate`
            Boolean expression that evaluates to `False` for rows that should
            be included and `False` for rows that should be filtered out.
        preferred_engine : `Engine`, optional
            Engine that the operation would ideally be performed in.  If this
            is not equal to ``self.engine``, the ``backtrack``, ``transfer``,
            and ``require_preferred_engine`` arguments control the behavior.
        backtrack : `bool`, optional
            If `True` (default) and the current engine is not the preferred
            engine, attempt to insert this selection before a transfer
            upstream of the current relation, as long as this can be done
            without breaking up any locked relations or changing the resulting
            relation content.
        transfer : `bool`, optional
            If `True` (`False` is default) and the current engine is not the
            preferred engine, insert a new `Transfer` before the
            `Selection`.  If ``backtrack`` is also true, the transfer is
            added only if the backtrack attempt fails.
        require_preferred_engine : `bool`, optional
            If `True` (`False` is default) and the current engine is not the
            preferred engine, raise `EngineError`.  If ``backtrack`` is also
            true, the exception is only raised if the backtrack attempt fails.
            Ignored if ``transfer`` is true.

        Returns
        -------
        relation : `Relation`
            New relation with only the rows that satisfy the given predicate.
            May be ``self`` if the predicate is
            `trivially True <Predicate.as_trivial>`.

        Raises
        ------
        ColumnError
            Raised if ``predicate.columns_required`` is not a subset of
            ``self.columns``.
        EngineError
            Raised if ``require_preferred_engine=True`` and it was impossible
            to insert this operation in the preferred engine, or if the
            expression was not supported by the engine.
        """
        raise NotImplementedError()

    @abstractmethod
    def __getitem__(self, key: slice) -> Relation:
        """Return a new relation whose rows are a slice of ``self``.

        This is a convenience method that constructs and applies a `Slice`
        operation.

        Parameters
        ----------
        key : `slice`
            Start and stop for the slice.  Non-unit step values are not
            supported.

        Returns
        -------
        relation : `Relation`
            New relation with only the rows between the given start and stop
            indices.  May be ``self`` if ``start=0`` and ``stop=None``.  If
            ``self`` is already a slice operation relation, the operations will
            be merged.

        Raises
        ------
        TypeError
            Raised if ``slice.step`` is a value other than ``1`` or ``None``.
        """
        raise NotImplementedError()

    @abstractmethod
    def sorted(
        self,
        terms: Sequence[SortTerm],
        *,
        preferred_engine: Engine | None = None,
        backtrack: bool = True,
        transfer: bool = False,
        require_preferred_engine: bool = False,
    ) -> Relation:
        """Return a new relation that sorts rows according to a sequence of
        column expressions.

        This is a convenience method that constructs and applies a `Sort`
        operation.

        Parameters
        ----------
        terms : `~collections.abc.Sequence` [ `SortTerm` ]
            Ordered sequence of column expressions to sort on, with whether to
            apply them in ascending or descending order.
        preferred_engine : `Engine`, optional
            Engine that the operation would ideally be performed in.  If this
            is not equal to ``self.engine``, the ``backtrack``, ``transfer``,
            and ``require_preferred_engine`` arguments control the behavior.
        backtrack : `bool`, optional
            If `True` (default) and the current engine is not the preferred
            engine, attempt to insert this sort before a transfer upstream of
            the current relation, as long as this can be done without breaking
            up any locked relations or changing the resulting relation content.
        transfer : `bool`, optional
            If `True` (`False` is default) and the current engine is not the
            preferred engine, insert a new `Transfer` before the `Sort`.  If
            ``backtrack`` is also true, the transfer is added only if the
            backtrack attempt fails.
        require_preferred_engine : `bool`, optional
            If `True` (`False` is default) and the current engine is not the
            preferred engine, raise `EngineError`.  If ``backtrack`` is also
            true, the exception is only raised if the backtrack attempt fails.
            Ignored if ``transfer`` is true.

        Returns
        -------
        relation : `Relation`
            New relation with sorted rows.  Will be ``self`` if ``terms`` is
            empty.    If ``self`` is already a sort operation relation, the
            operations will be merged by concatenating their terms, which may
            result in duplicate sort terms that have no effect.

        Raises
        ------
        ColumnError
            Raised if any column required by a `SortTerm` is not present in
            ``self.columns``.
        EngineError
            Raised if ``require_preferred_engine=True`` and it was impossible
            to insert this operation in the preferred engine, or if a
            `SortTerm` expression was not supported by the engine.
        """
        raise NotImplementedError()

    @abstractmethod
    def transferred_to(self, destination: Engine) -> Relation:
        """Return a new relation that transfers this relation to a new engine.

        This is a convenience method that constructs and applies a `Transfer`
        operation.

        Parameters
        ----------
        destination : `Engine`
            Engine for the new relation.

        Returns
        -------
        relation : `Relation`
            New relation in the given engine.  Will be ``self`` if
            ``self.engine == destination``.
        """
        raise NotImplementedError()


_M = TypeVar("_M", bound=Any)


def _copy_relation_docs(method: _M) -> _M:
    """Decorator that copies a docstring from the `Relation` class for the
    method of the same name.

    We want to document `Relation` since that's the public interface, but we
    also want those docs to appear in the concrete derived classes, and that
    means we need to put them on the `BaseRelation` class so they can be
    inherited.
    """  # noqa: D401
    method.__doc__ = getattr(Relation, method.__name__).__doc__
    return method


@dataclasses.dataclass(frozen=True)
class BaseRelation:
    """An implementation-focused target class for concrete `Relation` objects.

    This class provides method implementations for much of the `Relation`
    interface and is actually inherited from (unlike `Relation` itself) by all
    concrete relations.  It should only be used outside of the
    ``lsst.daf.relation`` package when needed for `isinstance` checks.
    """

    def __init_subclass__(cls) -> None:
        assert cls.__name__ in {
            "LeafRelation",
            "UnaryOperationRelation",
            "BinaryOperationRelation",
            "MarkerRelation",
        } or (
            cls.__base__ is not None and cls.__base__.__name__ != "Relation"
        ), "Relation inheritance is closed to predefined types in daf_relation and MarkerRelation subclasses."

    @property
    @_copy_relation_docs
    def is_join_identity(self: Relation) -> bool:
        return not self.columns and self.max_rows == 1 and self.min_rows == 1

    @property
    @_copy_relation_docs
    def is_trivial(self: Relation) -> bool:
        return self.is_join_identity or self.max_rows == 0

    @_copy_relation_docs
    def attach_payload(self: Relation, payload: Any) -> None:
        raise TypeError(f"Cannot attach payload {payload} to relation {self}.")

    @_copy_relation_docs
    def with_calculated_column(
        self: Relation,
        tag: ColumnTag,
        expression: ColumnExpression,
        *,
        preferred_engine: Engine | None = None,
        backtrack: bool = True,
        transfer: bool = False,
        require_preferred_engine: bool = False,
    ) -> Relation:
        from ._operations import Calculation

        return Calculation(tag, expression).apply(
            self,
            preferred_engine=preferred_engine,
            backtrack=backtrack,
            transfer=transfer,
            require_preferred_engine=require_preferred_engine,
        )

    @_copy_relation_docs
    def chain(self: Relation, rhs: Relation) -> Relation:
        from ._operations import Chain

        return Chain().apply(self, rhs)

    @_copy_relation_docs
    def without_duplicates(
        self: Relation,
        *,
        preferred_engine: Engine | None = None,
        backtrack: bool = True,
        transfer: bool = False,
        require_preferred_engine: bool = False,
    ) -> Relation:
        from ._operations import Deduplication

        return Deduplication().apply(
            self,
            preferred_engine=preferred_engine,
            backtrack=backtrack,
            transfer=transfer,
            require_preferred_engine=require_preferred_engine,
        )

    @_copy_relation_docs
    def join(
        self: Relation,
        rhs: Relation,
        predicate: Predicate | None = None,
        *,
        backtrack: bool = True,
        transfer: bool = False,
    ) -> Relation:
        from ._columns import Predicate
        from ._operations import Join

        return (
            Join(predicate if predicate is not None else Predicate.literal(True))
            .partial(rhs)
            .apply(self, backtrack=backtrack, transfer=transfer)
        )

    @_copy_relation_docs
    def materialized(
        self: Relation,
        name: str | None = None,
        *,
        name_prefix: str = "materialization",
    ) -> Relation:
        return self.engine.materialize(self, name, name_prefix)

    @_copy_relation_docs
    def with_only_columns(
        self: Relation,
        columns: Set[ColumnTag],
        *,
        preferred_engine: Engine | None = None,
        backtrack: bool = True,
        transfer: bool = False,
        require_preferred_engine: bool = False,
    ) -> Relation:
        from ._operations import Projection

        return Projection(frozenset(columns)).apply(
            self,
            preferred_engine=preferred_engine,
            backtrack=backtrack,
            transfer=transfer,
            require_preferred_engine=require_preferred_engine,
        )

    @_copy_relation_docs
    def with_rows_satisfying(
        self: Relation,
        predicate: Predicate,
        *,
        preferred_engine: Engine | None = None,
        backtrack: bool = True,
        transfer: bool = False,
        require_preferred_engine: bool = False,
    ) -> Relation:
        from ._operations import Selection

        return Selection(predicate).apply(
            self,
            preferred_engine=preferred_engine,
            backtrack=backtrack,
            transfer=transfer,
            require_preferred_engine=require_preferred_engine,
        )

    @_copy_relation_docs
    def __getitem__(self: Relation, key: slice) -> Relation:
        from ._operations import Slice

        if not isinstance(key, slice):
            raise TypeError("Only slices are supported in relation indexing.")
        if key.step not in (1, None):
            raise TypeError("Slices with non-unit step are not supported.")
        return Slice(key.start if key.start is not None else 0, key.stop).apply(self)

    @_copy_relation_docs
    def sorted(
        self: Relation,
        terms: Sequence[SortTerm],
        *,
        preferred_engine: Engine | None = None,
        backtrack: bool = True,
        transfer: bool = False,
        require_preferred_engine: bool = False,
    ) -> Relation:
        from ._operations import Sort

        return Sort(tuple(terms)).apply(
            self,
            preferred_engine=preferred_engine,
            backtrack=backtrack,
            transfer=transfer,
            require_preferred_engine=require_preferred_engine,
        )

    @_copy_relation_docs
    def transferred_to(self: Relation, destination: Engine) -> Relation:
        return destination.transfer(self)
