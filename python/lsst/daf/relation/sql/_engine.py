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

__all__ = ("Engine",)

import dataclasses
from collections.abc import Callable, Iterable, Mapping, Sequence, Set
from typing import TYPE_CHECKING, Any, ClassVar, Generic, TypeVar, cast

import sqlalchemy

from .._binary_operation import BinaryOperation, IgnoreOne
from .._columns import (
    ColumnExpression,
    ColumnExpressionSequence,
    ColumnFunction,
    ColumnInContainer,
    ColumnLiteral,
    ColumnRangeLiteral,
    ColumnReference,
    ColumnTag,
    LogicalAnd,
    LogicalNot,
    LogicalOr,
    Predicate,
    PredicateFunction,
    PredicateLiteral,
    PredicateReference,
    flatten_logical_and,
)
from .._engine import GenericConcreteEngine
from .._exceptions import EngineError, RelationalAlgebraError
from .._leaf_relation import LeafRelation
from .._marker_relation import MarkerRelation
from .._materialization import Materialization
from .._operation_relations import BinaryOperationRelation, UnaryOperationRelation
from .._operations import (
    Calculation,
    Chain,
    Deduplication,
    Join,
    PartialJoin,
    Projection,
    Selection,
    Slice,
    Sort,
    SortTerm,
)
from .._transfer import Transfer
from .._unary_operation import Identity, UnaryOperation
from ._payload import Payload
from ._select import Select

if TYPE_CHECKING:
    from .._relation import Relation


_L = TypeVar("_L")


@dataclasses.dataclass(repr=False, eq=False, kw_only=True)
class Engine(
    GenericConcreteEngine[Callable[..., sqlalchemy.sql.ColumnElement[Any]]],
    Generic[_L],
):
    """A concrete engine class for relations backed by a SQL database.

    See the `.sql` module documentation for details.
    """

    name: str = "sql"

    EMPTY_COLUMNS_NAME: ClassVar[str] = "IGNORED"
    """Name of the column added to a SQL ``SELECT`` query in order to represent
    relations that have no real columns.
    """

    EMPTY_COLUMNS_TYPE: ClassVar[type] = sqlalchemy.Boolean
    """Type of the column added to a SQL ``SELECT`` query in order to represent
    relations that have no real columns.
    """

    def __str__(self) -> str:
        return self.name

    def __repr__(self) -> str:
        return f"lsst.daf.relation.sql.Engine({self.name!r})@{id(self):0x}"

    def make_leaf(
        self,
        columns: Set[ColumnTag],
        payload: Payload[_L],
        *,
        min_rows: int = 0,
        max_rows: int | None = None,
        name: str = "",
        messages: Sequence[str] = (),
        name_prefix: str = "leaf",
        parameters: Any = None,
    ) -> Relation:
        """Create a nontrivial leaf relation in this engine.

        This is a convenience method that simply forwards all arguments to
        the `.LeafRelation` constructor, and then wraps the result in a
        `Select`; see `LeafRelation` for details.
        """
        return Select.apply_skip(
            LeafRelation(
                self,
                frozenset(columns),
                payload,
                min_rows=min_rows,
                max_rows=max_rows,
                messages=messages,
                name=name,
                name_prefix=name_prefix,
                parameters=parameters,
            )
        )

    def conform(self, relation: Relation) -> Select:
        # Docstring inherited.
        match relation:
            case Select():
                return relation
            case UnaryOperationRelation(operation=operation, target=target):
                conformed_target = self.conform(target)
                return self._append_unary_to_select(operation, conformed_target)
            case BinaryOperationRelation(operation=operation, lhs=lhs, rhs=rhs):
                conformed_lhs = self.conform(lhs)
                conformed_rhs = self.conform(rhs)
                return self._append_binary_to_select(operation, conformed_lhs, conformed_rhs)
            case Transfer() | Materialization() | LeafRelation():
                # We always conform upstream of transfers and materializations,
                # so no need to recurse.
                return Select.apply_skip(relation)
            case MarkerRelation(target=target):
                # Other marker relations.
                conformed_target = self.conform(target)
                return Select.apply_skip(conformed_target)
        raise AssertionError("Match should be exhaustive and all branches return.")

    def materialize(
        self, target: Relation, name: str | None = None, name_prefix: str = "materialization_"
    ) -> Select:
        # Docstring inherited.
        conformed_target = self.conform(target)
        if conformed_target.has_sort and not conformed_target.has_slice:
            raise RelationalAlgebraError(
                f"Materializing relation {conformed_target} will not preserve row order."
            )
        return Select.apply_skip(super().materialize(conformed_target, name, name_prefix))

    def transfer(self, target: Relation, payload: Any | None = None) -> Select:
        # Docstring inherited.
        return Select.apply_skip(super().transfer(target, payload))

    def make_doomed_relation(
        self, columns: Set[ColumnTag], messages: Sequence[str], name: str = "0"
    ) -> Relation:
        # Docstring inherited.
        return Select.apply_skip(super().make_doomed_relation(columns, messages, name))

    def make_join_identity_relation(self, name: str = "I") -> Relation:
        # Docstring inherited.
        return Select.apply_skip(super().make_join_identity_relation(name))

    def get_join_identity_payload(self) -> Payload[_L]:
        # Docstring inherited.
        select_columns: list[sqlalchemy.sql.ColumnElement] = []
        self.handle_empty_columns(select_columns)
        return Payload[_L](sqlalchemy.sql.select(*select_columns).subquery())

    def get_doomed_payload(self, columns: Set[ColumnTag]) -> Payload[_L]:
        # Docstring inherited.
        select_columns: list[sqlalchemy.sql.ColumnElement] = [
            sqlalchemy.sql.literal(None).label(self.get_identifier(tag)) for tag in columns
        ]
        self.handle_empty_columns(select_columns)
        subquery = sqlalchemy.sql.select(*select_columns).subquery()
        return Payload(
            subquery,
            where=[sqlalchemy.sql.literal(False)],
            columns_available=self.extract_mapping(columns, subquery.columns),
        )

    def append_unary(self, operation: UnaryOperation, target: Relation) -> Select:
        # Docstring inherited.
        conformed_target = self.conform(target)
        return self._append_unary_to_select(operation, conformed_target)

    def _append_unary_to_select(self, operation: UnaryOperation, select: Select) -> Select:
        """Internal recursive implementation of `append_unary`.

        This method should not be called by external code, but it may be
        overridden and called recursively by derived engine classes.

        Parameters
        ----------
        operation : `UnaryOperation`
            Operation to add to the tree.
        select : `Select`
            Existing already-conformed relation tree.

        Returns
        -------
        appended : `Select`
            Conformed relation tree that includes the given operation.
        """  # noqa: D401
        match operation:
            case Calculation(tag=tag):
                if select.is_compound:
                    # This Select wraps a Chain operation in order to represent
                    # a SQL UNION or UNION ALL, and we trust the user's intent
                    # in putting those upstream of this operation, so we also
                    # add a nested subquery here.
                    return Select.apply_skip(operation._finish_apply(select))
                elif select.has_projection:
                    return select.reapply_skip(
                        after=operation,
                        projection=Projection(frozenset(select.columns | {tag})),
                    )
                else:
                    return select.reapply_skip(after=operation)
            case Deduplication():
                if not select.has_deduplication:
                    if select.has_slice:
                        # There was a Slice upstream, which needs to be applied
                        # before this Deduplication, so we nest the existing
                        # subquery within a new one that has deduplication.
                        return Select.apply_skip(select, deduplication=operation)
                    else:
                        # Move the Deduplication into the Select's
                        # operations.
                        return select.reapply_skip(deduplication=operation)
                else:
                    # There was already another (redundant)
                    # Deduplication upstream.
                    return select
            case Projection():
                if select.has_deduplication:
                    # There was a Duplication upstream, so we need to ensure
                    # that is applied before this Projection via a nested
                    # subquery.  Instead of applying the existing subquery
                    # operations, though, we apply one without any sort or
                    # slice that might exist, and save those for the new outer
                    # query, since putting those in a subquery would destroy
                    # the ordering.
                    subquery = select.reapply_skip(sort=None, slice=None)
                    return Select.apply_skip(
                        subquery,
                        projection=operation,
                        sort=select.sort,
                        slice=select.slice,
                    )
                else:
                    # No Deduplication, so we can just add the Projection to
                    # the existing Select and reapply it.
                    match select.skip_to:
                        case BinaryOperationRelation(operation=Chain() as chain, lhs=lhs, rhs=rhs):
                            # ... unless the skip_to relation is a Chain; we
                            # want to move the Projection inside the Chain, to
                            # make it easier to avoid subqueries in UNION [ALL]
                            # constructs later.
                            return select.reapply_skip(
                                skip_to=chain._finish_apply(operation.apply(lhs), operation.apply(rhs)),
                                projection=None,
                            )
                        case _:
                            return select.reapply_skip(projection=operation)
            case Selection():
                if select.has_slice:
                    # There was a Slice upstream, which needs to be applied
                    # before this Selection via a nested subquery (which means
                    # we just apply the Selection to target, then add a new
                    # empty subquery marker).
                    return Select.apply_skip(operation._finish_apply(select))
                elif select.is_compound:
                    # This Select wraps a Chain operation in order to represent
                    # a SQL UNION or UNION ALL, and we trust the user's intent
                    # in putting those upstream of this operation, so we also
                    # add a nested subquery here.
                    return Select.apply_skip(operation._finish_apply(select))
                else:
                    return select.reapply_skip(after=operation)
            case Slice():
                return select.reapply_skip(slice=select.slice.then(operation))
            case Sort():
                if select.has_slice:
                    # There was a Slice upstream, which needs to be applied
                    # before this Sort via a nested subquery (which means we
                    # apply a new Sort-only Select to 'select' itself).
                    return Select.apply_skip(select, sort=operation)
                else:
                    return select.reapply_skip(sort=select.sort.then(operation))
            case PartialJoin(binary=binary, fixed=fixed, fixed_is_lhs=fixed_is_lhs):
                if fixed_is_lhs:
                    return self.append_binary(binary, fixed, select)
                else:
                    return self.append_binary(binary, select, fixed)
            case Identity():
                return select
        raise NotImplementedError(f"Unsupported operation type {operation} for engine {self}.")

    def append_binary(self, operation: BinaryOperation, lhs: Relation, rhs: Relation) -> Select:
        # Docstring inherited.
        conformed_lhs = self.conform(lhs)
        conformed_rhs = self.conform(rhs)
        return self._append_binary_to_select(operation, conformed_lhs, conformed_rhs)

    def _append_binary_to_select(self, operation: BinaryOperation, lhs: Select, rhs: Select) -> Select:
        """Internal recursive implementation of `append_binary`.

        This method should not be called by external code, but it may be
        overridden and called recursively by derived engine classes.

        Parameters
        ----------
        operation : `UnaryOperation`
            Operation to add to the tree.
        lhs : `Select`
            Existing already-conformed relation tree.
        rhs : `Select`
            The other existing already-conformed relation tree.

        Returns
        -------
        appended : `Select`
            Conformed relation tree that includes the given operation.
        """  # noqa: D401
        if lhs.has_sort and not lhs.has_slice:
            raise RelationalAlgebraError(
                f"Applying binary operation {operation} to relation {lhs} will not preserve row order."
            )
        if rhs.has_sort and not rhs.has_slice:
            raise RelationalAlgebraError(
                f"Applying binary operation {operation} to relation {rhs} will not preserve row order."
            )
        match operation:
            case Chain():
                # Chain operands should always be Selects, but they can't have
                # Sorts or Slices, at least not directly; if those exist we
                # need to move them into subqueries at which point we might as
                # well move any Projection or Dedulication there as well).  But
                # because of the check on Sorts-without-Slices above, there has
                # to be a Slice here for there to be a Sort here.
                if lhs.has_slice:
                    lhs = Select.apply_skip(lhs)
                if rhs.has_slice:
                    rhs = Select.apply_skip(rhs)
                return Select.apply_skip(operation._finish_apply(lhs, rhs))
            case Join():
                # For Joins, we move Projections after the Join, unless we have
                # another reason to make a subquery before the join, and the
                # operands are only Selects if they need to be subqueries.
                new_lhs, new_lhs_needs_projection = lhs.strip()
                new_rhs, new_rhs_needs_projection = rhs.strip()
                if new_lhs_needs_projection or new_rhs_needs_projection:
                    projection = Projection(frozenset(lhs.columns | rhs.columns))
                else:
                    projection = None
                return Select.apply_skip(operation._finish_apply(new_lhs, new_rhs), projection=projection)
            case IgnoreOne(ignore_lhs=ignore_lhs):
                if ignore_lhs:
                    return rhs
                else:
                    return lhs
        raise AssertionError(f"Match on {operation} should be exhaustive and all branches return..")

    def get_identifier(self, tag: ColumnTag) -> str:
        """Return the SQL identifier that should be used to represent the given
        column.

        Parameters
        ----------
        tag : `.ColumnTag`
            Object representing a column.

        Returns
        -------
        identifier : `str`
            SQL identifier for this column.

        Notes
        -----
        This method may be overridden to replace special characters not
        supported by a particular DBMS (even after quoting, which SQLAlchemy
        handles transparently), deal with case transformation, or ensure
        identifiers are not truncated (e.g. by PostgreSQL's 64-char limit).
        The default implementation returns ``tag.qualified_name`` unchanged.
        """
        return tag.qualified_name

    def extract_mapping(
        self, tags: Iterable[ColumnTag], sql_columns: sqlalchemy.sql.ColumnCollection
    ) -> dict[ColumnTag, _L]:
        """Extract a mapping with `.ColumnTag` keys and logical column values
        from a SQLAlchemy column collection.

        Parameters
        ----------
        tags : `Iterable`
            Set of `.ColumnTag` objects whose logical columns should be
            extracted.
        sql_columns : `sqlalchemy.sql.ColumnCollection`
            SQLAlchemy collection of columns, such as
            `sqlalchemy.sql.FromClause.columns`.

        Returns
        -------
        logical_columns : `dict`
            Dictionary mapping `.ColumnTag` to logical column type.

        Notes
        -----
        This method must be overridden to support a custom logical columns.
        """
        return {tag: cast(_L, sql_columns[self.get_identifier(tag)]) for tag in tags}

    def select_items(
        self,
        items: Iterable[tuple[ColumnTag, _L]],
        sql_from: sqlalchemy.sql.FromClause,
        *extra: sqlalchemy.sql.ColumnElement,
    ) -> sqlalchemy.sql.Select:
        """Construct a SQLAlchemy representation of a SELECT query.

        Parameters
        ----------
        items : `Iterable` [ `tuple` ]
            Iterable of (`.ColumnTag`, logical column) pairs.  This is
            typically the ``items()`` of a mapping returned by
            `extract_mapping` or obtained from `Payload.columns_available`.
        sql_from : `sqlalchemy.sql.FromClause`
            SQLAlchemy representation of a FROM clause, such as a single table,
            aliased subquery, or join expression.  Must provide all columns
            referenced by ``items``.
        *extra : `sqlalchemy.sql.ColumnElement`
            Additional SQL column expressions to include.

        Returns
        -------
        select : `sqlalchemy.sql.Select`
            SELECT query.

        Notes
        -----
        This method is responsible for handling the case where ``items`` is
        empty, typically by delegating to `handle_empty_columns`.

        This method must be overridden to support a custom logical columns.
        """
        select_columns: list[sqlalchemy.sql.ColumnElement] = [
            cast(sqlalchemy.sql.ColumnElement, logical_column).label(self.get_identifier(tag))
            for tag, logical_column in items
        ]
        select_columns.extend(extra)
        self.handle_empty_columns(select_columns)
        return sqlalchemy.sql.select(*select_columns).select_from(sql_from)

    def handle_empty_columns(self, columns: list[sqlalchemy.sql.ColumnElement]) -> None:
        """Handle the edge case where a SELECT statement has no columns, by
        adding a literal column that should be ignored.

        Parameters
        ----------
        columns : `list` [ `sqlalchemy.sql.ColumnElement` ]
            List of SQLAlchemy column objects.  This may have no elements when
            this method is called, and must always have at least one element
            when it returns.
        """
        if not columns:
            columns.append(sqlalchemy.sql.literal(True).label(self.EMPTY_COLUMNS_NAME))

    def to_executable(
        self, relation: Relation, extra_columns: Iterable[sqlalchemy.sql.ColumnElement] = ()
    ) -> sqlalchemy.sql.expression.SelectBase:
        """Convert a relation tree to an executable SQLAlchemy expression.

        Parameters
        ----------
        relation : `Relation`
            The relation tree to convert.
        extra_columns : `~collections.abc.Iterable`
            Iterable of additional SQLAlchemy column objects to include
            directly in the ``SELECT`` clause.

        Returns
        -------
        select : `sqlalchemy.sql.expression.SelectBase`
            A SQLAlchemy ``SELECT`` or compound ``SELECT`` query.

        Notes
        -----
        This method requires all relations in the tree to have the same engine
        (``self``).  It also cannot handle `.Materialization` operations
        unless they have already been processed once already (and hence have
        a payload attached).  Use the `.Processor` function to handle both of
        these cases.
        """
        if relation.engine != self:
            raise EngineError(
                f"Engine {self!r} cannot operate on relation {relation} with engine {relation.engine!r}. "
                "Use lsst.daf.relation.Processor to evaluate transfers first."
            )
        relation = self.conform(relation)
        return self._select_to_executable(relation, extra_columns)

    def _select_to_executable(
        self,
        select: Select,
        extra_columns: Iterable[sqlalchemy.sql.ColumnElement],
    ) -> sqlalchemy.sql.expression.SelectBase:
        """Internal recursive implementation of `to_executable`.

        This method should not be called by external code, but it may be
        overridden and called recursively by derived engine classes.

        Parameters
        ----------
        columns : `~collections.abc.Set` [ `ColumnTag` ]
            Columns to include in the ``SELECT`` clause.
        select : `Select`
            Already-conformed relation tree to convert.
        extra_columns : `~collections.abc.Iterable`
            Iterable of additional SQLAlchemy column objects to include
            directly in the ``SELECT`` clause.

        Returns
        -------
        executable : `sqlalchemy.sql.expression.SelectBase`
            SQLAlchemy ``SELECT`` or compound ``SELECT`` object.

        Notes
        -----
        This method handles trees terminated with `Select`, the operation
        relation types managed by `Select`, and `Chain` operations nested
        directly as the `skip_to` target of a `Select`.  It delegates to
        `to_payload` for all other relation types.
        """  # noqa: D401
        columns_available: Mapping[ColumnTag, _L] | None = None
        executable: sqlalchemy.sql.Select | sqlalchemy.sql.CompoundSelect
        match select.skip_to:
            case BinaryOperationRelation(operation=Chain(), lhs=lhs, rhs=rhs):
                lhs_executable = self._select_to_executable(cast(Select, lhs), extra_columns)
                rhs_executable = self._select_to_executable(cast(Select, rhs), extra_columns)
                if select.has_deduplication:
                    executable = sqlalchemy.sql.union(lhs_executable, rhs_executable)
                else:
                    executable = sqlalchemy.sql.union_all(lhs_executable, rhs_executable)
            case _:
                if select.skip_to.payload is not None:
                    payload: Payload[_L] = select.skip_to.payload
                else:
                    payload = self.to_payload(select.skip_to)
                if not select.columns and not extra_columns:
                    extra_columns = list(extra_columns)
                    self.handle_empty_columns(extra_columns)
                columns_available = payload.columns_available
                columns_projected = {tag: columns_available[tag] for tag in select.columns}
                executable = self.select_items(columns_projected.items(), payload.from_clause, *extra_columns)
                if len(payload.where) == 1:
                    executable = executable.where(payload.where[0])
                elif payload.where:
                    executable = executable.where(sqlalchemy.sql.and_(*payload.where))
                if select.has_deduplication:
                    executable = executable.distinct()
        if select.has_sort:
            if columns_available is None:
                columns_available = self.extract_mapping(select.skip_to.columns, executable.selected_columns)
            executable = executable.order_by(
                *[self.convert_sort_term(term, columns_available) for term in select.sort.terms]
            )
        if select.slice.start:
            executable = executable.offset(select.slice.start)
        if select.slice.limit is not None:
            executable = executable.limit(select.slice.limit)
        return executable

    def to_payload(self, relation: Relation) -> Payload[_L]:
        """Internal recursive implementation of `to_executable`.

        This method should not be called by external code, but it may be
        overridden and called recursively by derived engine classes.

        Parameters
        ----------
        relation : `Relation`
            Relation to convert.  This method handles all operation relation
            types other than `Chain` and the operations managed by `Select`.

        Returns
        -------
        payload : `Payload`
            Struct containing a SQLAlchemy represenation of a simple ``SELECT``
            query.
        """  # noqa: D401
        assert relation.engine == self, "Should be guaranteed by callers."
        if relation.payload is not None:  # Should cover all LeafRelations
            return relation.payload
        match relation:
            case UnaryOperationRelation(operation=operation, target=target):
                match operation:
                    case Calculation(tag=tag, expression=expression):
                        result = self.to_payload(target).copy()
                        result.columns_available[tag] = self.convert_column_expression(
                            expression, result.columns_available
                        )
                        return result
                    case Selection(predicate=predicate):
                        result = self.to_payload(target).copy()
                        result.where.extend(
                            self.convert_flattened_predicate(predicate, result.columns_available)
                        )
                        return result
            case BinaryOperationRelation(
                operation=Join(predicate=predicate, common_columns=common_columns), lhs=lhs, rhs=rhs
            ):
                lhs_payload = self.to_payload(lhs)
                rhs_payload = self.to_payload(rhs)
                assert common_columns is not None, "Guaranteed by Join.apply and PartialJoin.apply."
                on_terms: list[sqlalchemy.sql.ColumnElement] = []
                if common_columns:
                    on_terms.extend(
                        cast(
                            sqlalchemy.sql.ColumnElement,
                            lhs_payload.columns_available[tag] == rhs_payload.columns_available[tag],
                        )
                        for tag in common_columns
                    )
                columns_available = {**lhs_payload.columns_available, **rhs_payload.columns_available}
                if predicate.as_trivial() is not True:
                    on_terms.extend(self.convert_flattened_predicate(predicate, columns_available))
                on_clause: sqlalchemy.sql.ColumnElement
                if not on_terms:
                    on_clause = sqlalchemy.sql.literal(True)
                elif len(on_terms) == 1:
                    on_clause = on_terms[0]
                else:
                    on_clause = sqlalchemy.sql.and_(*on_terms)
                return Payload(
                    from_clause=lhs_payload.from_clause.join(rhs_payload.from_clause, onclause=on_clause),
                    where=lhs_payload.where + rhs_payload.where,
                    columns_available=columns_available,
                )
            case Select():
                from_clause = self._select_to_executable(relation, ()).subquery()
                columns_available = self.extract_mapping(relation.columns, from_clause.columns)
                return Payload(from_clause, columns_available=columns_available)
            case Materialization(name=name):
                raise EngineError(
                    f"Cannot persist materialization {name!r} during SQL conversion; "
                    "use `lsst.daf.relation.Processor` first to handle this operation."
                )
            case Transfer(target=target):
                raise EngineError(
                    f"Cannot handle transfer from {target.engine} during SQL conversion; "
                    "use `lsst.daf.relation.Processor` first to handle this operation."
                )
        raise NotImplementedError(f"Unsupported relation type {relation} for engine {self}.")

    def convert_column_expression(
        self, expression: ColumnExpression, columns_available: Mapping[ColumnTag, _L]
    ) -> _L:
        """Convert a `.ColumnExpression` to a logical column.

        Parameters
        ----------
        expression : `.ColumnExpression`
            Expression to convert.
        columns_available : `~collections.abc.Mapping`
            Mapping from `.ColumnTag` to logical column, typically produced by
            `extract_mapping` or obtained from `Payload.columns_available`.

        Returns
        -------
        logical_column
            SQLAlchemy expression object or other logical column value.

        See Also
        --------
        :ref:`lsst.daf.relation-sql-logical-columns`
        """
        match expression:
            case ColumnLiteral(value=value):
                return self.convert_column_literal(value)
            case ColumnReference(tag=tag):
                return columns_available[tag]
            case ColumnFunction(name=name, args=args):
                sql_args = [self.convert_column_expression(arg, columns_available) for arg in args]
                if (function := self.get_function(name)) is not None:
                    return cast(_L, function(*sql_args))
                return getattr(sql_args[0], name)(*sql_args[1:])
        raise AssertionError(
            f"matches should be exhaustive and all branches should return; got {expression!r}."
        )

    def convert_column_literal(self, value: Any) -> _L:
        """Convert a Python literal value to a logical column.

        Parameters
        ----------
        value
            Python value to convert.

        Returns
        -------
        logical_column
            SQLAlchemy expression object or other logical column value.

        Notes
        -----
        This method must be overridden to support a custom logical columns.

        See Also
        --------
        :ref:`lsst.daf.relation-sql-logical-columns`
        """
        return cast(_L, sqlalchemy.sql.literal(value))

    def expect_column_scalar(self, logical_column: _L) -> sqlalchemy.sql.ColumnElement:
        """Convert a logical column value to a SQLAlchemy expression.

        Parameters
        ----------
        logical_column
            SQLAlchemy expression object or other logical column value.

        Returns
        -------
        sql : `sqlalchemy.sql.ColumnElement`
            SQLAlchemy expression object.

        Notes
        -----
        The default implementation assumes the logical column type is just a
        SQLAlchemy type and returns the given object unchanged.  Subclasses
        with a custom logical column type should override to at least assert
        that the value is in fact a SQLAlchemy expression.  This is only called
        in contexts where true SQLAlchemy expressions are required, such as in
        ``ORDER BY`` or ``WHERE`` clauses.

        See Also
        --------
        :ref:`lsst.daf.relation-sql-logical-columns`
        """
        return cast(sqlalchemy.sql.ColumnElement, logical_column)

    def convert_flattened_predicate(
        self, predicate: Predicate, columns_available: Mapping[ColumnTag, _L]
    ) -> list[sqlalchemy.sql.ColumnElement]:
        """Flatten all logical AND operators in a `.Predicate` and convert each
        to a boolean SQLAlchemy expression.

        Parameters
        ----------
        predicate : `.Predicate`
            Predicate to convert.
        columns_available : `~collections.abc.Mapping`
            Mapping from `.ColumnTag` to logical column, typically produced by
            `extract_mapping` or obtained from `Payload.columns_available`.

        Returns
        -------
        sql : `list` [ `sqlalchemy.sql.ColumnElement` ]
            List of boolean SQLAlchemy expressions to be combined with the
            ``AND`` operator.
        """
        if (flattened := flatten_logical_and(predicate)) is False:
            return [sqlalchemy.sql.literal(False)]
        else:
            return [self.convert_predicate(p, columns_available) for p in flattened]

    def convert_predicate(
        self, predicate: Predicate, columns_available: Mapping[ColumnTag, _L]
    ) -> sqlalchemy.sql.ColumnElement:
        """Convert a `.Predicate` to a SQLAlchemy expression.

        Parameters
        ----------
        predicate : `.Predicate`
            Predicate to convert.
        columns_available : `~collections.abc.Mapping`
            Mapping from `.ColumnTag` to logical column, typically produced by
            `extract_mapping` or obtained from `Payload.columns_available`.

        Returns
        -------
        sql : `sqlalchemy.sql.ColumnElement`
            Boolean SQLAlchemy expression.
        """
        match predicate:
            case PredicateFunction(name=name, args=args):
                sql_args = [self.convert_column_expression(arg, columns_available) for arg in args]
                if (function := self.get_function(name)) is not None:
                    return function(*sql_args)
                return getattr(sql_args[0], name)(*sql_args[1:])
            case LogicalAnd(operands=operands):
                if not operands:
                    return sqlalchemy.sql.literal(True)
                if len(operands) == 1:
                    return self.convert_predicate(operands[0], columns_available)
                else:
                    return sqlalchemy.sql.and_(
                        *[self.convert_predicate(operand, columns_available) for operand in operands]
                    )
            case LogicalOr(operands=operands):
                if not operands:
                    return sqlalchemy.sql.literal(False)
                if len(operands) == 1:
                    return self.convert_predicate(operands[0], columns_available)
                else:
                    return sqlalchemy.sql.or_(
                        *[self.convert_predicate(operand, columns_available) for operand in operands]
                    )
            case LogicalNot(operand=operand):
                return sqlalchemy.sql.not_(self.convert_predicate(operand, columns_available))
            case PredicateReference(tag=tag):
                return self.expect_column_scalar(columns_available[tag])
            case PredicateLiteral(value=value):
                return sqlalchemy.sql.literal(value)
            case ColumnInContainer(item=item, container=container):
                sql_item = self.expect_column_scalar(self.convert_column_expression(item, columns_available))
                match container:
                    case ColumnRangeLiteral(value=range(start=start, stop=stop_exclusive, step=step)):
                        # The convert_column_literal calls below should just
                        # call sqlalchemy.sql.literal(int), which would also
                        # happen automatically internal to any of the other
                        # sqlalchemy function calls, but they get the typing
                        # right, reflecting the fact that the derived engine is
                        # supposed to have final say over how we convert
                        # literals.
                        stop_inclusive = stop_exclusive - 1
                        if start == stop_inclusive:
                            return sql_item == self.convert_column_literal(start)
                        else:
                            target = sqlalchemy.sql.between(
                                sql_item,
                                self.convert_column_literal(start),
                                self.convert_column_literal(stop_inclusive),
                            )
                            if step != 1:
                                return sqlalchemy.sql.and_(
                                    *[
                                        target,
                                        sql_item % self.convert_column_literal(step)
                                        == self.convert_column_literal(start % step),
                                    ]
                                )
                            else:
                                return target
                    case ColumnExpressionSequence(items=items):
                        return sql_item.in_(
                            [self.convert_column_expression(item, columns_available) for item in items]
                        )
        raise AssertionError(
            f"matches should be exhaustive and all branches should return; got {predicate!r}."
        )

    def convert_sort_term(
        self, term: SortTerm, columns_available: Mapping[ColumnTag, _L]
    ) -> sqlalchemy.sql.ColumnElement:
        """Convert a `.SortTerm` to a SQLAlchemy expression.

        Parameters
        ----------
        term : `.SortTerm`
            Sort term to convert.
        columns_available : `~collections.abc.Mapping`
            Mapping from `.ColumnTag` to logical column, typically produced by
            `extract_mapping` or obtained from `Payload.columns_available`.

        Returns
        -------
        sql : `sqlalchemy.sql.ColumnElement`
            Scalar SQLAlchemy expression.
        """
        result = self.expect_column_scalar(self.convert_column_expression(term.expression, columns_available))
        if term.ascending:
            return result
        else:
            return result.desc()
