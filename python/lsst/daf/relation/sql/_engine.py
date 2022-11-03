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
from .._exceptions import EngineError
from .._leaf_relation import LeafRelation
from .._operation_relations import BinaryOperationRelation, UnaryOperationRelation
from .._operations import (
    Calculation,
    Chain,
    Deduplication,
    Join,
    Materialization,
    Projection,
    Selection,
    Slice,
    Sort,
    SortTerm,
    Transfer,
)
from .._unary_operation import UnaryOperation
from ._select_parts import SelectParts

if TYPE_CHECKING:
    from .._relation import Relation


_L = TypeVar("_L")


@dataclasses.dataclass(repr=False, eq=False, kw_only=True)
class Engine(
    GenericConcreteEngine[Callable[..., sqlalchemy.sql.ColumnElement]],
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

    def preserves_order(self, operation: UnaryOperation | Chain) -> bool:
        # Docstring inherited.
        match operation:
            case Slice() | Deduplication():
                # SELECT DISTINCT ... ORDER BY preserves the ORDER BY,
                # as does SELECT ... ORDER BY ... OFFSET ... LIMIT.
                # Since the only way to get an ordered relation before either
                # a Slice or a Deduplication is to have them immediately after
                # a Sort, and that won't result in a subquery before the
                # DISTINCT or OFFSET ... LIMIT is a applied, we can
                # declare that these operations preserve order.
                return True
            case Transfer(destination=destination):
                if destination == self:
                    # Transfer *to* SQL usually means insertion into a
                    # [temporary] table, which does not preserve order when
                    # that table is then included in a query.  While it would
                    # be possible to insert those rows with a column that
                    # records the original order, controlling when to include
                    # an ORDER BY on that column when querying the table later
                    # would be a lot of additional complexity, so we'll add it
                    # in the future only if there's a demonstrated need.
                    return False
                else:
                    # Transfer *from* SQL also preserves order, since that
                    # means executing a SELECT ... ORDER BY and iterating over
                    # the rows somehow, as long as the destination also
                    # preserves order.
                    return destination.preserves_order(operation)
            case _:
                return False

    def __str__(self) -> str:
        return self.name

    def __repr__(self) -> str:
        return f"lsst.daf.relation.sql.Engine({self.name!r})@{id(self):0x}"

    def make_leaf(
        self,
        columns: Set[ColumnTag],
        payload: SelectParts[_L],
        *,
        min_rows: int = 0,
        max_rows: int | None = None,
        name: str = "",
        messages: Sequence[str] = (),
        name_prefix: str = "leaf",
        parameters: Any = None,
    ) -> LeafRelation:
        """Create a nontrivial leaf relation in this engine.

        This is a convenience method that simply forwards all arguments to
        the `.LeafRelation` constructor; see that class for details.
        """
        return LeafRelation(
            self,
            columns,
            payload,
            min_rows=min_rows,
            max_rows=max_rows,
            messages=messages,
            name=name,
            name_prefix=name_prefix,
            parameters=parameters,
        )

    def to_executable(
        self,
        relation: Relation,
        *,
        distinct: bool = False,
        order_by: Sequence[SortTerm] = (),
        offset: int = 0,
        limit: int | None = None,
    ) -> sqlalchemy.sql.expression.SelectBase:
        """Convert a relation tree to an executable SQLAlchemy expression.

        Parameters
        ----------
        relation : `.Relation`
            The relation tree to convert.
        distinct : `bool`
            Whether to generate an expression whose rows are forced to be
            unique.
        order_by : `Iterable` [ `.SortTerm` ]
            Iterable of objects that specify a sort order.
        offset : `int`, optional
            Starting index for returned rows, with ``0`` as the first row.
        limit : `int` or `None`, optional
            Maximum number of rows returned, or `None` (default) for no limit.

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
                f"Engine {self!r} cannot operate on relation {relation} with engine {relation.engine!r}. x"
                "Use lsst.daf.relation.Processor to evaluate transfers first."
            )
        match relation:
            case LeafRelation():
                return SelectParts.from_relation(relation, self).to_executable(
                    relation.columns,
                    self,
                    distinct=distinct,
                    order_by=order_by,
                    offset=offset,
                    limit=limit,
                )
            case UnaryOperationRelation(operation=operation, target=target):
                match operation:
                    case Deduplication():
                        return self.to_executable(
                            target, distinct=True, order_by=order_by, offset=offset, limit=limit
                        )
                    case Slice():
                        if offset or limit:
                            # This call wants to impose another slice on top of
                            # the existing one, via these kwargs.  We apply
                            # that as a Slice on top of the existing relation,
                            # and then try again with those kwargs reset to
                            # their default no-op values.  That will land us
                            # back in the 'else' branch, but with a merged
                            # operation thanks to logic in Slice.apply.
                            new_relation = Slice(
                                start=offset, stop=offset + limit if limit is not None else None
                            ).apply(relation)
                            return self.to_executable(new_relation, order_by=order_by, distinct=distinct)
                        elif distinct or order_by:
                            # This call wants to impose operations on the final
                            # result that don't commute with slicing, and SQL
                            # would normally apply those in an order
                            # inconsistent with what the relation tree says if
                            # we just slapped the corresponding modifiers on
                            # the exiting SELECT statement (e.g.  "SELECT
                            # DISTINCT ... LIMIT .." will do the DISTINCT
                            # before the LIMIT).  So we delegate to
                            # SelectParts.from_relation, which will call back
                            # here but land in the 'else' branch this time.
                            # And then SelectParts.to_executable will wrap that
                            # in a subquery where we can apply the new order_by
                            # and/or distinct.
                            return SelectParts.from_relation(relation, self).to_executable(
                                relation.columns,
                                self,
                                distinct=distinct,
                                order_by=order_by,
                            )
                        else:
                            # This call doesn't apply any Slice operations or
                            # operations that don't commute with Slice
                            # operations, so we can recurse with the Slice's
                            # operations on its target, applied via kwargs.
                            # Slice.apply guarantees there are no back-to-back
                            # Slices in a relation tree.
                            return self.to_executable(
                                target,
                                distinct=distinct,
                                order_by=order_by,
                                offset=operation.start,
                                limit=operation.limit,
                            )
                    case Sort():
                        # We don't care in this branch whether the call
                        # includes:
                        # - distinct=True (because that commutes with sorting)
                        # - nontrivial offset/limit (because the call's
                        #   operation is considered to act after the relation
                        #   operation's sorting, and that's the order SQL
                        #   applies those operations when ORDER BY and LIMIT
                        #   and/or OFFSET are present in the same statement)
                        if order_by:
                            # This call wants to impose its own sorting.  We
                            # apply that as a Sort on top of the existing
                            # relation, and then try again with order_by=().
                            # That will land us back in the 'else' branch, but
                            # with a merged operation thanks to logic in
                            # Sort.apply.
                            new_relation = Sort(order_by).apply(relation)
                            return self.to_executable(
                                new_relation, distinct=distinct, offset=offset, limit=limit
                            )
                        else:
                            # This call doesn't apply any sort operations, so
                            # we can recurse with the Sort's operations on its
                            # target applied via kwargs.  Sort.apply guarantees
                            # that a relation tree never has back-to-back
                            # Sorts.
                            return self.to_executable(
                                target,
                                distinct=distinct,
                                order_by=operation.terms,
                                offset=offset,
                                limit=limit,
                            )
                    case Transfer(destination=destination):
                        raise EngineError(
                            f"Engine {self!r} cannot handle transfer from "
                            f"{target.engine!r} to {destination!r}; "
                            "use `lsst.daf.relation.Processor` first to handle this operation."
                        )
                    case Calculation() | Materialization() | Projection() | Selection():
                        return SelectParts.from_relation(relation, self).to_executable(
                            relation.columns,
                            self,
                            distinct=distinct,
                            order_by=order_by,
                            offset=offset,
                            limit=limit,
                        )
                    case _:
                        return self.apply_custom_unary_operation(operation, target)
            case BinaryOperationRelation(operation=operation, lhs=lhs, rhs=rhs):
                match operation:
                    case Chain():
                        lhs_result = self.to_executable(lhs)
                        rhs_result = self.to_executable(rhs)
                        result: sqlalchemy.sql.CompoundSelect = (
                            sqlalchemy.sql.union(lhs_result, rhs_result)
                            if distinct
                            else sqlalchemy.sql.union_all(lhs_result, rhs_result)
                        )
                        if order_by:
                            columns_available = self.extract_mapping(
                                relation.columns,
                                result.selected_columns,
                            )
                            result = result.order_by(
                                *[self.convert_sort_term(term, columns_available) for term in order_by]
                            )
                        if offset:
                            result = result.offset(offset)
                        if limit is not None:
                            result = result.limit(limit)
                        return result
                    case Join():
                        return SelectParts.from_relation(relation, self).to_executable(
                            relation.columns,
                            self,
                            distinct=distinct,
                            order_by=order_by,
                            offset=offset,
                            limit=limit,
                        )
                raise EngineError(f"Custom binary operation {operation} is not supported.")
        raise AssertionError(f"Match should be exhaustive and all branches should return; got {relation}.")

    def get_join_identity_payload(self) -> SelectParts[_L]:
        # Docstring inherited.
        return SelectParts[_L](self.make_identity_subquery())

    def get_doomed_payload(self, columns: Set[ColumnTag]) -> SelectParts[_L]:
        # Docstring inherited.
        return SelectParts(self.make_doomed_select(columns).subquery())

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
        return {tag: cast(_L, sql_columns[tag.qualified_name]) for tag in tags}

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
            `extract_mapping` or obtained from `SelectParts.columns_available`.
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
        select_columns = [
            cast(sqlalchemy.sql.ColumnElement, logical_column).label(tag.qualified_name)
            for tag, logical_column in items
        ]
        select_columns.extend(extra)
        self.handle_empty_columns(select_columns)
        return sqlalchemy.sql.select(*select_columns).select_from(sql_from)

    def make_doomed_select(self, tags: Set[ColumnTag]) -> sqlalchemy.sql.Select:
        """Construct a SQLAlchemy SELECT query that yields no rows.

        Parameters
        ----------
        tags : `~collections.abc.Set`
            Set of tags for the columns the query should have.

        Returns
        -------
        zero_select : `sqlalchemy.sql.Select`
            SELECT query that yields no rows.

        Notes
        -----
        This method is responsible for handling the case where ``items`` is
        empty, typically by delegating to `handle_empty_columns`.

        This method must be overridden to support a custom logical columns.
        """
        select_columns = [sqlalchemy.sql.literal(None).label(tag.qualified_name) for tag in tags]
        self.handle_empty_columns(select_columns)
        return sqlalchemy.sql.select(*select_columns).where(sqlalchemy.sql.literal(False))

    def make_identity_subquery(self) -> sqlalchemy.sql.FromClause:
        """Construct a SQLAlchemy FROM clause with one row and no (meaningful)
        columns.

        Returns
        -------
        identity_from : `sqlalchemy.sql.FromClause`
            FROM clause with one column and no meaningful columns.

        Notes
        -----
        SQL SELECT queries and similar queries are not permitted to actually
        have no columns, but we can add a literal column that isn't associated
        with any `.ColumnTag`, making it appear to the relation system as if
        there are no columns.  The default implementation does this by
        delegating to `handle_empty_columns`.
        """
        select_columns: list[sqlalchemy.sql.ColumnElement] = []
        self.handle_empty_columns(select_columns)
        return sqlalchemy.sql.select(*select_columns).subquery()

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
            `extract_mapping` or obtained from `SelectParts.columns_available`.

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
                    return function(*sql_args)
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
        return sqlalchemy.sql.literal(value)

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
            `extract_mapping` or obtained from `SelectParts.columns_available`.

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
            `extract_mapping` or obtained from `SelectParts.columns_available`.

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
                        self.convert_predicate(operand, columns_available) for operand in operands
                    )
            case LogicalOr(operands=operands):
                if not operands:
                    return sqlalchemy.sql.literal(False)
                if len(operands) == 1:
                    return self.convert_predicate(operands[0], columns_available)
                else:
                    return sqlalchemy.sql.or_(
                        self.convert_predicate(operand, columns_available) for operand in operands
                    )
            case LogicalNot(operand=operand):
                return sqlalchemy.sql.logical_not(self.convert_predicate(operand, columns_available))
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
                            return [sql_item == self.convert_column_literal(start)]
                        else:
                            target = sqlalchemy.sql.between(
                                sql_item,
                                self.convert_column_literal(start),
                                self.convert_column_literal(stop_inclusive),
                            )
                            if step != 1:
                                return [
                                    target,
                                    sql_item % self.convert_column_literal(step)
                                    == self.convert_column_literal(start % step),
                                ]
                            else:
                                return [target]
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
            `extract_mapping` or obtained from `SelectParts.columns_available`.

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

    def apply_custom_unary_operation(
        self, operation: UnaryOperation, target: Relation
    ) -> sqlalchemy.sql.expression.SelectBase:
        """Convert a custom `.UnaryOperation` to a SQL executable.

        This method must be implemented in a subclass engine in order to
        support any custom `.UnaryOperation`.

        Parameters
        ----------
        operation : `.UnaryOperation`
            Operation to apply.  Guaranteed to be a `.Marker`, `.Reordering`,
            or `.RowFilter` subclass.
        target : `.Relation`
            Target of the unary operation.  Typically this will be passed to
            `to_executable` or `SelectParts.from_relation`, and the result used
            to construct a new SQL executable.

        Returns
        -------
        sql : `sqlalchemy.sql.expression.SelectBase`
            SQLAlchemy executable representing this relation.
        """
        raise EngineError(f"Custom operation {operation} not supported by engine {self}.")
