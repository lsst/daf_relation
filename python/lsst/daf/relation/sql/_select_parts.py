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

__all__ = ("FullSelectParts", "SelectParts")

import dataclasses
from collections.abc import Iterable, Mapping, Set
from typing import TYPE_CHECKING, Generic, TypeVar

import sqlalchemy

from .._columns import ColumnTag
from .._exceptions import EngineError
from .._operation_relations import BinaryOperationRelation, UnaryOperationRelation
from .._operations import Calculation, Join, Materialization, Projection, Selection, SortTerm

if TYPE_CHECKING:
    from .._relation import Relation
    from ._engine import Engine


_L = TypeVar("_L")


@dataclasses.dataclass(eq=False)
class SelectParts(Generic[_L]):
    """A struct that represents a SQL table or simple ``SELECT`` query."""

    from_clause: sqlalchemy.sql.FromClause
    """SQLAlchemy representation of the FROM clause or table
    (`sqlalchemy.sql.FromClause`).
    """

    where: list[sqlalchemy.sql.ColumnElement] = dataclasses.field(default_factory=list)
    """SQLAlchemy representation of the WHERE clause, as a sequence of
    boolean expressions to be combined with ``AND``
    (`Sequence` [ `sqlalchemy.sql.ColumnElement` ]).
    """

    columns_available: dict[ColumnTag, _L] | None = None
    """Mapping from `.ColumnTag` to logical column for the columns available
    from the FROM clause (`dict` or `None`).

    If `None`, the columns available are just the columns provided by the
    relation these parts represent, and they can be obtained as needed by
    calling `Engine.extract_mapping` on `from_clause`.  This is an optimization
    that avoids calls to `Engine.extract_mapping` when `columns_available`
    isn't actually needed.
    """

    def to_executable(
        self,
        select_columns: Iterable[ColumnTag],
        engine: Engine[_L],
        *,
        distinct: bool = False,
        order_by: Iterable[SortTerm] = (),
        offset: int = 0,
        limit: int | None = None,
        extra_columns: Iterable[sqlalchemy.sql.ColumnElement] = (),
    ) -> sqlalchemy.sql.Select:
        """Create a SQL executable from this struct.

        Parameters
        ----------
        select_columns : `~collections.abc.Iterable`
            Columns for the SELECT clause.  Also used to construct
            `columns_available` if that is `None`.
        engine : `Engine`
            SQL engine object.
        distinct : `bool`
            Whether to generate an expression whose rows are forced to be
            unique.
        order_by : `~collections.abc.Iterable` [ `.SortTerm` ]
            Iterable of objects that specify a sort order.
        offset : `int`, optional
            Starting index for returned rows, with ``0`` as the first row.
        limit : `int` or `None`, optional
            Maximum number of rows returned, or `None` (default) for no limit.
        extra_columns : `~colletions.abc.Iterable` [ \
                `sqlalchemy.sql.ColumnElement` ]
            Extra SQLAlchemy expressions to include in the ``SELECT`` clause.

        Returns
        -------
        select : `sqlalchemy.sql.Select`
            SQL ``SELECT`` statement.
        """
        if not select_columns and not extra_columns:
            extra_columns = list(extra_columns)
            engine.handle_empty_columns(extra_columns)
        if self.columns_available is None:
            columns_available: Mapping[ColumnTag, _L] = engine.extract_mapping(
                select_columns, self.from_clause.columns
            )
            columns_projected = columns_available
        else:
            columns_available = self.columns_available
            columns_projected = {tag: columns_available[tag] for tag in select_columns}
        select = engine.select_items(columns_projected.items(), self.from_clause, *extra_columns)
        if len(self.where) == 1:
            select = select.where(self.where[0])
        elif self.where:
            select = select.where(sqlalchemy.sql.and_(*self.where))
        if distinct:
            select = select.distinct()
        if order_by:
            select = select.order_by(
                *[engine.convert_sort_term(term, columns_available) for term in order_by]
            )
        if offset:
            select = select.offset(offset)
        if limit is not None:
            select = select.limit(limit)
        return select

    def full_copy(self, columns: Set[ColumnTag], engine: Engine[_L]) -> FullSelectParts[_L]:
        """Create a copy of this struct with `columns_available` not `None`.

        Parameters
        ----------
        columns : `~collections.abc.Set` [ `ColumnTag` ]
            Columns to extract; typically `.Relation.columns`.
        engine : `Engine`
            SQL engine object.
        """
        if self.columns_available is None:
            columns_available = engine.extract_mapping(columns, self.from_clause.columns)
        else:
            columns_available = dict(self.columns_available)
        return FullSelectParts(
            from_clause=self.from_clause,
            where=list(self.where),
            columns_available=columns_available,
        )

    @classmethod
    def from_relation(cls, relation: Relation, engine: Engine[_L]) -> SelectParts[_L]:
        """Construct from a relation, processing it as necessary.

        Parameters
        ----------
        relation : `Relation`
            Relation to process.
        engine : `Engine`
            Engine all relations must belong to.

        Returns
        -------
        select_parts : `SelectParts`
            New struct that represents the relation as a SQL query.

        Notes
        -----
        This method is used to provide much of the implementation of
        `Engine.to_executable`, and vice versa (each deals with certain
        operation types, and delegates th others).
        """
        if relation.engine != engine:
            raise EngineError(
                f"Engine {engine!r} cannot operate on relation {relation} with engine {relation.engine!r}. "
                "Use lsst.daf.relation.Processor to evaluate transfers first."
            )
        if (result := relation.payload) is not None:
            return result
        match relation:
            case UnaryOperationRelation(operation=operation, target=target):
                match operation:
                    case Materialization(name=name):
                        raise EngineError(
                            f"Cannot persist materialization {name!r} during SQL conversion; "
                            "use `lsst.daf.relation.Processor` first to handle this operation."
                        )
                    case Calculation(tag=tag, expression=expression):
                        result = FullSelectParts.from_relation(target, engine)
                        result.columns_available[tag] = engine.convert_column_expression(
                            expression, result.columns_available
                        )
                        return result
                    case Projection():
                        # We can just recurse to target because Projection only
                        # affects Engine.to_executable, and the default
                        # implementation for that already only selects the
                        # relation's own columns.  From the SQL perspective,
                        # this means we don't actually make a SELECT subquery
                        # whenever we see a Projection - we wait until some
                        # outer operation wants the executable form, and then
                        # put the right columns there.
                        return SelectParts.from_relation(target, engine)
                    case Selection(predicate=predicate):
                        result = FullSelectParts.from_relation(
                            target, engine, columns_required=predicate.columns_required
                        )
                        result.where.extend(
                            engine.convert_flattened_predicate(predicate, result.columns_available)
                        )
                        return result
            case BinaryOperationRelation(
                operation=Join(predicate=predicate, common_columns=common_columns),
                lhs=lhs,
                rhs=rhs,
            ):
                assert common_columns is not None, "Guaranteed by Join.apply and PartialJoin.apply."
                lhs_parts = FullSelectParts.from_relation(
                    lhs, engine, columns_required=(predicate.columns_required & lhs.columns) | common_columns
                )
                rhs_parts = FullSelectParts.from_relation(
                    rhs, engine, columns_required=(predicate.columns_required & rhs.columns) | common_columns
                )
                on_terms: list[sqlalchemy.sql.ColumnElement] = []
                if common_columns:
                    on_terms.extend(
                        lhs_parts.columns_available[tag] == rhs_parts.columns_available[tag]
                        for tag in common_columns
                    )
                columns_available = {**lhs_parts.columns_available, **rhs_parts.columns_available}
                if predicate.as_trivial() is not True:
                    on_terms.extend(engine.convert_flattened_predicate(predicate, columns_available))
                on_clause: sqlalchemy.sql.ColumnElement
                if not on_terms:
                    on_clause = sqlalchemy.sql.literal(True)
                elif len(on_terms) == 1:
                    on_clause = on_terms[0]
                else:
                    on_clause = sqlalchemy.sql.and_(*on_terms)
                return SelectParts(
                    from_clause=lhs_parts.from_clause.join(rhs_parts.from_clause, onclause=on_clause),
                    where=lhs_parts.where + rhs_parts.where,
                    columns_available=columns_available,
                )
        return SelectParts(engine.to_executable(relation).subquery())


@dataclasses.dataclass(eq=False)
class FullSelectParts(SelectParts[_L]):
    """A complete variant of `SelectParts`."""

    columns_available: dict[ColumnTag, _L] = dataclasses.field(default_factory=dict)
    """Mapping from `.ColumnTag` to logical column for the columns available
    from the FROM clause (`dict`).
    """

    @classmethod
    def from_relation(
        cls, relation: Relation, engine: Engine[_L], *, columns_required: Set[ColumnTag] = frozenset()
    ) -> FullSelectParts[_L]:
        # Docstring inherited.
        return SelectParts.from_relation(relation, engine).full_copy(relation.columns, engine)
