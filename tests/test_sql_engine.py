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

import unittest
from typing import TypeAlias

import sqlalchemy
from lsst.daf.relation import (
    ColumnExpression,
    ColumnTag,
    Identity,
    IgnoreOne,
    Relation,
    RelationalAlgebraError,
    SortTerm,
    sql,
    tests,
)

_L: TypeAlias = sqlalchemy.sql.ColumnElement


def _make_leaf(
    engine: sql.Engine, md: sqlalchemy.schema.MetaData, name: str, *columns: ColumnTag
) -> Relation:
    """Make a leaf relation in a SQL engine, backed by SQLAlchemy tables.

    This doesn't actually create tables in any database (even in memory); it
    just creates their SQLAlchemy representations so their names and columns
    render as expected in SQL strings.

    Parameters
    ----------
    engine : `lsst.daf.relation.sql.Engine`
        Relation engine for the new leaf.
    md : `sqlalchemy.schema.MetaData`
        SQLAlchemy metadata object to add tables to.
    name : `str`
        Name of the relation and its table.
    *columns: `ColumnTag`
        Columns to include in the relation and its table.

    Returns
    -------
    leaf : `Relation`
        Leaf relation backed by the new table.
    """
    columns_available = {
        tag: sqlalchemy.schema.Column(tag.qualified_name, sqlalchemy.Integer) for tag in columns
    }
    table = sqlalchemy.schema.Table(name, md, *columns_available.values())
    payload = sql.Payload[_L](from_clause=table, columns_available=columns_available)
    return engine.make_leaf(columns_available.keys(), payload=payload, name=name)


class SqlEngineTestCase(tests.RelationTestCase):
    """Test the SQL engine."""

    def setUp(self):
        self.maxDiff = None

    def test_select_operations(self) -> None:
        """Test SQL engine conversion for different combinations and
        permutations of the operation types managed by the `Select` marker
        relation.
        """
        engine = sql.Engine[_L]()
        md = sqlalchemy.schema.MetaData()
        a = tests.ColumnTag("a")
        b = tests.ColumnTag("b")
        c = tests.ColumnTag("c")
        d = tests.ColumnTag("d")
        expression = ColumnExpression.reference(b).method("__neg__")
        predicate = ColumnExpression.reference(c).gt(ColumnExpression.literal(0))
        terms = [SortTerm(ColumnExpression.reference(b))]
        leaf1 = _make_leaf(engine, md, "leaf1", a, b)
        leaf2 = _make_leaf(engine, md, "leaf2", a, c)
        r = leaf1.with_calculated_column(d, expression).join(leaf2.with_rows_satisfying(predicate))
        self.check_sql_str(
            "SELECT leaf2.a AS a, leaf1.b AS b, leaf2.c AS c, -leaf1.b AS d "
            "FROM leaf1 JOIN leaf2 ON leaf1.a = leaf2.a WHERE leaf2.c > 0",
            engine.to_executable(r),
        )
        # Add modifiers to that query via relation operations.
        self.check_sql_str(
            "SELECT DISTINCT leaf2.a AS a, leaf1.b AS b, leaf2.c AS c, -leaf1.b AS d "
            "FROM leaf1 JOIN leaf2 ON leaf1.a = leaf2.a WHERE leaf2.c > 0",
            engine.to_executable(r.without_duplicates()),
        )
        self.check_sql_str(
            "SELECT leaf1.b AS b, leaf2.c AS c, -leaf1.b AS d "
            "FROM leaf1 JOIN leaf2 ON leaf1.a = leaf2.a WHERE leaf2.c > 0",
            engine.to_executable(r.with_only_columns({b, c, d})),
        )
        self.check_sql_str(
            "SELECT leaf2.a AS a, leaf1.b AS b, leaf2.c AS c, -leaf1.b AS d "
            "FROM leaf1 JOIN leaf2 ON leaf1.a = leaf2.a WHERE leaf2.c > 0 ORDER BY leaf1.b",
            engine.to_executable(r.sorted(terms)),
        )
        self.check_sql_str(
            "SELECT leaf2.a AS a, leaf1.b AS b, leaf2.c AS c, -leaf1.b AS d "
            "FROM leaf1 JOIN leaf2 ON leaf1.a = leaf2.a WHERE leaf2.c > 0 LIMIT 3 OFFSET 2",
            engine.to_executable(r[2:5]),
        )
        # Add both a Projection and then a Deduplication.
        self.check_sql_str(
            "SELECT DISTINCT leaf1.b AS b, leaf2.c AS c, -leaf1.b AS d "
            "FROM leaf1 JOIN leaf2 ON leaf1.a = leaf2.a WHERE leaf2.c > 0",
            engine.to_executable(r.with_only_columns({b, c, d}).without_duplicates()),
        )
        # Add a Deduplication and then a Projection, which requires a subquery.
        self.check_sql_str(
            "SELECT anon_1.b AS b, anon_1.c AS c, anon_1.d AS d FROM ("
            "SELECT DISTINCT leaf2.a AS a, leaf1.b AS b, leaf2.c AS c, -leaf1.b AS d "
            "FROM leaf1 JOIN leaf2 ON leaf1.a = leaf2.a WHERE leaf2.c > 0"
            ") AS anon_1",
            engine.to_executable(r.without_duplicates().with_only_columns({b, c, d})),
        )
        # Projection and Sort together, in any order.
        self.check_sql_str(
            "SELECT leaf1.b AS b, leaf2.c AS c, -leaf1.b AS d "
            "FROM leaf1 JOIN leaf2 ON leaf1.a = leaf2.a WHERE leaf2.c > 0 ORDER BY leaf1.b",
            engine.to_executable(r.with_only_columns({b, c, d}).sorted(terms)),
            engine.to_executable(r.sorted(terms).with_only_columns({b, c, d})),
        )
        # Projection and Slice together, in any order.
        self.check_sql_str(
            "SELECT leaf1.b AS b, leaf2.c AS c, -leaf1.b AS d "
            "FROM leaf1 JOIN leaf2 ON leaf1.a = leaf2.a WHERE leaf2.c > 0 LIMIT 2 OFFSET 1",
            engine.to_executable(r.with_only_columns({b, c, d})[1:3]),
            engine.to_executable(r[1:3].with_only_columns({b, c, d})),
        )
        # Deduplication and Sort together, in any order.
        self.check_sql_str(
            "SELECT DISTINCT leaf2.a AS a, leaf1.b AS b, leaf2.c AS c, -leaf1.b AS d "
            "FROM leaf1 JOIN leaf2 ON leaf1.a = leaf2.a WHERE leaf2.c > 0 ORDER BY leaf1.b",
            engine.to_executable(r.without_duplicates().sorted(terms)),
            engine.to_executable(r.sorted(terms).without_duplicates()),
        )
        # Deduplication and then Slice.
        self.check_sql_str(
            "SELECT DISTINCT leaf2.a AS a, leaf1.b AS b, leaf2.c AS c, -leaf1.b AS d "
            "FROM leaf1 JOIN leaf2 ON leaf1.a = leaf2.a WHERE leaf2.c > 0 LIMIT 2 OFFSET 1",
            engine.to_executable(r.without_duplicates()[1:3]),
        )
        # Slice and then Deduplication, which requires a subquery.
        self.check_sql_str(
            "SELECT DISTINCT anon_1.a AS a, anon_1.b AS b, anon_1.c AS c, anon_1.d AS d FROM ("
            "SELECT leaf2.a AS a, leaf1.b AS b, leaf2.c AS c, -leaf1.b AS d "
            "FROM leaf1 JOIN leaf2 ON leaf1.a = leaf2.a WHERE leaf2.c > 0 LIMIT 2 OFFSET 1"
            ") AS anon_1",
            engine.to_executable(r[1:3].without_duplicates()),
        )
        # Sort and then Slice.
        self.check_sql_str(
            "SELECT leaf2.a AS a, leaf1.b AS b, leaf2.c AS c, -leaf1.b AS d "
            "FROM leaf1 JOIN leaf2 ON leaf1.a = leaf2.a WHERE leaf2.c > 0 ORDER BY leaf1.b LIMIT 2 OFFSET 1",
            engine.to_executable(r.sorted(terms)[1:3]),
        )
        # Slice and then Sort, which requires a subquery.
        self.check_sql_str(
            "SELECT anon_1.a AS a, anon_1.b AS b, anon_1.c AS c, anon_1.d AS d FROM ("
            "SELECT leaf2.a AS a, leaf1.b AS b, leaf2.c AS c, -leaf1.b AS d "
            "FROM leaf1 JOIN leaf2 ON leaf1.a = leaf2.a WHERE leaf2.c > 0 LIMIT 2 OFFSET 1"
            ") AS anon_1 ORDER BY anon_1.b",
            engine.to_executable(r[1:3].sorted(terms)),
        )
        # Projection then Deduplication, with Sort at any point since it should
        # commute with both.
        self.check_sql_str(
            "SELECT DISTINCT leaf1.b AS b, leaf2.c AS c, -leaf1.b AS d "
            "FROM leaf1 JOIN leaf2 ON leaf1.a = leaf2.a WHERE leaf2.c > 0 ORDER BY leaf1.b",
            engine.to_executable(r.with_only_columns({b, c, d}).sorted(terms).without_duplicates()),
            engine.to_executable(r.with_only_columns({b, c, d}).without_duplicates().sorted(terms)),
            engine.to_executable(r.sorted(terms).with_only_columns({b, c, d}).without_duplicates()),
        )
        # Deduplication then Projection (via a subquery), with a Sort at any
        # point - but the engine will need to make sure the Sort always appears
        # in the outer query, since the subquery does not preserve order.
        self.check_sql_str(
            "SELECT anon_1.b AS b, anon_1.c AS c, anon_1.d AS d FROM ("
            "SELECT DISTINCT leaf2.a AS a, leaf1.b AS b, leaf2.c AS c, -leaf1.b AS d "
            "FROM leaf1 JOIN leaf2 ON leaf1.a = leaf2.a WHERE leaf2.c > 0"
            ") AS anon_1 ORDER BY anon_1.b",
            engine.to_executable(r.without_duplicates().with_only_columns({b, c, d}).sorted(terms)),
            engine.to_executable(r.without_duplicates().sorted(terms).with_only_columns({b, c, d})),
            engine.to_executable(r.sorted(terms).without_duplicates().with_only_columns({b, c, d})),
        )
        # Projection then Deduplication then Slice.
        self.check_sql_str(
            "SELECT DISTINCT leaf1.b AS b, leaf2.c AS c, -leaf1.b AS d "
            "FROM leaf1 JOIN leaf2 ON leaf1.a = leaf2.a WHERE leaf2.c > 0 LIMIT 2 OFFSET 1",
            engine.to_executable(r.with_only_columns({b, c, d}).without_duplicates()[1:3]),
        )
        # Projection and Slice in any order, then Deduplication, which requires
        # a subquery.
        self.check_sql_str(
            "SELECT DISTINCT anon_1.b AS b, anon_1.c AS c, anon_1.d AS d FROM ("
            "SELECT leaf1.b AS b, leaf2.c AS c, -leaf1.b AS d "
            "FROM leaf1 JOIN leaf2 ON leaf1.a = leaf2.a WHERE leaf2.c > 0 LIMIT 2 OFFSET 1"
            ") AS anon_1",
            engine.to_executable(r.with_only_columns({b, c, d})[1:3].without_duplicates()),
            engine.to_executable(r[1:3].with_only_columns({b, c, d}).without_duplicates()),
        )
        # Deduplication then Projection and Slice in any order, which requires
        # a subquery.
        self.check_sql_str(
            "SELECT anon_1.b AS b, anon_1.c AS c, anon_1.d AS d FROM ("
            "SELECT DISTINCT leaf2.a AS a, leaf1.b AS b, leaf2.c AS c, -leaf1.b AS d "
            "FROM leaf1 JOIN leaf2 ON leaf1.a = leaf2.a WHERE leaf2.c > 0"
            ") AS anon_1 LIMIT 2 OFFSET 1",
            engine.to_executable(r.without_duplicates().with_only_columns({b, c, d})[1:3]),
            engine.to_executable(r.without_duplicates()[1:3].with_only_columns({b, c, d})),
        )
        # Slice then Deduplication then Projection, which requires two
        # subqueries.
        self.check_sql_str(
            "SELECT anon_1.b AS b, anon_1.c AS c, anon_1.d AS d FROM ("
            "SELECT DISTINCT anon_2.a AS a, anon_2.b AS b, anon_2.c AS c, anon_2.d AS d FROM ("
            "SELECT leaf2.a AS a, leaf1.b AS b, leaf2.c AS c, -leaf1.b AS d "
            "FROM leaf1 JOIN leaf2 ON leaf1.a = leaf2.a WHERE leaf2.c > 0 LIMIT 2 OFFSET 1"
            ") AS anon_2"
            ") AS anon_1",
            engine.to_executable(r[1:3].without_duplicates().with_only_columns({b, c, d})),
        )
        # Sort then Slice, with Projection anywhere.
        self.check_sql_str(
            "SELECT leaf1.b AS b, leaf2.c AS c, -leaf1.b AS d "
            "FROM leaf1 JOIN leaf2 ON leaf1.a = leaf2.a WHERE leaf2.c > 0 ORDER BY leaf1.b LIMIT 2 OFFSET 1",
            engine.to_executable(r.sorted(terms)[1:3].with_only_columns({b, c, d})),
            engine.to_executable(r.sorted(terms).with_only_columns({b, c, d})[1:3]),
            engine.to_executable(r.with_only_columns({b, c, d}).sorted(terms)[1:3]),
        )
        # Slice then Sort then Projection, which requires a subquery.
        self.check_sql_str(
            "SELECT anon_1.b AS b, anon_1.c AS c, anon_1.d AS d FROM ("
            "SELECT leaf2.a AS a, leaf1.b AS b, leaf2.c AS c, -leaf1.b AS d "
            "FROM leaf1 JOIN leaf2 ON leaf1.a = leaf2.a WHERE leaf2.c > 0 LIMIT 2 OFFSET 1"
            ") AS anon_1 ORDER BY anon_1.b",
            engine.to_executable(r[1:3].sorted(terms).with_only_columns({b, c, d})),
        )
        # Slice and Projection in any order, then Sort, which requires a
        # subquery.
        self.check_sql_str(
            "SELECT anon_1.b AS b, anon_1.c AS c, anon_1.d AS d FROM ("
            "SELECT leaf1.b AS b, leaf2.c AS c, -leaf1.b AS d "
            "FROM leaf1 JOIN leaf2 ON leaf1.a = leaf2.a WHERE leaf2.c > 0 LIMIT 2 OFFSET 1"
            ") AS anon_1 ORDER BY anon_1.b",
            engine.to_executable(r[1:3].with_only_columns({b, c, d}).sorted(terms)),
            engine.to_executable(r.with_only_columns({b, c, d})[1:3].sorted(terms)),
        )
        # Deduplication and Sort in any order, then Slice.
        self.check_sql_str(
            "SELECT DISTINCT leaf2.a AS a, leaf1.b AS b, leaf2.c AS c, -leaf1.b AS d "
            "FROM leaf1 JOIN leaf2 ON leaf1.a = leaf2.a WHERE leaf2.c > 0 ORDER BY leaf1.b "
            "LIMIT 2 OFFSET 1",
            engine.to_executable(r.without_duplicates().sorted(terms)[1:3]),
            engine.to_executable(r.sorted(terms).without_duplicates()[1:3]),
        )
        # Duplication then Slice then Sort, which requires a subquery.
        self.check_sql_str(
            "SELECT anon_1.a AS a, anon_1.b AS b, anon_1.c AS c, anon_1.d AS d FROM ("
            "SELECT DISTINCT leaf2.a AS a, leaf1.b AS b, leaf2.c AS c, -leaf1.b AS d "
            "FROM leaf1 JOIN leaf2 ON leaf1.a = leaf2.a WHERE leaf2.c > 0 LIMIT 2 OFFSET 1"
            ") AS anon_1 ORDER BY anon_1.b",
            engine.to_executable(r.without_duplicates()[1:3].sorted(terms)),
        )
        # Slice then Sort and Deduplication in any order, which requires a
        # subquery.
        self.check_sql_str(
            "SELECT DISTINCT anon_1.a AS a, anon_1.b AS b, anon_1.c AS c, anon_1.d AS d FROM ("
            "SELECT leaf2.a AS a, leaf1.b AS b, leaf2.c AS c, -leaf1.b AS d "
            "FROM leaf1 JOIN leaf2 ON leaf1.a = leaf2.a WHERE leaf2.c > 0 LIMIT 2 OFFSET 1"
            ") AS anon_1 ORDER BY anon_1.b",
            engine.to_executable(r[1:3].without_duplicates().sorted(terms)),
            engine.to_executable(r[1:3].sorted(terms).without_duplicates()),
        )
        # Projection then Deduplication, with Sort at any point, and finally a
        # Slice.
        self.check_sql_str(
            "SELECT DISTINCT leaf1.b AS b, leaf2.c AS c, -leaf1.b AS d "
            "FROM leaf1 JOIN leaf2 ON leaf1.a = leaf2.a WHERE leaf2.c > 0 ORDER BY leaf1.b LIMIT 2 OFFSET 1",
            engine.to_executable(r.with_only_columns({b, c, d}).sorted(terms).without_duplicates()[1:3]),
            engine.to_executable(r.with_only_columns({b, c, d}).without_duplicates().sorted(terms)[1:3]),
            engine.to_executable(r.sorted(terms).with_only_columns({b, c, d}).without_duplicates()[1:3]),
        )
        # Projection, Deduplication, Slice, Sort.
        self.check_sql_str(
            "SELECT anon_1.b AS b, anon_1.c AS c, anon_1.d AS d FROM ("
            "SELECT DISTINCT leaf1.b AS b, leaf2.c AS c, -leaf1.b AS d "
            "FROM leaf1 JOIN leaf2 ON leaf1.a = leaf2.a WHERE leaf2.c > 0 LIMIT 2 OFFSET 1"
            ") AS anon_1 ORDER BY anon_1.b",
            engine.to_executable(r.with_only_columns({b, c, d}).without_duplicates()[1:3].sorted(terms)),
        )
        # Projection and Slice in any order, then Deduplication and Sort in any
        # order.
        self.check_sql_str(
            "SELECT DISTINCT anon_1.b AS b, anon_1.c AS c, anon_1.d AS d FROM ("
            "SELECT leaf1.b AS b, leaf2.c AS c, -leaf1.b AS d "
            "FROM leaf1 JOIN leaf2 ON leaf1.a = leaf2.a WHERE leaf2.c > 0 LIMIT 2 OFFSET 1"
            ") AS anon_1 ORDER BY anon_1.b",
            engine.to_executable(r.with_only_columns({b, c, d})[1:3].without_duplicates().sorted(terms)),
            engine.to_executable(r.with_only_columns({b, c, d})[1:3].sorted(terms).without_duplicates()),
            engine.to_executable(r[1:3].with_only_columns({b, c, d}).without_duplicates().sorted(terms)),
            engine.to_executable(r[1:3].with_only_columns({b, c, d}).sorted(terms).without_duplicates()),
        )
        # Projection and Sort in any order, then Slice, then Deduplication.
        self.check_sql_str(
            "SELECT DISTINCT anon_1.b AS b, anon_1.c AS c, anon_1.d AS d FROM ("
            "SELECT leaf1.b AS b, leaf2.c AS c, -leaf1.b AS d "
            "FROM leaf1 JOIN leaf2 ON leaf1.a = leaf2.a WHERE leaf2.c > 0 ORDER BY leaf1.b "
            "LIMIT 2 OFFSET 1"
            ") AS anon_1",
            engine.to_executable(r.with_only_columns({b, c, d}).sorted(terms)[1:3].without_duplicates()),
            engine.to_executable(r.sorted(terms).with_only_columns({b, c, d})[1:3].without_duplicates()),
        )
        # Deduplication then Projection (via a subquery), with a Sort at any
        # point, and then finally a Slice.
        self.check_sql_str(
            "SELECT anon_1.b AS b, anon_1.c AS c, anon_1.d AS d FROM ("
            "SELECT DISTINCT leaf2.a AS a, leaf1.b AS b, leaf2.c AS c, -leaf1.b AS d "
            "FROM leaf1 JOIN leaf2 ON leaf1.a = leaf2.a WHERE leaf2.c > 0"
            ") AS anon_1 ORDER BY anon_1.b LIMIT 2 OFFSET 1",
            engine.to_executable(r.without_duplicates().with_only_columns({b, c, d}).sorted(terms)[1:3]),
            engine.to_executable(r.without_duplicates().sorted(terms).with_only_columns({b, c, d})[1:3]),
            engine.to_executable(r.sorted(terms).without_duplicates().with_only_columns({b, c, d})[1:3]),
        )
        # Deduplication, then Projection and Slice in any order, then Sort.
        self.check_sql_str(
            "SELECT anon_1.b AS b, anon_1.c AS c, anon_1.d AS d FROM ("
            "SELECT anon_2.b AS b, anon_2.c AS c, anon_2.d AS d FROM ("
            "SELECT DISTINCT leaf2.a AS a, leaf1.b AS b, leaf2.c AS c, -leaf1.b AS d "
            "FROM leaf1 JOIN leaf2 ON leaf1.a = leaf2.a WHERE leaf2.c > 0"
            ") AS anon_2 LIMIT 2 OFFSET 1"
            ") AS anon_1 ORDER BY anon_1.b",
            engine.to_executable(r.without_duplicates().with_only_columns({b, c, d})[1:3].sorted(terms)),
            engine.to_executable(r.without_duplicates()[1:3].with_only_columns({b, c, d}).sorted(terms)),
        )
        # Sort and Deduplication in any order, then Projection and Slice in any
        # order.
        self.check_sql_str(
            "SELECT anon_1.b AS b, anon_1.c AS c, anon_1.d AS d FROM ("
            "SELECT DISTINCT leaf2.a AS a, leaf1.b AS b, leaf2.c AS c, -leaf1.b AS d "
            "FROM leaf1 JOIN leaf2 ON leaf1.a = leaf2.a WHERE leaf2.c > 0"
            ") AS anon_1 ORDER BY anon_1.b LIMIT 2 OFFSET 1",
            engine.to_executable(r.without_duplicates().sorted(terms).with_only_columns({b, c, d})[1:3]),
            engine.to_executable(r.sorted(terms).without_duplicates().with_only_columns({b, c, d})[1:3]),
            engine.to_executable(r.without_duplicates().sorted(terms)[1:3].with_only_columns({b, c, d})),
            engine.to_executable(r.sorted(terms).without_duplicates()[1:3].with_only_columns({b, c, d})),
        )
        # Sort, then Slice and Projection in any order, then Deduplication.
        self.check_sql_str(
            "SELECT DISTINCT anon_1.b AS b, anon_1.c AS c, anon_1.d AS d FROM ("
            "SELECT leaf1.b AS b, leaf2.c AS c, -leaf1.b AS d "
            "FROM leaf1 JOIN leaf2 ON leaf1.a = leaf2.a WHERE leaf2.c > 0 ORDER BY leaf1.b LIMIT 2 OFFSET 1"
            ") AS anon_1",
            engine.to_executable(r.sorted(terms)[1:3].with_only_columns({b, c, d}).without_duplicates()),
            engine.to_executable(r.sorted(terms).with_only_columns({b, c, d})[1:3].without_duplicates()),
        )
        # Slice, then Projection, then Deduplication and Sort in any order.
        self.check_sql_str(
            "SELECT DISTINCT anon_1.b AS b, anon_1.c AS c, anon_1.d AS d FROM ("
            "SELECT leaf1.b AS b, leaf2.c AS c, -leaf1.b AS d "
            "FROM leaf1 JOIN leaf2 ON leaf1.a = leaf2.a WHERE leaf2.c > 0 LIMIT 2 OFFSET 1"
            ") AS anon_1 ORDER BY anon_1.b",
            engine.to_executable(r[1:3].with_only_columns({b, c, d}).without_duplicates().sorted(terms)),
            engine.to_executable(r[1:3].with_only_columns({b, c, d}).sorted(terms).without_duplicates()),
        )
        # Slice, then Sort, then Projection, then Deduplication.
        self.check_sql_str(
            "SELECT DISTINCT anon_1.b AS b, anon_1.c AS c, anon_1.d AS d FROM ("
            "SELECT leaf2.a AS a, leaf1.b AS b, leaf2.c AS c, -leaf1.b AS d "
            "FROM leaf1 JOIN leaf2 ON leaf1.a = leaf2.a WHERE leaf2.c > 0 LIMIT 2 OFFSET 1"
            ") AS anon_1 ORDER BY anon_1.b",
            engine.to_executable(r[1:3].sorted(terms).with_only_columns({b, c, d}).without_duplicates()),
        )
        # Slice, then Sort, Deduplication, and Projection as long as the
        # Deduplication precedes the Projection.
        self.check_sql_str(
            "SELECT anon_1.b AS b, anon_1.c AS c, anon_1.d AS d FROM ("
            "SELECT DISTINCT anon_2.a AS a, anon_2.b AS b, anon_2.c AS c, anon_2.d AS d FROM ("
            "SELECT leaf2.a AS a, leaf1.b AS b, leaf2.c AS c, -leaf1.b AS d "
            "FROM leaf1 JOIN leaf2 ON leaf1.a = leaf2.a WHERE leaf2.c > 0 LIMIT 2 OFFSET 1"
            ") AS anon_2"
            ") AS anon_1 ORDER BY anon_1.b",
            engine.to_executable(r[1:3].sorted(terms).without_duplicates().with_only_columns({b, c, d})),
            engine.to_executable(r[1:3].without_duplicates().sorted(terms).with_only_columns({b, c, d})),
            engine.to_executable(r[1:3].without_duplicates().with_only_columns({b, c, d}).sorted(terms)),
        )

    def test_additional_append_unary(self) -> None:
        """Test append_unary rules that involve more than just the
        Select-managed operation types.
        """
        engine = sql.Engine[_L]()
        md = sqlalchemy.schema.MetaData()
        a = tests.ColumnTag("a")
        b = tests.ColumnTag("b")
        c = tests.ColumnTag("c")
        d = tests.ColumnTag("d")
        expression = ColumnExpression.reference(b).method("__neg__")
        predicate = ColumnExpression.reference(c).gt(ColumnExpression.literal(0))
        leaf1 = _make_leaf(engine, md, "leaf1", a, b)
        leaf2 = _make_leaf(engine, md, "leaf2", a, c)
        # Applying a Calculation to a Projection expands the latter and
        # commutes them.
        self.assert_relations_equal(
            leaf1.with_only_columns({b}).with_calculated_column(d, expression),
            leaf1.with_calculated_column(d, expression).with_only_columns({b, d}),
        )
        # Back-to-back Deduplications reduce to one.
        self.assert_relations_equal(
            leaf1.without_duplicates().without_duplicates(),
            leaf1.without_duplicates(),
        )
        # Selections after Slices involve a subquery.
        self.check_sql_str(
            "SELECT anon_1.a AS a, anon_1.c AS c FROM ("
            "SELECT leaf2.a AS a, leaf2.c AS c FROM leaf2 LIMIT 2 OFFSET 1"
            ") AS anon_1 WHERE anon_1.c > 0",
            engine.to_executable(leaf2[1:3].with_rows_satisfying(predicate)),
        )
        # Identity does nothing.
        self.assert_relations_equal(Identity().apply(leaf1), leaf1)

    def test_additional_append_binary(self) -> None:
        """Test append_binary rules that involve more than just the
        Select-managed operation types.
        """
        engine = sql.Engine[_L]()
        md = sqlalchemy.schema.MetaData()
        a = tests.ColumnTag("a")
        b = tests.ColumnTag("b")
        c = tests.ColumnTag("c")
        leaf1 = _make_leaf(engine, md, "leaf1", a, b)
        leaf2 = _make_leaf(engine, md, "leaf2", a, c)
        # Projections are moved outside joins.
        self.assert_relations_equal(
            leaf1.join(leaf2.with_only_columns({a})),
            leaf1.join(leaf2).with_only_columns({a, b}),
        )
        # IgnoreOne does what it should.
        self.assert_relations_equal(IgnoreOne(ignore_lhs=True).apply(leaf1, leaf2), leaf2)
        self.assert_relations_equal(IgnoreOne(ignore_lhs=False).apply(leaf1, leaf2), leaf1)

    def test_chains(self) -> None:
        """Test relation trees that involve Chain operations."""
        engine = sql.Engine[_L]()
        md = sqlalchemy.schema.MetaData()
        a = tests.ColumnTag("a")
        b = tests.ColumnTag("b")
        leaf1 = _make_leaf(engine, md, "leaf1", a, b)
        leaf2 = _make_leaf(engine, md, "leaf2", a, b)
        sort_terms = [SortTerm(ColumnExpression.reference(b))]
        # A Chain on its own maps directly to SQL UNION.
        self.check_sql_str(
            "SELECT leaf1.a AS a, leaf1.b AS b FROM leaf1 "
            "UNION ALL "
            "SELECT leaf2.a AS a, leaf2.b AS b FROM leaf2",
            engine.to_executable(leaf1.chain(leaf2)),
        )
        # Deduplication transforms this to UNION ALL.
        self.check_sql_str(
            "SELECT leaf1.a AS a, leaf1.b AS b FROM leaf1 "
            "UNION "
            "SELECT leaf2.a AS a, leaf2.b AS b FROM leaf2",
            engine.to_executable(leaf1.chain(leaf2).without_duplicates()),
        )
        # Sorting happens after the second SELECT.
        self.check_sql_str(
            "SELECT leaf1.a AS a, leaf1.b AS b FROM leaf1 "
            "UNION ALL "
            "SELECT leaf2.a AS a, leaf2.b AS b FROM leaf2 ORDER BY b",
            engine.to_executable(leaf1.chain(leaf2).sorted(sort_terms)),
        )
        # Slicing also happens after the second SELECT.
        self.check_sql_str(
            "SELECT leaf1.a AS a, leaf1.b AS b FROM leaf1 "
            "UNION ALL "
            "SELECT leaf2.a AS a, leaf2.b AS b FROM leaf2 LIMIT 3 OFFSET 2",
            engine.to_executable(leaf1.chain(leaf2)[2:5]),
        )
        # Projection just before the Chain is handled without any extra
        # subqueries.
        self.check_sql_str(
            "SELECT leaf1.a AS a FROM leaf1 UNION ALL SELECT leaf2.a AS a FROM leaf2",
            engine.to_executable(leaf1.with_only_columns({a}).chain(leaf2.with_only_columns({a}))),
        )
        # Deduplication prior to Chain adds DISTINCT.
        self.check_sql_str(
            "SELECT DISTINCT leaf1.a AS a, leaf1.b AS b FROM leaf1 "
            "UNION ALL "
            "SELECT leaf2.a AS a, leaf2.b AS b FROM leaf2",
            engine.to_executable(leaf1.without_duplicates().chain(leaf2)),
        )
        # Projection and Deduplication prior to Chain also just adds DISTINCT.
        self.check_sql_str(
            "SELECT DISTINCT leaf1.a AS a FROM leaf1 UNION ALL SELECT leaf2.a AS a FROM leaf2",
            engine.to_executable(
                leaf1.with_only_columns({a}).without_duplicates().chain(leaf2.with_only_columns({a}))
            ),
        )
        # Nested chains should flatten out (again, no subqueries), but use
        # parentheses for precedence.
        leaf3 = _make_leaf(engine, md, "leaf3", a, b)
        self.check_sql_str(
            "(SELECT leaf1.a AS a, leaf1.b AS b FROM leaf1 "
            "UNION ALL "
            "SELECT leaf2.a AS a, leaf2.b AS b FROM leaf2) "
            "UNION ALL "
            "SELECT leaf3.a AS a, leaf3.b AS b FROM leaf3",
            engine.to_executable(leaf1.chain(leaf2).chain(leaf3)),
        )
        # Nested chains with deduplication should do the same.
        self.check_sql_str(
            "(SELECT leaf1.a AS a, leaf1.b AS b FROM leaf1 "
            "UNION ALL "
            "SELECT leaf2.a AS a, leaf2.b AS b FROM leaf2) "
            "UNION "
            "SELECT leaf3.a AS a, leaf3.b AS b FROM leaf3",
            engine.to_executable(leaf1.chain(leaf2).chain(leaf3).without_duplicates()),
        )
        self.check_sql_str(
            "(SELECT leaf1.a AS a, leaf1.b AS b FROM leaf1 "
            "UNION "
            "SELECT leaf2.a AS a, leaf2.b AS b FROM leaf2) "
            "UNION "
            "SELECT leaf3.a AS a, leaf3.b AS b FROM leaf3",
            engine.to_executable(leaf1.chain(leaf2).without_duplicates().chain(leaf3).without_duplicates()),
        )
        # Nested chains with projections.
        self.check_sql_str(
            "(SELECT leaf1.a AS a FROM leaf1 "
            "UNION ALL "
            "SELECT leaf2.a AS a FROM leaf2) "
            "UNION ALL "
            "SELECT leaf3.a AS a FROM leaf3",
            engine.to_executable(
                leaf1.chain(leaf2).with_only_columns({a}).chain(leaf3.with_only_columns({a}))
            ),
        )
        # Nested chains with projections and (then) deduplication.
        self.check_sql_str(
            "(SELECT leaf1.a AS a FROM leaf1 "
            "UNION "
            "SELECT leaf2.a AS a FROM leaf2) "
            "UNION "
            "SELECT leaf3.a AS a FROM leaf3",
            engine.to_executable(
                leaf1.chain(leaf2)
                .with_only_columns({a})
                .without_duplicates()
                .chain(leaf3.with_only_columns({a}))
                .without_duplicates()
            ),
        )
        # Chains with Slices and possibly Sorts in operands need subqueries.
        self.check_sql_str(
            "SELECT anon_1.a AS a, anon_1.b AS b FROM "
            "(SELECT leaf1.a AS a, leaf1.b AS b FROM leaf1 LIMIT 2 OFFSET 1) AS anon_1 "
            "UNION ALL "
            "SELECT leaf2.a AS a, leaf2.b AS b FROM leaf2",
            engine.to_executable(leaf1[1:3].chain(leaf2)),
        )
        self.check_sql_str(
            "SELECT leaf1.a AS a, leaf1.b AS b FROM leaf1 "
            "UNION ALL "
            "SELECT anon_1.a AS a, anon_1.b AS b FROM "
            "(SELECT leaf2.a AS a, leaf2.b AS b FROM leaf2 LIMIT 2 OFFSET 1) AS anon_1",
            engine.to_executable(leaf1.chain(leaf2[1:3])),
        )
        # Add a Selection or Calculation on top of a Chain yields subqueries
        # to avoid reordering operations.
        expression = ColumnExpression.reference(b).method("__neg__")
        self.check_sql_str(
            "SELECT anon_1.a AS a, anon_1.b AS b, -anon_1.b AS c FROM "
            "(SELECT leaf1.a AS a, leaf1.b AS b FROM leaf1 "
            "UNION ALL "
            "SELECT leaf2.a AS a, leaf2.b AS b FROM leaf2) AS anon_1",
            engine.to_executable(leaf1.chain(leaf2).with_calculated_column(tests.ColumnTag("c"), expression)),
        )
        predicate = ColumnExpression.reference(a).gt(ColumnExpression.literal(0))
        self.check_sql_str(
            "SELECT anon_1.a AS a, anon_1.b AS b FROM "
            "(SELECT leaf1.a AS a, leaf1.b AS b FROM leaf1 "
            "UNION ALL "
            "SELECT leaf2.a AS a, leaf2.b AS b FROM leaf2) AS anon_1 "
            "WHERE anon_1.a > 0",
            engine.to_executable(leaf1.chain(leaf2).with_rows_satisfying(predicate)),
        )

    def test_row_ordering_loss(self) -> None:
        """Test that we raise when we would have to make an existing Sort do
        nothing by putting it in a subquery.
        """
        engine = sql.Engine[_L]()
        md = sqlalchemy.schema.MetaData()
        a = tests.ColumnTag("a")
        b = tests.ColumnTag("b")
        leaf1 = _make_leaf(engine, md, "leaf1", a, b)
        leaf2 = _make_leaf(engine, md, "leaf2", a, b)
        relation = leaf1.sorted([SortTerm(ColumnExpression.reference(b))])
        with self.assertRaises(RelationalAlgebraError):
            relation.materialized()
        with self.assertRaises(RelationalAlgebraError):
            relation.chain(leaf2)
        with self.assertRaises(RelationalAlgebraError):
            leaf2.chain(relation)
        with self.assertRaises(RelationalAlgebraError):
            relation.join(leaf2)
        with self.assertRaises(RelationalAlgebraError):
            leaf2.join(relation)

    def test_trivial(self) -> None:
        """Test that we can emit valid SQL for relations with no columns or
        no rows.
        """
        # No points for pretty; subqueries here are unnecessary but
        # Payload.from_clause would need to be able to be None to drop them,
        # and that's not worth it.
        engine = sql.Engine[_L]()
        join_identity = engine.make_join_identity_relation()
        self.check_sql_str(
            'SELECT 1 AS "IGNORED" FROM (SELECT 1 AS "IGNORED") AS anon_1',
            engine.to_executable(join_identity),
        )
        doomed = engine.make_doomed_relation({tests.ColumnTag("a")}, messages=[])
        self.check_sql_str(
            "SELECT anon_1.a AS a FROM (SELECT NULL AS a) AS anon_1 WHERE 0 = 1",
            engine.to_executable(doomed),
        )


if __name__ == "__main__":
    unittest.main()
