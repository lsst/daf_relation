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
from collections.abc import Mapping

import sqlalchemy
from lsst.daf.relation import (
    ColumnContainer,
    ColumnExpression,
    ColumnFunction,
    ColumnTag,
    LogicalAnd,
    LogicalOr,
    Predicate,
    flatten_logical_and,
    iteration,
    sql,
    tests,
)


class ColumnExpressionTestCase(tests.RelationTestCase):
    def test_operator_function(self) -> None:
        """Test ColumnFunction via its constructor and a name found in the
        `operator` module.
        """
        tag = tests.ColumnTag("tag")
        expression = ColumnFunction(
            "__sub__",
            (ColumnExpression.reference(tag), ColumnExpression.literal(5)),
            dtype=int,
            supporting_engine_types=None,
        )
        self.assertEqual(expression.dtype, int)
        self.assertEqual(expression.supporting_engine_types, None)
        self.assertEqual(expression.args, (ColumnExpression.reference(tag), ColumnExpression.literal(5)))
        self.assertEqual(expression.name, "__sub__")
        self.assertEqual(expression.columns_required, {tag})
        self.assertEqual(
            str(expression),
            "__sub__(tag, 5)",
        )
        iteration_engine = iteration.Engine()
        self.assertTrue(expression.is_supported_by(iteration_engine))
        callable = iteration_engine.convert_column_expression(expression)
        self.assertEqual(callable({tag: 3}), -2)
        self.assertEqual(callable({tag: 6}), 1)
        sql_engine = sql.Engine[sqlalchemy.sql.ColumnElement]()
        self.assertTrue(expression.is_supported_by(sql_engine))
        sql_expression = sql_engine.convert_column_expression(
            expression, {tag: sqlalchemy.schema.Column("tag")}
        )
        self.check_sql_str("tag - 5", sql_expression)

    def test_method(self) -> None:
        """Test ColumnFunction via ColumnExpression.method and a name found
        on the object itself.
        """
        tag = tests.ColumnTag("tag")
        expression = ColumnExpression.reference(tag).method("lower")
        self.assertEqual(expression.dtype, None)
        self.assertEqual(expression.supporting_engine_types, None)
        self.assertEqual(expression.args, (ColumnExpression.reference(tag),))
        self.assertEqual(expression.name, "lower")
        self.assertEqual(expression.columns_required, {tag})
        engine = iteration.Engine()
        self.assertTrue(expression.is_supported_by(engine))
        callable = engine.convert_column_expression(expression)
        self.assertEqual(callable({tag: "MiXeDcAsE"}), "mixedcase")

    def test_engine_function(self) -> None:
        """Test ColumnFunction via ColumnExpression.function and a name
        that references a callable held by the engine.
        """
        tag = tests.ColumnTag("tag")
        engine = iteration.Engine()
        engine.functions["test_function"] = lambda x: x**2
        expression = ColumnExpression.function(
            "test_function",
            ColumnExpression.reference(tag),
            supporting_engine_types={iteration.Engine},
            dtype=int,
        )
        self.assertEqual(expression.dtype, int)
        self.assertEqual(expression.supporting_engine_types, (iteration.Engine,))
        self.assertEqual(expression.args, (ColumnExpression.reference(tag),))
        self.assertEqual(expression.name, "test_function")
        self.assertEqual(expression.columns_required, {tag})
        self.assertTrue(expression.is_supported_by(engine))
        self.assertFalse(expression.is_supported_by(sql.Engine[sqlalchemy.sql.ColumnElement]()))
        callable = engine.convert_column_expression(expression)
        self.assertEqual(callable({tag: 3}), 9)
        self.assertEqual(callable({tag: 4}), 16)

    def test_operator_predicate_function(self) -> None:
        """Test PredicateFunction via ColumnExpression factory methods and
        names found in the `operator` module.
        """
        tag = tests.ColumnTag("tag")
        ref = ColumnExpression.reference(tag, dtype=int)
        zero = ColumnExpression.literal(0, dtype=int)
        expressions = [ref.eq(zero), ref.ne(zero), ref.lt(zero), ref.le(zero), ref.gt(zero), ref.ge(zero)]
        self.assertEqual([x.dtype for x in expressions], [bool] * 6)
        self.assertEqual([x.supporting_engine_types for x in expressions], [None] * 6)
        self.assertEqual([x.args for x in expressions], [(ref, zero)] * 6)
        self.assertEqual(
            [x.name for x in expressions], ["__eq__", "__ne__", "__lt__", "__le__", "__gt__", "__ge__"]
        )
        self.assertEqual([x.columns_required for x in expressions], [{tag}] * 6)
        self.assertEqual(
            [str(x) for x in expressions],
            ["tag=0", "tag≠0", "tag<0", "tag≤0", "tag>0", "tag≥0"],
        )
        iteration_engine = iteration.Engine()
        self.assertEqual([x.is_supported_by(iteration_engine) for x in expressions], [True] * 6)
        callables = [iteration_engine.convert_predicate(x) for x in expressions]
        self.assertEqual([c({tag: 0}) for c in callables], [True, False, False, True, False, True])
        self.assertEqual([c({tag: 1}) for c in callables], [False, True, False, False, True, True])
        self.assertEqual([c({tag: -1}) for c in callables], [False, True, True, True, False, False])
        sql_bind = sqlalchemy.schema.Column("tag")
        sql_engine = sql.Engine[sqlalchemy.sql.ColumnElement]()
        self.assertEqual([x.is_supported_by(sql_engine) for x in expressions], [True] * 6)
        sql_expressions: list[sqlalchemy.sql.ColumnElement] = [
            sql_engine.convert_predicate(x, {tag: sql_bind}) for x in expressions
        ]
        self.assertEqual(
            [tests.to_sql_str(s) for s in sql_expressions],
            [
                "tag = 0",
                "tag != 0",
                "tag < 0",
                "tag <= 0",
                "tag > 0",
                "tag >= 0",
            ],
        )

    def test_column_expression_sequence(self) -> None:
        """Test ColumnExpressionSequence and ColumnInContainer."""
        a = tests.ColumnTag("a")
        seq = ColumnContainer.sequence(
            [ColumnExpression.reference(a, dtype=int), ColumnExpression.literal(4, dtype=int)], dtype=int
        )
        self.assertEqual(seq.dtype, int)
        self.assertEqual(
            list(seq.items),
            [ColumnExpression.reference(a, dtype=int), ColumnExpression.literal(4, dtype=int)],
        )
        self.assertEqual(seq.columns_required, {a})
        self.assertEqual(str(seq), "[a, 4]")
        iteration_engine = iteration.Engine()
        sql_engine = sql.Engine[sqlalchemy.sql.ColumnElement]()
        self.assertTrue(seq.is_supported_by(iteration_engine))
        self.assertTrue(seq.is_supported_by(sql_engine))
        b = tests.ColumnTag("b")
        contains = seq.contains(ColumnExpression.reference(b))
        self.assertEqual(contains.dtype, bool)
        self.assertEqual(contains.columns_required, {a, b})
        self.assertEqual(str(contains), "b∈[a, 4]")
        self.assertTrue(contains.is_supported_by(iteration_engine))
        self.assertTrue(contains.is_supported_by(sql_engine))
        callable = iteration_engine.convert_predicate(contains)
        self.assertEqual(callable({a: 0, b: 0}), True)
        self.assertEqual(callable({a: 0, b: 3}), False)
        self.assertEqual(callable({a: 0, b: 4}), True)
        bind_a = sqlalchemy.schema.Column("a")
        bind_b = sqlalchemy.schema.Column("b")
        self.check_sql_str("b IN (a, 4)", sql_engine.convert_predicate(contains, {a: bind_a, b: bind_b}))

    def test_range_literal(self) -> None:
        """Test ColumnRangeLiteral and ColumnInContainer."""
        ranges = [
            ColumnContainer.range_literal(range(3, 4)),
            ColumnContainer.range_literal(range(3, 6)),
            ColumnContainer.range_literal(range(2, 11, 3)),
        ]
        self.assertEqual([r.dtype for r in ranges], [int] * 3)
        self.assertEqual([r.columns_required for r in ranges], [frozenset()] * 3)
        self.assertEqual([str(r) for r in ranges], ["[3:4:1]", "[3:6:1]", "[2:11:3]"])
        a = tests.ColumnTag("a")
        contains = [r.contains(ColumnExpression.reference(a)) for r in ranges]
        self.assertEqual([c.dtype for c in contains], [bool] * 3)
        self.assertEqual([c.columns_required for c in contains], [{a}] * 3)
        self.assertEqual([str(c) for c in contains], ["a∈[3:4:1]", "a∈[3:6:1]", "a∈[2:11:3]"])
        iteration_engine = iteration.Engine()
        self.assertEqual([c.is_supported_by(iteration_engine) for c in contains], [True] * 3)
        callables = [iteration_engine.convert_predicate(c) for c in contains]
        self.assertEqual([c({a: 3}) for c in callables], [True, True, False])
        self.assertEqual([c({a: 5}) for c in callables], [False, True, True])
        self.assertEqual([c({a: 8}) for c in callables], [False, False, True])
        sql_engine = sql.Engine[sqlalchemy.sql.ColumnElement]()
        self.assertEqual([c.is_supported_by(sql_engine) for c in contains], [True] * 3)
        bind_a = sqlalchemy.schema.Column("a")
        sql_expressions = [sql_engine.convert_predicate(c, {a: bind_a}) for c in contains]
        self.assertEqual(
            [tests.to_sql_str(s) for s in sql_expressions],
            ["a = 3", "a BETWEEN 3 AND 5", "a BETWEEN 2 AND 10 AND a % 3 = 2"],
        )

    def test_logical_operators(self) -> None:
        """Test predicate logical operator expressions, Predicate.as_literal,
        and flatten_logical_and.
        """
        a = tests.ColumnTag("a")
        b = tests.ColumnTag("b")
        t = Predicate.literal(True)
        f = Predicate.literal(False)
        x = ColumnExpression.reference(a).gt(ColumnExpression.literal(0))
        y = Predicate.reference(b)
        iteration_engine = iteration.Engine()
        sql_engine = sql.Engine[sqlalchemy.sql.ColumnElement]()
        # Check attributes and simple accessors for predicate literals and
        # references.  Some as_trivial overloads are not checked because MyPy
        # can tell they always return None and complains if we try to use that
        # return value (which also means MyPy takes care of that "test" for us,
        # even though coverage can't tell).
        self.assertEqual(t.columns_required, frozenset())
        self.assertEqual(f.columns_required, frozenset())
        self.assertEqual(y.columns_required, {b})
        self.assertEqual(str(t), "True")
        self.assertEqual(str(f), "False")
        self.assertEqual(str(y), "b")
        self.assertIs(t.as_trivial(), True)
        self.assertIs(f.as_trivial(), False)
        self.assertTrue(t.is_supported_by(iteration_engine))
        self.assertTrue(f.is_supported_by(iteration_engine))
        self.assertTrue(y.is_supported_by(iteration_engine))
        self.assertTrue(t.is_supported_by(sql_engine))
        self.assertTrue(f.is_supported_by(sql_engine))
        self.assertTrue(y.is_supported_by(sql_engine))
        # Test factory methods for logical operators, including simplification.
        self.assertIs(t.logical_not().as_trivial(), False)
        self.assertIs(f.logical_not().as_trivial(), True)
        self.assertIs(t.logical_and(x).as_trivial(), None)
        self.assertIs(f.logical_and(x).as_trivial(), False)
        self.assertIs(t.logical_or(x).as_trivial(), True)
        self.assertIs(f.logical_or(x).as_trivial(), None)
        self.assertEqual(Predicate.logical_and(), t)
        self.assertEqual(Predicate.logical_or(), f)
        self.assertEqual(Predicate.logical_and(x), x)
        self.assertEqual(Predicate.logical_or(x), x)
        # Test attributes and simple accessors for logical operators.
        not_x = x.logical_not()
        self.assertEqual(not_x.columns_required, {a})
        self.assertEqual(str(not_x), "not (a>0)")
        self.assertIs(not_x.as_trivial(), None)
        self.assertTrue(not_x.is_supported_by(iteration_engine))
        self.assertTrue(not_x.is_supported_by(sql_engine))
        x_and_y = x.logical_and(y)
        self.assertEqual(x_and_y.columns_required, {a, b})
        self.assertEqual(str(x_and_y), "a>0 and b")
        self.assertIs(x_and_y.as_trivial(), None)
        self.assertTrue(x_and_y.is_supported_by(iteration_engine))
        self.assertTrue(x_and_y.is_supported_by(sql_engine))
        x_or_y = x.logical_or(y)
        self.assertEqual(x_or_y.columns_required, {a, b})
        self.assertEqual(str(x_or_y), "a>0 or b")
        self.assertIs(x_or_y.as_trivial(), None)
        self.assertTrue(x_or_y.is_supported_by(iteration_engine))
        self.assertTrue(x_or_y.is_supported_by(sql_engine))
        # Test iteration engine conversions.
        self.assertEqual(iteration_engine.convert_predicate(t)({}), True)
        self.assertEqual(iteration_engine.convert_predicate(f)({}), False)
        self.assertEqual(iteration_engine.convert_predicate(y)({b: True}), True)
        self.assertEqual(iteration_engine.convert_predicate(y)({b: False}), False)
        self.assertEqual(iteration_engine.convert_predicate(not_x)({a: 1}), False)
        self.assertEqual(iteration_engine.convert_predicate(not_x)({a: 0}), True)
        self.assertEqual(iteration_engine.convert_predicate(x_and_y)({a: 1, b: True}), True)
        self.assertEqual(iteration_engine.convert_predicate(x_and_y)({a: 1, b: False}), False)
        self.assertEqual(iteration_engine.convert_predicate(x_and_y)({a: 0, b: True}), False)
        self.assertEqual(iteration_engine.convert_predicate(x_and_y)({a: 0, b: False}), False)
        self.assertEqual(iteration_engine.convert_predicate(x_or_y)({a: 1, b: True}), True)
        self.assertEqual(iteration_engine.convert_predicate(x_or_y)({a: 1, b: False}), True)
        self.assertEqual(iteration_engine.convert_predicate(x_or_y)({a: 0, b: True}), True)
        self.assertEqual(iteration_engine.convert_predicate(x_or_y)({a: 0, b: False}), False)
        # Test SQL engine conversions.
        columns: Mapping[ColumnTag, sqlalchemy.sql.ColumnElement] = {
            a: sqlalchemy.schema.Column("a"),
            b: sqlalchemy.schema.Column("b"),
        }
        self.check_sql_str(
            "1",
            sql_engine.convert_predicate(t, columns),
            sql_engine.convert_predicate(LogicalAnd(()), columns),
        )
        self.check_sql_str(
            "0",
            sql_engine.convert_predicate(f, columns),
            sql_engine.convert_predicate(LogicalOr(()), columns),
        )
        self.check_sql_str(
            "b",
            sql_engine.convert_predicate(y, columns),
            sql_engine.convert_predicate(LogicalAnd((y,)), columns),
            sql_engine.convert_predicate(LogicalOr((y,)), columns),
        )
        # Apparently SQLAlchemy does some simplifications of its own on NOT
        # operations.
        self.check_sql_str("a <= 0", sql_engine.convert_predicate(not_x, columns))
        self.check_sql_str("a > 0 AND b", sql_engine.convert_predicate(x_and_y, columns))
        self.check_sql_str("a > 0 OR b", sql_engine.convert_predicate(x_or_y, columns))
        # Test flatten_logical_and
        self.assertEqual(flatten_logical_and(t), [])
        self.assertIs(flatten_logical_and(f), False)
        self.assertEqual(flatten_logical_and(not_x), [not_x])
        self.assertEqual(flatten_logical_and(x_and_y), [x, y])
        self.assertEqual(flatten_logical_and(x_or_y), [x_or_y])
        self.assertEqual(flatten_logical_and(x_and_y.logical_and(t)), [x, y])
        self.assertEqual(flatten_logical_and(x_and_y.logical_and(f)), False)
        c = tests.ColumnTag("c")
        z = Predicate.reference(c)
        self.assertEqual(flatten_logical_and(x_and_y.logical_and(z)), [x, y, z])


if __name__ == "__main__":
    unittest.main()
