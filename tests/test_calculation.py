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

from lsst.daf.relation import (
    Calculation,
    ColumnError,
    ColumnExpression,
    EngineError,
    SortTerm,
    UnaryOperationRelation,
    iteration,
    tests,
)


class CalculationTestCase(tests.RelationTestCase):
    """Tests for the Calculation operation and relations based on it."""

    def setUp(self) -> None:
        self.a = tests.ColumnTag("a")
        self.b = tests.ColumnTag("b")
        self.expression = ColumnExpression.function(
            "__add__",
            ColumnExpression.reference(self.a),
            ColumnExpression.literal(10),
        )
        self.engine = iteration.Engine(name="preferred")
        self.leaf = self.engine.make_leaf(
            {self.a}, payload=iteration.RowSequence([{self.a: 0}, {self.a: 1}]), name="leaf"
        )

    def test_attributes(self) -> None:
        """Check that all UnaryOperation and Relation attributes have the
        expected values.
        """
        relation = self.leaf.with_calculated_column(self.b, self.expression)
        assert isinstance(relation, UnaryOperationRelation)
        self.assertEqual(relation.columns, {self.a, self.b})
        self.assertEqual(relation.engine, self.engine)
        self.assertEqual(relation.min_rows, self.leaf.min_rows)
        self.assertEqual(relation.max_rows, self.leaf.max_rows)
        operation = relation.operation
        assert isinstance(operation, Calculation)
        self.assertEqual(operation.expression, self.expression)
        self.assertEqual(operation.columns_required, {self.a})
        self.assertTrue(operation.is_empty_invariant)
        self.assertTrue(operation.is_count_invariant)
        self.assertFalse(operation.is_order_dependent)
        self.assertFalse(operation.is_count_dependent)

    def test_apply_failures(self) -> None:
        """Test failure modes of constructing and applying Calculations."""
        # Calculations must depend on existing columns.
        with self.assertRaises(ColumnError):
            self.leaf.with_calculated_column(self.b, ColumnExpression.literal(10))
        # Required columns must be present.
        with self.assertRaises(ColumnError):
            self.leaf.with_calculated_column(self.b, ColumnExpression.reference(tests.ColumnTag("c")))
        # New column must not already be present.
        with self.assertRaises(ColumnError):
            self.leaf.with_calculated_column(self.a, self.expression)

    def test_backtracking_apply(self) -> None:
        """Test apply logic that involves reordering operations in the existing
        tree to perform the new operation in a preferred engine.
        """
        new_engine = iteration.Engine(name="downstream")
        predicate = ColumnExpression.reference(self.b).lt(ColumnExpression.literal(20))
        # Apply a bunch of operations in a new engine that a Calculation should
        # commute with.
        target = (
            self.leaf.transferred_to(new_engine)
            .with_calculated_column(self.b, self.expression)
            .with_rows_satisfying(predicate)
            .without_duplicates()
            .with_only_columns({self.a})
            .sorted([SortTerm(ColumnExpression.reference(self.a))])
        )[:2]
        # Apply a new Calculation with backtracking and see that it appears
        # before the transfer to the new engine, with adjustments as needed
        # downstream (to the Projection and Chain, in this case).
        tag = tests.ColumnTag("tag")
        relation = target.with_calculated_column(
            tag, self.expression, preferred_engine=self.engine, require_preferred_engine=True
        )
        self.assert_relations_equal(
            relation,
            (
                self.leaf.with_calculated_column(tag, self.expression)
                .transferred_to(new_engine)
                .with_calculated_column(self.b, self.expression)
                .with_rows_satisfying(predicate)
                .without_duplicates()
                .with_only_columns({self.a, tag})
                .sorted([SortTerm(ColumnExpression.reference(self.a))])
            )[:2],
        )

    def test_no_backtracking(self) -> None:
        """Test apply logic that handles preferred engines without reordering
        operations in the existing tree.
        """
        new_engine = iteration.Engine(name="downstream")
        # Construct a relation tree we can't reorder when inserting a
        # Calculation, for various reasons.
        leaf = self.engine.make_leaf(
            {self.a},
            payload=iteration.RowSequence([{self.a: 100}, {self.a: 200}]),
            name="leaf",
        )
        # Can't insert after leaf because Materialization is locked.
        target = leaf.transferred_to(new_engine).materialized("lock")
        # Preferred engine is ignored if we can't backtrack and don't enable
        # anything else.
        self.assert_relations_equal(
            target.with_calculated_column(self.b, self.expression, preferred_engine=self.engine),
            target.with_calculated_column(self.b, self.expression),
        )
        # We can force this to be an error.
        with self.assertRaises(EngineError):
            target.with_calculated_column(
                self.b, self.expression, preferred_engine=self.engine, require_preferred_engine=True
            )
        # We can also automatically transfer (back) to the preferred engine.
        self.assert_relations_equal(
            target.with_calculated_column(
                self.b, self.expression, preferred_engine=self.engine, transfer=True
            ),
            target.transferred_to(self.engine).with_calculated_column(self.b, self.expression),
        )
        # Can't commute past an existing Calculation that provides a required
        # column.
        target = leaf.transferred_to(new_engine).with_calculated_column(self.b, self.expression)
        with self.assertRaises(EngineError):
            target.with_calculated_column(
                tests.ColumnTag("c"),
                ColumnExpression.reference(self.b),
                preferred_engine=self.engine,
                require_preferred_engine=True,
            )

    def test_iteration(self) -> None:
        """Test Calculation execution in the iteration engine."""
        relation = self.leaf.with_calculated_column(self.b, self.expression)
        self.assertEqual(
            list(self.engine.execute(relation)),
            [{self.a: 0, self.b: 10}, {self.a: 1, self.b: 11}],
        )

    def test_str(self) -> None:
        """Test str(Calculation) and
        str(UnaryOperationRelation[Calculation]).
        """
        relation = self.leaf.with_calculated_column(self.b, self.expression)
        self.assertEqual(str(relation), "+[b=__add__(a, 10)](leaf)")


if __name__ == "__main__":
    unittest.main()
