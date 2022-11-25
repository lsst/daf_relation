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
    ColumnError,
    ColumnExpression,
    EngineError,
    Predicate,
    Selection,
    SortTerm,
    UnaryOperationRelation,
    iteration,
    tests,
)


class SelectionTestCase(tests.RelationTestCase):
    """Tests for the Selection operation and relations based on it."""

    def setUp(self) -> None:
        self.a = tests.ColumnTag("a")
        self.predicate = ColumnExpression.reference(self.a).gt(ColumnExpression.literal(0))
        self.engine = iteration.Engine(name="preferred")
        self.leaf = self.engine.make_leaf(
            {self.a}, payload=iteration.RowSequence([{self.a: 0}, {self.a: 1}]), name="leaf"
        )

    def test_attributes(self) -> None:
        """Check that all UnaryOperation and Relation attributes have the
        expected values.
        """
        relation = self.leaf.with_rows_satisfying(self.predicate)
        assert isinstance(relation, UnaryOperationRelation)
        self.assertEqual(relation.columns, {self.a})
        self.assertEqual(relation.engine, self.engine)
        self.assertEqual(relation.min_rows, 0)
        self.assertEqual(relation.max_rows, self.leaf.max_rows)
        operation = relation.operation
        assert isinstance(operation, Selection)
        self.assertEqual(operation.predicate, self.predicate)
        self.assertEqual(operation.columns_required, {self.a})
        self.assertFalse(operation.is_empty_invariant)
        self.assertFalse(operation.is_count_invariant)
        self.assertFalse(operation.is_order_dependent)
        self.assertFalse(operation.is_count_dependent)

    def test_apply_failures(self) -> None:
        """Test failure modes of constructing and applying Selections."""
        # Required columns must be present.
        with self.assertRaises(ColumnError):
            self.leaf.with_rows_satisfying(
                ColumnExpression.reference(tests.ColumnTag("c")).lt(ColumnExpression.literal(0))
            )

    def test_backtracking_apply(self) -> None:
        """Test apply logic that involves reordering operations in the existing
        tree to perform the new operation in a preferred engine.
        """
        new_engine = iteration.Engine(name="downstream")
        b = tests.ColumnTag("b")
        expression = ColumnExpression.function(
            "__add__", ColumnExpression.reference(self.a), ColumnExpression.literal(5)
        )
        sort_terms = [SortTerm(ColumnExpression.reference(self.a))]
        other_predicate = ColumnExpression.reference(b).gt(ColumnExpression.literal(0))
        # Apply a bunch of operations in a new engine that a Selection should
        # commute with.
        target = (
            self.leaf.transferred_to(new_engine)
            .with_calculated_column(b, expression)
            .with_rows_satisfying(other_predicate)
            .without_duplicates()
            .with_only_columns({self.a})
            .sorted(sort_terms)
        )
        # Apply a new Selection with backtracking and see that it appears
        # before the transfer to the new engine, with adjustments as needed.
        relation = target.with_rows_satisfying(
            self.predicate, preferred_engine=self.engine, require_preferred_engine=True
        )
        self.assert_relations_equal(
            relation,
            (
                self.leaf.with_rows_satisfying(self.predicate)
                .transferred_to(new_engine)
                .with_calculated_column(b, expression)
                .with_rows_satisfying(other_predicate)
                .without_duplicates()
                .with_only_columns({self.a})
                .sorted(sort_terms)
            ),
        )

    def test_no_backtracking(self) -> None:
        """Test apply logic that handles preferred engines without reordering
        operations in the existing tree.
        """
        new_engine = iteration.Engine(name="downstream")
        # Construct a relation tree we can't reorder when inserting a
        # Selection, because there is a locked Materialization in the way.
        target = self.leaf.transferred_to(new_engine).materialized("lock")
        # Preferred engine is ignored if we can't backtrack and don't enable
        # anything else.
        self.assert_relations_equal(
            target.with_rows_satisfying(self.predicate, preferred_engine=self.engine),
            target.with_rows_satisfying(self.predicate),
        )
        # We can force this to be an error.
        with self.assertRaises(EngineError):
            target.with_rows_satisfying(
                self.predicate, preferred_engine=self.engine, require_preferred_engine=True
            )
        # We can also automatically transfer (back) to the preferred engine.
        self.assert_relations_equal(
            target.with_rows_satisfying(self.predicate, preferred_engine=self.engine, transfer=True),
            target.transferred_to(self.engine).with_rows_satisfying(self.predicate),
        )
        # Can't backtrack through a Calculation that provides required columns.
        # In the future, we could make this possible by subsuming the
        # calculated columns into the predicate.
        b = tests.ColumnTag("b")
        target = self.leaf.transferred_to(new_engine).with_calculated_column(
            b, ColumnExpression.reference(self.a)
        )
        with self.assertRaises(EngineError):
            target.with_rows_satisfying(
                ColumnExpression.reference(b).gt(ColumnExpression.literal(0)),
                preferred_engine=self.engine,
                require_preferred_engine=True,
            )
        # Can't backtrack through a slice.
        target = self.leaf.transferred_to(new_engine)[1:]
        with self.assertRaises(EngineError):
            target.with_rows_satisfying(
                self.predicate,
                preferred_engine=self.engine,
                require_preferred_engine=True,
            )

    def test_apply_simplify(self) -> None:
        """Test simplification logic in Selection.apply."""
        self.assertEqual(self.leaf.with_rows_satisfying(Predicate.literal(True)), self.leaf)
        new_predicate = ColumnExpression.reference(self.a).lt(ColumnExpression.literal(5))
        self.assertEqual(
            self.leaf.with_rows_satisfying(self.predicate).with_rows_satisfying(new_predicate),
            self.leaf.with_rows_satisfying(self.predicate.logical_and(new_predicate)),
        )

    def test_iteration(self) -> None:
        """Test Selection execution in the iteration engine."""
        relation = self.leaf.with_rows_satisfying(self.predicate)
        self.assertEqual(list(self.engine.execute(relation)), [{self.a: 1}])

    def test_str(self) -> None:
        """Test str(Selection) and
        str(UnaryOperationRelation[Selection]).
        """
        relation = self.leaf.with_rows_satisfying(self.predicate)
        self.assertEqual(str(relation), "σ[a>0](leaf)")


if __name__ == "__main__":
    unittest.main()
