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
    Projection,
    SortTerm,
    UnaryOperationRelation,
    iteration,
    tests,
)


class ProjectionTestCase(tests.RelationTestCase):
    """Tests for the Projection operation and relations based on it."""

    def setUp(self) -> None:
        self.a = tests.ColumnTag("a")
        self.b = tests.ColumnTag("b")
        self.c = tests.ColumnTag("c")
        self.engine = iteration.Engine(name="preferred")
        self.leaf = self.engine.make_leaf(
            {self.a, self.b, self.c},
            payload=iteration.RowSequence(
                [{self.a: 1, self.b: 4}, {self.a: 0, self.b: 5}, {self.a: 1, self.b: 6}]
            ),
            name="leaf",
        )

    def test_attributes(self) -> None:
        """Check that all UnaryOperation and Relation attributes have the
        expected values.
        """
        relation = self.leaf.with_only_columns({self.a})
        assert isinstance(relation, UnaryOperationRelation)
        self.assertEqual(relation.columns, {self.a})
        self.assertEqual(relation.engine, self.engine)
        self.assertEqual(relation.min_rows, self.leaf.min_rows)
        self.assertEqual(relation.max_rows, self.leaf.max_rows)
        operation = relation.operation
        assert isinstance(operation, Projection)
        self.assertEqual(operation.columns, {self.a})
        self.assertEqual(operation.columns_required, {self.a})
        self.assertTrue(operation.is_empty_invariant)
        self.assertTrue(operation.is_count_invariant)
        self.assertFalse(operation.is_order_dependent)
        self.assertFalse(operation.is_count_dependent)

    def test_apply_failures(self) -> None:
        """Test failure modes of constructing and applying Projections."""
        # Required columns must be present.
        with self.assertRaises(ColumnError):
            self.leaf.with_only_columns({self.a, tests.ColumnTag("d")})

    def test_backtracking_apply(self) -> None:
        """Test apply logic that involves reordering operations in the existing
        tree to perform the new operation in a preferred engine.
        """
        new_engine = iteration.Engine(name="downstream")
        d = tests.ColumnTag("d")
        predicate = ColumnExpression.reference(self.a).lt(ColumnExpression.literal(2))
        expression = ColumnExpression.function(
            "__add__", ColumnExpression.reference(self.a), ColumnExpression.literal(5)
        )
        sort_terms = [SortTerm(ColumnExpression.reference(self.a))]
        # Apply a bunch of operations in a new engine that a Projection should
        # commute with.
        target = (
            self.leaf.transferred_to(new_engine)
            .with_calculated_column(d, expression)
            .with_rows_satisfying(predicate)
            .without_duplicates()
            .sorted(sort_terms)
        )[:2]
        # Apply a Projection to just {self.a} and check that it:
        # - appears before the transfer
        # - results in the Calculation being dropped (since that now does
        #   nothing).
        self.assert_relations_equal(
            target.with_only_columns({self.a}, preferred_engine=self.engine, require_preferred_engine=True),
            (
                self.leaf.with_only_columns({self.a})
                .transferred_to(new_engine)
                .with_rows_satisfying(predicate)
                .without_duplicates()
                .sorted(sort_terms)
            )[:2],
        )
        # Apply a Projection to {self.b}, which cannot be moved past the
        # calculation or selection in its entirety, since they all depend on
        # {self.a}.  We should move as much of the projection as possible as
        # far upstream as possible, and leave the rest to be applied in the
        # non-preferred engine.
        self.assert_relations_equal(
            target.with_only_columns({self.b}, preferred_engine=self.engine),
            (
                self.leaf.with_only_columns({self.a, self.b})
                .transferred_to(new_engine)
                .with_rows_satisfying(predicate)
                .without_duplicates()
                .sorted(sort_terms)
            )[:2].with_only_columns({self.b}),
        )
        # Similar attempt with Projection to {self.c}, this time with a
        # transfer request.
        self.assert_relations_equal(
            target.with_only_columns({self.c}, preferred_engine=self.engine, transfer=True),
            (
                self.leaf.with_only_columns({self.a, self.c})
                .transferred_to(new_engine)
                .with_rows_satisfying(predicate)
                .without_duplicates()
                .sorted(sort_terms)
            )[:2]
            .transferred_to(self.engine)
            .with_only_columns({self.c}),
        )
        # Test that a projection that does nothing is simplified away.
        self.assert_relations_equal(self.leaf.with_only_columns(self.leaf.columns), self.leaf)
        # Test that back-to-back projections and do-nothing calculations are
        # simplified away, regardless of whether they are before or after the
        # preferred-engine transfer.
        target = self.leaf.with_calculated_column(d, expression).transferred_to(new_engine)
        self.assert_relations_equal(
            target.with_only_columns({self.a, d}, preferred_engine=self.engine).with_only_columns(
                {self.a}, preferred_engine=self.engine
            ),
            target.with_only_columns({self.a}, preferred_engine=self.engine),
        )
        self.assert_relations_equal(
            target.with_only_columns({self.a, d}).with_only_columns({self.a}, preferred_engine=self.engine),
            target.with_only_columns({self.a}, preferred_engine=self.engine),
        )

    def test_no_backtracking(self) -> None:
        """Test apply logic that handles preferred engines without reordering
        operations in the existing tree.
        """
        new_engine = iteration.Engine(name="downstream")
        # Construct a relation tree we can't reorder when inserting a
        # Projection, because there is a locked Materialization in the way.
        target = self.leaf.transferred_to(new_engine).materialized("lock")
        # Preferred engine is ignored if we can't backtrack and don't enable
        # anything else.
        self.assert_relations_equal(
            target.with_only_columns({self.a}, preferred_engine=self.engine),
            target.with_only_columns({self.a}),
        )
        # We can force this to be an error.
        with self.assertRaises(EngineError):
            target.with_only_columns({self.a}, preferred_engine=self.engine, require_preferred_engine=True)
        # We can also automatically transfer (back) to the preferred engine.
        self.assert_relations_equal(
            target.with_only_columns({self.a}, preferred_engine=self.engine, transfer=True),
            target.transferred_to(self.engine).with_only_columns({self.a}),
        )

    def test_iteration(self) -> None:
        """Test Projection execution in the iteration engine."""
        relation = self.leaf.with_only_columns({self.a})
        self.assertEqual(
            list(self.engine.execute(relation)),
            [{self.a: 1}, {self.a: 0}, {self.a: 1}],
        )

    def test_str(self) -> None:
        """Test str(Projection) and
        str(UnaryOperationRelation[Projection]).
        """
        relation = self.leaf.with_only_columns({self.a})
        self.assertEqual(str(relation), "Π[a](leaf)")


if __name__ == "__main__":
    unittest.main()
