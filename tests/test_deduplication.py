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
    ColumnExpression,
    Deduplication,
    EngineError,
    SortTerm,
    UnaryOperationRelation,
    iteration,
    tests,
)


class DeduplicationTestCase(tests.RelationTestCase):
    """Tests for the Deduplication operation and relations based on it."""

    def setUp(self) -> None:
        self.a = tests.ColumnTag("a")
        self.b = tests.ColumnTag("b", is_key=False)
        self.engine = iteration.Engine(name="preferred")
        self.leaf = self.engine.make_leaf(
            {self.a}, payload=iteration.RowSequence([{self.a: 1}, {self.a: 0}, {self.a: 1}]), name="leaf"
        )

    def test_attributes(self) -> None:
        """Check that all UnaryOperation and Relation attributes have the
        expected values.
        """
        relation = self.leaf.without_duplicates()
        assert isinstance(relation, UnaryOperationRelation)
        self.assertEqual(relation.columns, {self.a})
        self.assertEqual(relation.engine, self.engine)
        self.assertEqual(relation.min_rows, 1)
        self.assertEqual(relation.max_rows, self.leaf.max_rows)
        operation = relation.operation
        assert isinstance(operation, Deduplication)
        self.assertEqual(operation.columns_required, set())
        self.assertTrue(operation.is_empty_invariant)
        self.assertFalse(operation.is_count_invariant)
        self.assertFalse(operation.is_order_dependent)
        self.assertFalse(operation.is_count_dependent)

    def test_min_max_rows(self) -> None:
        """Test min and max rows for edge-case deduplications."""
        self.assertEqual(self.leaf.with_only_columns(set()).without_duplicates().min_rows, 1)
        self.assertEqual(self.leaf.with_only_columns(set()).without_duplicates().max_rows, 1)
        leaf0 = self.engine.make_leaf({self.a}, payload=iteration.RowSequence([]), name="leaf")
        self.assertEqual(leaf0.without_duplicates().min_rows, 0)
        self.assertEqual(leaf0.without_duplicates().max_rows, 0)
        self.assertEqual(leaf0.with_only_columns([]).without_duplicates().min_rows, 0)
        self.assertEqual(leaf0.with_only_columns([]).without_duplicates().max_rows, 0)

    def test_backtracking_apply(self) -> None:
        """Test apply logic that involves reordering operations in the existing
        tree to perform the new operation in a preferred engine.
        """
        new_engine = iteration.Engine(name="downstream")
        expression = ColumnExpression.reference(self.a)
        predicate = expression.lt(ColumnExpression.literal(20))
        # Apply a bunch of operations in a new engine that a Deduplication
        # should commute with.
        target = (
            self.leaf.transferred_to(new_engine)
            .with_calculated_column(self.b, expression)
            .with_rows_satisfying(predicate)
            .sorted([SortTerm(ColumnExpression.reference(self.a))])
        )
        # Apply a new Deduplication with backtracking and see that it appears
        # before the transfer to the new engine, with adjustments as needed
        # downstream (to the Projection and Chain, in this case).
        relation = target.without_duplicates(preferred_engine=self.engine, require_preferred_engine=True)
        self.assert_relations_equal(
            relation,
            (
                self.leaf.without_duplicates()
                .transferred_to(new_engine)
                .with_calculated_column(self.b, expression)
                .with_rows_satisfying(predicate)
                .sorted([SortTerm(ColumnExpression.reference(self.a))])
            ),
        )

    def test_no_backtracking(self) -> None:
        """Test apply logic that handles preferred engines without reordering
        operations in the existing tree.
        """
        new_engine = iteration.Engine(name="downstream")
        # Construct a relation tree we can't reorder when inserting a
        # Deduplication, because there is a locked Materialization in the way.
        target = self.leaf.transferred_to(new_engine).materialized("lock")
        # Preferred engine is ignored if we can't backtrack and don't enable
        # anything else.
        self.assert_relations_equal(
            target.without_duplicates(preferred_engine=self.engine),
            target.without_duplicates(),
        )
        # We can force this to be an error.
        with self.assertRaises(EngineError):
            target.without_duplicates(preferred_engine=self.engine, require_preferred_engine=True)
        # We can also automatically transfer (back) to the preferred engine.
        self.assert_relations_equal(
            target.without_duplicates(preferred_engine=self.engine, transfer=True),
            target.transferred_to(self.engine).without_duplicates(),
        )
        # Now try a few other ways of making backtrack fail.
        # Deduplication does not commute with Projection.
        with self.assertRaises(EngineError):
            self.engine.make_leaf(
                {self.a, self.b},
                payload=iteration.RowSequence([{self.a: 0, self.b: 0}, {self.a: 0, self.b: 1}]),
                name="leaf",
            ).transferred_to(new_engine).with_only_columns({self.a}).without_duplicates(
                preferred_engine=self.engine, require_preferred_engine=True
            )
        # Deduplication does not commute with Slice.
        with self.assertRaises(EngineError):
            self.leaf.transferred_to(new_engine)[:1].without_duplicates(
                preferred_engine=self.engine, require_preferred_engine=True
            )
        # Deduplication cannot be inserted past Chains or Joins
        # (at least not without more information than we have, like whether
        # Chain branches are disjoint or leaf relations start out with unique
        # rows).
        with self.assertRaises(EngineError):
            target = self.leaf.transferred_to(new_engine).chain(
                new_engine.make_leaf(
                    {self.a},
                    payload=iteration.RowSequence([{self.a: 0}]),
                    name="chain_leaf",
                )
            )
            target.without_duplicates(preferred_engine=self.engine, require_preferred_engine=True)
        with self.assertRaises(EngineError):
            target = self.leaf.transferred_to(new_engine).join(
                new_engine.make_leaf(
                    {self.a},
                    payload=iteration.RowSequence([{self.a: 0}]),
                    name="join_leaf",
                )
            )
            target.without_duplicates(preferred_engine=self.engine, require_preferred_engine=True)

    def test_iteration(self) -> None:
        """Test Deduplication execution in the iteration engine."""
        relation = self.leaf.without_duplicates()
        self.assertEqual(
            list(self.engine.execute(relation)),
            [{self.a: 1}, {self.a: 0}],
        )

    def test_str(self) -> None:
        """Test str(Deduplication) and
        str(UnaryOperationRelation[Deduplication]).
        """
        relation = self.leaf.without_duplicates()
        self.assertEqual(str(relation), "deduplicate(leaf)")


if __name__ == "__main__":
    unittest.main()
