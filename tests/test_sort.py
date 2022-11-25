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

import numpy as np
from lsst.daf.relation import (
    ColumnError,
    ColumnExpression,
    EngineError,
    Sort,
    SortTerm,
    UnaryOperationRelation,
    iteration,
    tests,
)


class SortTestCase(tests.RelationTestCase):
    """Tests for the Sort operation and relations based on it."""

    def setUp(self) -> None:
        self.columns = {k: tests.ColumnTag(k) for k in "abcd"}
        self.sort_terms = (
            SortTerm(ColumnExpression.reference(self.columns["a"]), ascending=True),
            SortTerm(ColumnExpression.reference(self.columns["b"]), ascending=True),
            SortTerm(ColumnExpression.reference(self.columns["c"]), ascending=False),
            SortTerm(ColumnExpression.reference(self.columns["d"]), ascending=True),
        )
        self.engine = iteration.Engine(name="preferred")
        rng = np.random.RandomState(1)
        self.table = np.zeros(32, dtype=[(k, int) for k in self.columns])
        for k in self.columns:
            self.table[k] = rng.randint(0, 4, size=32)
        self.leaf = self.engine.make_leaf(
            frozenset(self.columns.values()),
            payload=iteration.RowSequence(
                [{v: row[k] for k, v in self.columns.items()} for row in self.table]
            ),
            name="leaf",
        )

    def test_attributes(self) -> None:
        """Check that all UnaryOperation and Relation attributes have the
        expected values.
        """
        relation = self.leaf.sorted(self.sort_terms)
        assert isinstance(relation, UnaryOperationRelation)
        self.assertEqual(relation.columns, frozenset(self.columns.values()))
        self.assertEqual(relation.engine, self.engine)
        self.assertEqual(relation.min_rows, self.leaf.min_rows)
        self.assertEqual(relation.max_rows, self.leaf.max_rows)
        operation = relation.operation
        assert isinstance(operation, Sort)
        self.assertEqual(operation.terms, self.sort_terms)
        self.assertEqual(operation.columns_required, frozenset(self.columns.values()))
        self.assertTrue(operation.is_empty_invariant)
        self.assertTrue(operation.is_count_invariant)
        self.assertFalse(operation.is_order_dependent)
        self.assertFalse(operation.is_count_dependent)

    def test_apply_failures(self) -> None:
        """Test failure modes of constructing and applying Sorts."""
        # Required columns must be present.
        with self.assertRaises(ColumnError):
            self.leaf.sorted([SortTerm(ColumnExpression.reference(tests.ColumnTag("e")))])

    def test_apply_simplify(self) -> None:
        """Test simplification logic in Sort.apply."""
        # Test that applying a Sort to an existing Sort merges them.
        self.assert_relations_equal(
            self.leaf.sorted(self.sort_terms[2:4]).sorted(self.sort_terms[0:2]),
            self.leaf.sorted(self.sort_terms),
        )
        # Test that a no-op Sort does nothing.
        self.assert_relations_equal(self.leaf.sorted([]), self.leaf)

    def test_backtracking_apply(self) -> None:
        """Test apply logic that involves reordering operations in the existing
        tree to perform the new operation in a preferred engine.
        """
        new_engine = iteration.Engine(name="downstream")
        expression = ColumnExpression.function(
            "__add__",
            ColumnExpression.reference(self.columns["a"]),
            ColumnExpression.reference(self.columns["b"]),
        )
        predicate = ColumnExpression.reference(self.columns["c"]).gt(
            ColumnExpression.reference(self.columns["d"])
        )
        e = tests.ColumnTag("e")
        # Apply a bunch of operations in a new engine that a Sort should
        # commute with.
        target = (
            self.leaf.transferred_to(new_engine)
            .with_calculated_column(e, expression)
            .with_rows_satisfying(predicate)
            .without_duplicates()
            .with_only_columns(frozenset(self.columns.values()))
        )
        # Apply a new Sort with backtracking and see that it appears before the
        # transfer to the new engine, with adjustments as needed.
        relation = target.sorted(self.sort_terms, preferred_engine=self.engine, require_preferred_engine=True)
        self.assert_relations_equal(
            relation,
            (
                self.leaf.sorted(self.sort_terms, preferred_engine=self.engine, require_preferred_engine=True)
                .transferred_to(new_engine)
                .with_calculated_column(e, expression)
                .with_rows_satisfying(predicate)
                .without_duplicates()
                .with_only_columns(frozenset(self.columns.values()))
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
            target.sorted(self.sort_terms, preferred_engine=self.engine),
            target.sorted(self.sort_terms),
        )
        # We can force this to be an error.
        with self.assertRaises(EngineError):
            target.sorted(self.sort_terms, preferred_engine=self.engine, require_preferred_engine=True)
        # We can also automatically transfer (back) to the preferred engine.
        self.assert_relations_equal(
            target.sorted(self.sort_terms, preferred_engine=self.engine, transfer=True),
            target.transferred_to(self.engine).sorted(self.sort_terms),
        )
        # Can't backtrack through a Calculation that provides required columns.
        # In the future, we could make this possible by subsuming the
        # calculated columns into the predicate.
        e = tests.ColumnTag("e")
        target = self.leaf.transferred_to(new_engine).with_calculated_column(
            e, ColumnExpression.reference(self.columns["a"])
        )
        with self.assertRaises(EngineError):
            target.sorted(
                [SortTerm(ColumnExpression.reference(e))],
                preferred_engine=self.engine,
                require_preferred_engine=True,
            )
        # Can't backtrack through a slice.
        target = self.leaf.transferred_to(new_engine)[1:3]
        with self.assertRaises(EngineError):
            target.sorted(self.sort_terms, preferred_engine=self.engine, require_preferred_engine=True)

    def test_iteration(self) -> None:
        """Test Sort execution in the iteration engine."""
        relation = self.leaf.sorted(self.sort_terms)
        sorted_table = self.table.copy()
        sorted_table["c"] *= -1
        sorted_table.sort(kind="stable", order="d")
        sorted_table.sort(kind="stable", order="c")
        sorted_table.sort(kind="stable", order=["a", "b"])
        sorted_table["c"] *= -1
        self.assertEqual(
            list(self.engine.execute(relation)),
            [{v: row[k] for k, v in self.columns.items()} for row in sorted_table],
        )

    def test_str(self) -> None:
        """Test str(Sort) and
        str(UnaryOperationRelation[Sort]).
        """
        relation = self.leaf.sorted(self.sort_terms)
        self.assertEqual(str(relation), "sort[a, b, -c, d](leaf)")


if __name__ == "__main__":
    unittest.main()
