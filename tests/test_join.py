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
    BinaryOperationRelation,
    ColumnError,
    ColumnExpression,
    EngineError,
    Join,
    Predicate,
    SortTerm,
    iteration,
    tests,
)


class JoinTestCase(tests.RelationTestCase):
    """Tests for the Join operation and relations based on it."""

    def setUp(self) -> None:
        self.a = tests.ColumnTag("a")
        self.b = tests.ColumnTag("b")
        self.c = tests.ColumnTag("c")
        self.engine = iteration.Engine(name="preferred")
        self.leaf_1 = self.engine.make_leaf(
            {self.a, self.b},
            payload=iteration.RowSequence(
                [{self.a: 0, self.b: 5}, {self.a: 1, self.b: 10}, {self.a: 2, self.b: 25}]
            ),
            name="leaf_1",
        )
        self.leaf_2 = self.engine.make_leaf(
            {self.a, self.c},
            payload=iteration.RowSequence(
                [{self.a: 0, self.c: 15}, {self.a: 2, self.c: 20}, {self.a: 3, self.b: 0}]
            ),
            name="leaf_2",
        )

    def test_attributes(self) -> None:
        """Check that all Relation and PartialJoin attributes have the expected
        values."""
        relation = self.leaf_1.join(self.leaf_2)
        assert isinstance(relation, BinaryOperationRelation)
        self.assertEqual(relation.columns, {self.a, self.b, self.c})
        self.assertEqual(relation.engine, self.engine)
        self.assertEqual(relation.min_rows, 0)
        self.assertEqual(relation.max_rows, 9)
        self.assertFalse(relation.is_locked)
        operation = relation.operation
        assert isinstance(operation, Join)
        self.assertEqual(operation.min_columns, {self.a})
        self.assertEqual(operation.max_columns, {self.a})
        self.assertEqual(operation.common_columns, {self.a})
        self.assertEqual(operation.predicate, Predicate.literal(True))
        partial = Join().partial(self.leaf_1)
        self.assertEqual(partial.columns_required, frozenset())
        self.assert_relations_equal(partial.fixed, self.leaf_1)
        self.assertFalse(partial.is_count_dependent)
        self.assertFalse(partial.is_order_dependent)
        self.assertFalse(partial.is_count_invariant)
        self.assertFalse(partial.is_empty_invariant)
        self.assertEqual(partial.applied_columns(self.leaf_2), {self.a, self.b, self.c})
        self.assertEqual(partial.applied_min_rows(self.leaf_2), 0)
        self.assertEqual(partial.applied_max_rows(self.leaf_2), 9)

    def test_apply_failures(self) -> None:
        """Test failure modes of constructing and applying Join."""
        # Mismatched engines.
        new_engine = iteration.Engine(name="downstream")
        with self.assertRaises(EngineError):
            Join().apply(self.leaf_1.transferred_to(new_engine), self.leaf_2)
        # Predicate requires nonexistent columns.
        predicate = ColumnExpression.reference(tests.ColumnTag("d")).lt(ColumnExpression.literal(0))
        with self.assertRaises(ColumnError):
            Join(predicate=predicate).apply(self.leaf_1, self.leaf_2)
        with self.assertRaises(ColumnError):
            Join(predicate=predicate).partial(self.leaf_1).apply(self.leaf_2)
        with self.assertRaises(ColumnError):
            Join(predicate=predicate).partial(self.leaf_2).apply(self.leaf_1)
        # Bounds on columns internally inconsistent.
        with self.assertRaises(ColumnError):
            Join(min_columns=frozenset({self.a, self.b}), max_columns=frozenset({self.a}))
        # Minimum columns not satisfied.
        join = Join(min_columns=frozenset({self.a, self.b}))
        with self.assertRaises(ColumnError):
            join.apply(self.leaf_1, self.leaf_2)
        with self.assertRaises(ColumnError):
            join.apply(self.leaf_2, self.leaf_1)
        with self.assertRaises(ColumnError):
            join.partial(self.leaf_2)
        with self.assertRaises(ColumnError):
            join.partial(self.leaf_1).apply(self.leaf_2)
        # Common columns not satisfied.
        join = Join(min_columns=frozenset({self.a, self.b}), max_columns=frozenset({self.a, self.b}))
        with self.assertRaises(ColumnError):
            join.apply(self.leaf_1, self.leaf_2)
        with self.assertRaises(ColumnError):
            join.apply(self.leaf_2, self.leaf_1)

    def test_apply_simplify(self) -> None:
        """Test Join.apply simplifications."""
        join_identity = self.engine.make_join_identity_relation()
        self.assertIs(self.leaf_1.join(join_identity), self.leaf_1)
        self.assertIs(join_identity.join(self.leaf_1), self.leaf_1)

    def test_backtracking_apply(self) -> None:
        """Test `PartialJoin.apply` logic that involves reordering operations
        in the existing tree to perform the new operation in a preferred
        engine.
        """
        new_engine = iteration.Engine(name="downstream")
        d = tests.ColumnTag("d")
        expression = ColumnExpression.function(
            "__add__", ColumnExpression.reference(self.a), ColumnExpression.literal(5)
        )
        sort_terms = [SortTerm(ColumnExpression.reference(self.a))]
        predicate = ColumnExpression.reference(self.b).gt(ColumnExpression.literal(0))
        # Apply a bunch of operations in a new engine that a PartialJoin should
        # commute with.
        target = (
            self.leaf_1.transferred_to(new_engine)
            .with_calculated_column(d, expression)
            .with_rows_satisfying(predicate)
            .with_only_columns({self.a, d})
            .sorted(sort_terms)
        )
        # Apply a new PartialJoin with backtracking and see that it appears
        # before the transfer to the new engine, with adjustments as needed.
        relation = target.join(self.leaf_2)
        self.assert_relations_equal(
            relation,
            (
                self.leaf_1.join(self.leaf_2)
                .transferred_to(new_engine)
                .with_calculated_column(d, expression)
                .with_rows_satisfying(predicate)
                .with_only_columns({self.a, self.c, d})
                .sorted(sort_terms)
            ),
        )

    def test_no_backtracking(self) -> None:
        """Test `PartialJoin.apply` logic that handles differing engines
        without reordering operations in the existing tree, as well as failures
        in that reordering.
        """
        new_engine = iteration.Engine(name="downstream")
        # Construct a relation tree we can't reorder when inserting a Join,
        # because there is a locked Materialization in the way.
        target = self.leaf_1.transferred_to(new_engine).materialized("lock")
        # We can automatically transfer (back) to the new relation's engine.
        self.assert_relations_equal(
            target.join(self.leaf_2, transfer=True),
            target.transferred_to(self.engine).join(self.leaf_2),
        )
        # Can't backtrack through a Deduplication.
        target = self.leaf_1.transferred_to(new_engine).without_duplicates()
        with self.assertRaises(EngineError):
            target.join(self.leaf_2)
        # Can't backtrack through a Slice, because it's order/count dependent.
        target = self.leaf_1.transferred_to(new_engine)[:2]
        with self.assertRaises(EngineError):
            target.join(self.leaf_2)

    def test_common_columns(self) -> None:
        """Test Join.applied_common_columns logic."""
        leaf_3 = self.engine.make_leaf(
            {self.a, self.b, self.c},
            payload=iteration.RowSequence(
                [{self.a: 0, self.b: 2, self.c: 15}, {self.a: 2, self.b: 4, self.c: 20}]
            ),
            name="leaf_2",
        )
        # With no min or max columns, common_columns is just the intersection
        # of the columns of the operands.
        self.assertEqual(Join().applied_common_columns(self.leaf_1, leaf_3), {self.a, self.b})
        # Check that max_columns is enforced.
        self.assertEqual(
            Join(max_columns=frozenset({self.a})).applied_common_columns(self.leaf_1, leaf_3), {self.a}
        )
        # Check that min_columns is enforced.
        with self.assertRaises(ColumnError):
            Join(min_columns=frozenset({self.c})).applied_common_columns(self.leaf_1, leaf_3)
        # Repeat last two checks with min_columns == max_columns.
        self.assertEqual(
            Join(min_columns=frozenset({self.a}), max_columns=frozenset({self.a})).applied_common_columns(
                self.leaf_1, leaf_3
            ),
            {self.a},
        )
        with self.assertRaises(ColumnError):
            Join(min_columns=frozenset({self.c}), max_columns=frozenset({self.c})).apply(self.leaf_1, leaf_3)

    def test_str(self) -> None:
        """Test str(Join), str(PartialJoin), and
        str(BinaryOperationRelation[Join]).
        """
        relation = self.leaf_1.join(self.leaf_2)
        self.assertEqual(str(relation), "leaf_1 ⋈ leaf_2")
        partial = Join().partial(self.leaf_1)
        self.assertEqual(str(partial), "⋈[leaf_1]")
        # Nested operations get parentheses, unless they're joins or leaves.
        leaf_3 = self.engine.make_leaf(
            {self.a, self.b},
            payload=iteration.RowSequence([{self.a: 3, self.b: 4}]),
            name="leaf_3",
        )
        self.assertEqual(str(relation.join(leaf_3)), "leaf_1 ⋈ leaf_2 ⋈ leaf_3")
        self.assertEqual(str(self.leaf_1.chain(leaf_3).join(self.leaf_2)), "(leaf_1 ∪ leaf_3) ⋈ leaf_2")


if __name__ == "__main__":
    unittest.main()
