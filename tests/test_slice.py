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
    EngineError,
    LeafRelation,
    Relation,
    Slice,
    UnaryOperationRelation,
    iteration,
    tests,
)


class SliceTestCase(tests.RelationTestCase):
    """Tests for the Slice operation and relations based on it."""

    def setUp(self) -> None:
        self.a = tests.ColumnTag("a")
        self.engine = iteration.Engine(name="preferred")
        self.leaf = self.engine.make_leaf(
            {self.a}, payload=iteration.RowSequence([{self.a: 0}, {self.a: 1}]), name="leaf"
        )

    def test_attributes(self) -> None:
        """Check that all UnaryOperation and Relation attributes have the
        expected values.
        """
        relation = self.leaf[1:2]
        assert isinstance(relation, UnaryOperationRelation)
        self.assertEqual(relation.columns, {self.a})
        self.assertEqual(relation.engine, self.engine)
        self.assertEqual(relation.min_rows, 1)
        self.assertEqual(relation.max_rows, 1)
        self.assertFalse(relation.is_locked)
        operation = relation.operation
        assert isinstance(operation, Slice)
        self.assertEqual(operation.start, 1)
        self.assertEqual(operation.stop, 2)
        self.assertEqual(operation.limit, 1)
        self.assertEqual(operation.columns_required, frozenset())
        self.assertFalse(operation.is_empty_invariant)
        self.assertFalse(operation.is_count_invariant)
        self.assertTrue(operation.is_order_dependent)
        self.assertTrue(operation.is_count_dependent)
        # Also check min/max attributes an unbounded Slice, since that involves
        # a few different logic branches.
        relation = self.leaf[1:]
        assert isinstance(relation, UnaryOperationRelation)
        self.assertEqual(relation.columns, {self.a})
        self.assertEqual(relation.engine, self.engine)
        self.assertEqual(relation.min_rows, 1)
        self.assertEqual(relation.max_rows, 1)
        self.assertFalse(relation.is_locked)
        operation = relation.operation
        assert isinstance(operation, Slice)
        self.assertEqual(operation.start, 1)
        self.assertEqual(operation.stop, None)
        self.assertEqual(operation.limit, None)
        self.assertEqual(operation.columns_required, frozenset())
        self.assertFalse(operation.is_empty_invariant)
        self.assertFalse(operation.is_count_invariant)
        self.assertTrue(operation.is_order_dependent)
        self.assertTrue(operation.is_count_dependent)

    def test_min_max_rows(self) -> None:
        """Test min_rows and max_rows for different kinds of slices
        and original min/max rows.
        """
        # iteration.Engine.make_leaf sets min_rows and max_rows based on
        # len(payload), which we don't want here.
        leaf1 = LeafRelation(self.engine, frozenset({self.a}), payload=..., min_rows=0, max_rows=None)
        leaf2 = LeafRelation(self.engine, frozenset({self.a}), payload=..., min_rows=0, max_rows=5)
        leaf3 = LeafRelation(self.engine, frozenset({self.a}), payload=..., min_rows=5, max_rows=5)
        leaf4 = LeafRelation(self.engine, frozenset({self.a}), payload=..., min_rows=5, max_rows=8)
        leaf5 = LeafRelation(self.engine, frozenset({self.a}), payload=..., min_rows=5, max_rows=None)

        # Reasoning about the expected values of slice operations is really
        # easy to get wrong, so instead we brute-force the expected values,
        # ultimately delegating to Python's own implementation of slicing
        # range objects.

        def brute_force_row_bounds(
            input_min_rows: int, input_max_rows: int | None, start: int, stop: int | None
        ) -> tuple[int, int | None]:
            """Compute the minimum and maximum number of rows a sequence could
            have after slicing.

            Parameters
            ----------
            input_min_rows, input_min_rows : `int` or `None`
                Original bounds on the number of rows.
            start, stop: `int` or `None`
                Slice parameters

            Returns
            -------
            output_min_rows, output_min_rows : `int` or `None`
                Bounds on the number of rows for the sliced sequence.

            Notes
            -----
            Since this is just a test helper, we handle `None` by assuming it
            can be replaced by a large value and that large values in the
            results indicate a `None` result.  Keep all concrete integers below
            100 to avoid problems.
            """
            sizes = []
            if input_max_rows is None:
                output_min_rows, output_max_rows = brute_force_row_bounds(input_min_rows, 100, start, stop)
                if output_max_rows is not None and output_max_rows > 50:
                    output_max_rows = None
                return output_min_rows, output_max_rows
            for n_rows in range(input_min_rows, input_max_rows + 1):
                sequence = range(n_rows)
                sizes.append(len(sequence[slice(start, stop)]))
            return min(sizes), max(sizes)

        def check(leaf: Relation) -> None:
            """Run tests on the given leaf relation by applying slices with
            a number of start and stop values that are just above, just below,
            or equal to its min and max rows.
            """
            breaks_set = {0, leaf.min_rows - 1, leaf.min_rows, leaf.min_rows + 1}
            if leaf.max_rows is not None:
                breaks_set.update({leaf.max_rows - 1, leaf.max_rows, leaf.max_rows + 1})
            breaks_list = list(breaks_set)
            breaks_list.sort()
            for start in breaks_list:
                for stop in breaks_list + [None]:
                    if start < 0:
                        with self.assertRaises(ValueError):
                            Slice(start, stop)
                    elif stop is not None and stop < start:
                        with self.assertRaises(ValueError):
                            Slice(start, stop)
                    else:
                        relation = leaf[slice(start, stop)]
                        self.assertEqual(
                            (relation.min_rows, relation.max_rows),
                            brute_force_row_bounds(leaf.min_rows, leaf.max_rows, start, stop),
                            msg=(
                                f"leaf.min_rows={leaf.min_rows}, "
                                f"leaf.max_rows={leaf.max_rows}, "
                                f"slice=[{start}:{stop}]"
                            ),
                        )

        check(leaf1)
        check(leaf2)
        check(leaf3)
        check(leaf4)
        check(leaf5)

    def test_backtracking_apply(self) -> None:
        """Test apply logic that involves reordering operations in the existing
        tree to perform the new operation in a preferred engine.
        """
        new_engine = iteration.Engine(name="downstream")
        b = tests.ColumnTag("b")
        expression = ColumnExpression.function(
            "__add__", ColumnExpression.reference(self.a), ColumnExpression.literal(5)
        )
        # Apply operations in a new engine that a Slice should commute with.
        target = (
            self.leaf.transferred_to(new_engine).with_calculated_column(b, expression).with_only_columns({b})
        )
        # Apply a new Slice with backtracking and see that it appears
        # before the transfer to the new engine.
        relation = Slice(start=1, stop=3).apply(
            target, preferred_engine=self.engine, require_preferred_engine=True
        )
        self.assert_relations_equal(
            relation,
            (
                self.leaf[1:3]
                .transferred_to(new_engine)
                .with_calculated_column(b, expression)
                .with_only_columns({b})
            ),
        )

    def test_no_backtracking(self) -> None:
        """Test apply logic that handles preferred engines without reordering
        operations in the existing tree.
        """
        new_engine = iteration.Engine(name="downstream")
        # Construct a relation tree we can't reorder when inserting a
        # Sort, because there is a locked Materialization in the way.
        target = self.leaf.transferred_to(new_engine).materialized("lock")
        # Preferred engine is ignored if we can't backtrack and don't enable
        # anything else.
        self.assert_relations_equal(
            Slice(start=1, stop=3).apply(target, preferred_engine=self.engine),
            Slice(start=1, stop=3).apply(target),
        )
        # We can force this to be an error.
        with self.assertRaises(EngineError):
            Slice(start=1, stop=3).apply(target, preferred_engine=self.engine, require_preferred_engine=True)
        # We can also automatically transfer (back) to the preferred engine.
        self.assert_relations_equal(
            Slice(start=1, stop=3).apply(target, preferred_engine=self.engine, transfer=True),
            target.transferred_to(self.engine)[1:3],
        )
        # Can't backtrack through anything other than a Projection or
        # a Calculation.
        target = self.leaf.transferred_to(new_engine).without_duplicates()
        with self.assertRaises(EngineError):
            Slice(start=1, stop=3).apply(target, preferred_engine=self.engine, require_preferred_engine=True)

    def test_apply_simplify(self) -> None:
        """Test simplification logic in Slice.apply."""
        # Test that applying a Slice to an existing Slice merges them.
        self.assert_relations_equal(self.leaf[1:][:2], self.leaf[1:3])
        self.assert_relations_equal(self.leaf[1:][1:], self.leaf[2:])
        self.assert_relations_equal(self.leaf[1:3][1:2], self.leaf[2:3])
        self.assert_relations_equal(self.leaf[1:3][1:], self.leaf[2:3])
        # Test that a no-op slice does nothing.
        self.assert_relations_equal(self.leaf[:], self.leaf)

    def test_iteration(self) -> None:
        """Test Slice execution in the iteration engine."""
        self.assertEqual(list(self.engine.execute(self.leaf[1:])), [{self.a: 1}])
        self.assertEqual(list(self.engine.execute(self.leaf[:1])), [{self.a: 0}])
        self.assertEqual(list(self.engine.execute(self.leaf[1:2])), [{self.a: 1}])
        self.assertEqual(list(self.engine.execute(self.leaf[2:])), [])
        self.assertEqual(list(self.engine.execute(self.leaf[2:3])), [])
        # Also try a non-leaf target, since that's a different code branch in
        # the iteration engine.
        b = tests.ColumnTag("b")
        target = self.leaf.with_calculated_column(b, ColumnExpression.reference(self.a))
        self.assertEqual(list(self.engine.execute(target[1:])), [{self.a: 1, b: 1}])
        self.assertEqual(list(self.engine.execute(target[:1])), [{self.a: 0, b: 0}])
        self.assertEqual(list(self.engine.execute(target[1:2])), [{self.a: 1, b: 1}])
        self.assertEqual(list(self.engine.execute(target[2:])), [])
        self.assertEqual(list(self.engine.execute(target[2:3])), [])

    def test_str(self) -> None:
        """Test str(Slice) and
        str(UnaryOperationRelation[Slice]).
        """
        relation = self.leaf[1:2]
        self.assertEqual(str(relation), f"slice[1:2]({self.leaf})")


if __name__ == "__main__":
    unittest.main()
