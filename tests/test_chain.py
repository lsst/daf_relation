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

from lsst.daf.relation import BinaryOperationRelation, Chain, ColumnError, EngineError, iteration, tests


class ChainTestCase(tests.RelationTestCase):
    """Tests for the Chain operation and relations based on it."""

    def setUp(self) -> None:
        self.a = tests.ColumnTag("a")
        self.b = tests.ColumnTag("b")
        self.engine = iteration.Engine(name="preferred")
        self.leaf_1 = self.engine.make_leaf(
            {self.a, self.b},
            payload=iteration.RowSequence([{self.a: 0, self.b: 5}, {self.a: 1, self.b: 10}]),
            name="leaf_1",
        )
        self.leaf_2 = self.engine.make_leaf(
            {self.a, self.b},
            payload=iteration.RowSequence([{self.a: 2, self.b: 15}, {self.a: 3, self.b: 20}]),
            name="leaf_2",
        )

    def test_attributes(self) -> None:
        """Check that all Relation attributes have the expected values."""
        relation = self.leaf_1.chain(self.leaf_2)
        assert isinstance(relation, BinaryOperationRelation)
        self.assertEqual(relation.columns, {self.a, self.b})
        self.assertEqual(relation.engine, self.engine)
        self.assertEqual(relation.min_rows, 4)
        self.assertEqual(relation.max_rows, 4)
        self.assertFalse(relation.is_locked)
        operation = relation.operation
        assert isinstance(operation, Chain)

    def test_apply_failures(self) -> None:
        """Test failure modes of constructing and applying Chains."""
        new_engine = iteration.Engine(name="other")
        leaf_3 = new_engine.make_leaf(
            {self.a, self.b},
            payload=iteration.RowSequence([{self.a: 3, self.b: 4}]),
            name="leaf_3",
        )
        with self.assertRaises(EngineError):
            self.leaf_1.chain(leaf_3)
        leaf_4 = self.engine.make_leaf(
            {self.a},
            payload=iteration.RowSequence([{self.a: 3}]),
            name="leaf_4",
        )
        with self.assertRaises(ColumnError):
            self.leaf_1.chain(leaf_4)

    def test_iteration(self) -> None:
        """Test Chain execution in the iteration engine."""
        relation = self.leaf_1.chain(self.leaf_2)
        self.assertEqual(
            list(self.engine.execute(relation)),
            list(self.leaf_1.payload) + list(self.leaf_2.payload),
        )

    def test_str(self) -> None:
        """Test str(Chain) and str(BinaryOperationRelation[Chain])."""
        relation = self.leaf_1.chain(self.leaf_2)
        self.assertEqual(str(relation), "leaf_1 ∪ leaf_2")
        # Nested operations get parentheses, unless they're chains or leaves.
        leaf_3 = self.engine.make_leaf(
            {self.a, self.b},
            payload=iteration.RowSequence([{self.a: 3, self.b: 4}]),
            name="leaf_3",
        )
        self.assertEqual(str(relation.chain(leaf_3)), "leaf_1 ∪ leaf_2 ∪ leaf_3")
        self.assertEqual(str(self.leaf_1.join(self.leaf_2).chain(leaf_3)), "(leaf_1 ⋈ leaf_2) ∪ leaf_3")


if __name__ == "__main__":
    unittest.main()
