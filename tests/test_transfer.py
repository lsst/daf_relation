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

from lsst.daf.relation import Transfer, iteration, tests


class TransferTestCase(tests.RelationTestCase):
    """Tests for the Transfer operation and relations based on it."""

    def setUp(self) -> None:
        self.a = tests.ColumnTag("a")
        self.source_engine = iteration.Engine(name="source")
        self.destination_engine = iteration.Engine(name="destination")
        self.leaf = self.source_engine.make_leaf(
            {self.a}, payload=iteration.RowSequence([{self.a: 0}, {self.a: 1}]), name="leaf"
        )

    def test_attributes(self) -> None:
        """Check that all UnaryOperation and Relation attributes have the
        expected values.
        """
        relation = self.leaf.transferred_to(self.destination_engine)
        assert isinstance(relation, Transfer)
        self.assertEqual(relation.columns, self.leaf.columns)
        self.assertEqual(relation.engine, self.destination_engine)
        self.assertEqual(relation.min_rows, self.leaf.min_rows)
        self.assertEqual(relation.max_rows, self.leaf.max_rows)
        self.assertFalse(relation.is_locked)
        self.assertTrue(relation.destination, self.destination_engine)

    def test_apply_simplify(self) -> None:
        """Test simplification logic in Transfer.apply."""
        self.assert_relations_equal(self.leaf.transferred_to(self.source_engine), self.leaf)
        self.assert_relations_equal(
            self.leaf.transferred_to(self.destination_engine).transferred_to(self.source_engine), self.leaf
        )

    def test_str(self) -> None:
        """Test str(Transfer) and
        str(UnaryOperationRelation[Transfer]).
        """
        relation = self.leaf.transferred_to(self.destination_engine)
        self.assertEqual(str(relation), f"→[destination]({self.leaf})")


if __name__ == "__main__":
    unittest.main()
