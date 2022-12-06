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

from lsst.daf.relation import ColumnExpression, Materialization, iteration, tests


class MaterializationTestCase(tests.RelationTestCase):
    """Tests for the Materialization operation and relations based on it."""

    def setUp(self) -> None:
        self.a = tests.ColumnTag("a")
        self.engine = iteration.Engine(name="preferred")
        self.leaf = self.engine.make_leaf(
            {self.a}, payload=iteration.RowSequence([{self.a: 0}, {self.a: 1}]), name="leaf"
        )
        # Materializing a leaf just returns the leaf, so we add a Selection to
        # make applying it nontrivial.
        self.target = self.leaf.with_rows_satisfying(
            ColumnExpression.reference(self.a).gt(ColumnExpression.literal(0))
        )

    def test_attributes(self) -> None:
        """Check that all UnaryOperation and Relation attributes have the
        expected values.
        """
        relation = self.target.materialized(name_prefix="prefix")
        assert isinstance(relation, Materialization)
        self.assertEqual(relation.columns, {self.a})
        self.assertEqual(relation.engine, self.engine)
        self.assertEqual(relation.min_rows, self.target.min_rows)
        self.assertEqual(relation.max_rows, self.target.max_rows)
        self.assertTrue(relation.is_locked)
        self.assertTrue(relation.name.startswith("prefix"))

    def test_apply_simplify(self) -> None:
        """Test that applying a Materialization to a leaf or an existing
        materialization does nothing.
        """
        self.assertEqual(self.leaf.materialized(), self.leaf)
        self.assertEqual(self.target.materialized(name="a").materialized("b"), self.target.materialized("a"))

    def test_iteration(self) -> None:
        """Test Materialization execution in the iteration engine."""
        relation = self.target.materialized(name="m")
        self.assertEqual(
            list(self.engine.execute(relation)),
            [{self.a: 1}],
        )
        self.assertIsNotNone(relation.payload)
        self.assertIsInstance(relation.payload, iteration.MaterializedRowIterable)

    def test_str(self) -> None:
        """Test str(Materialization) and
        str(UnaryOperationRelation[Materialization]).
        """
        relation = self.target.materialized(name="m")
        self.assertEqual(str(relation), f"materialize['m']({self.target})")


if __name__ == "__main__":
    unittest.main()
