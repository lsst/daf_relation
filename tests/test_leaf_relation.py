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

from lsst.daf.relation import LeafRelation, iteration, tests


class LeafRelationTestCase(unittest.TestCase):
    """Tests for LeafRelation and closely-related iteration-engine methods."""

    def setUp(self) -> None:
        self.a = tests.ColumnTag("a")
        self.b = tests.ColumnTag("b")

    def test_iteration_make_leaf(self) -> None:
        """Test the iteration engine's `make_leaf` function, and with it
        the dataclass field definitions for `LeafRelation` itself`.
        """
        engine = iteration.Engine()
        columns = {self.a, self.b}
        sequence_payload = iteration.RowSequence(
            [{self.a: 0, self.b: 0}, {self.a: 0, self.b: 1}, {self.a: 1, self.b: 0}, {self.a: 0, self.b: 0}]
        )
        mapping_payload = sequence_payload.to_mapping((self.a, self.b))
        sequence_leaf = engine.make_leaf(columns, payload=sequence_payload)
        self.assertEqual(sequence_leaf.engine, engine)
        self.assertEqual(sequence_leaf.columns, columns)
        self.assertEqual(sequence_leaf.min_rows, 4)
        self.assertEqual(sequence_leaf.max_rows, 4)
        self.assertTrue(sequence_leaf.is_locked)
        self.assertFalse(sequence_leaf.is_join_identity)
        self.assertFalse(sequence_leaf.is_trivial)
        self.assertIsNone(sequence_leaf.parameters)
        self.assertTrue(sequence_leaf.name.startswith("leaf"))
        self.assertCountEqual(sequence_leaf.payload, sequence_payload)
        mapping_leaf = engine.make_leaf(columns=columns, payload=mapping_payload)
        self.assertEqual(mapping_leaf.engine, engine)
        self.assertEqual(mapping_leaf.columns, columns)
        self.assertEqual(mapping_leaf.min_rows, 3)
        self.assertEqual(mapping_leaf.max_rows, 3)
        self.assertTrue(mapping_leaf.is_locked)
        self.assertFalse(mapping_leaf.is_join_identity)
        self.assertFalse(mapping_leaf.is_trivial)
        self.assertIsNone(mapping_leaf.parameters)
        self.assertTrue(mapping_leaf.name.startswith("leaf"))
        self.assertCountEqual(mapping_leaf.payload, mapping_payload)
        self.assertNotEqual(mapping_leaf.name, sequence_leaf.name)
        self.assertNotEqual(mapping_leaf, sequence_leaf)

    def test_join_identity(self) -> None:
        """Test `LeafRelation.make_join_identity and the iteration engine's
        get_join_identity_payload."""
        engine = iteration.Engine()
        join_identity_leaf = LeafRelation.make_join_identity(engine)
        self.assertEqual(join_identity_leaf, LeafRelation.make_join_identity(engine))
        self.assertEqual(join_identity_leaf.engine, engine)
        self.assertEqual(join_identity_leaf.columns, set())
        self.assertEqual(join_identity_leaf.min_rows, 1)
        self.assertEqual(join_identity_leaf.max_rows, 1)
        self.assertTrue(join_identity_leaf.is_locked)
        self.assertTrue(join_identity_leaf.is_join_identity)
        self.assertTrue(join_identity_leaf.is_trivial)
        self.assertIsNone(join_identity_leaf.parameters)
        self.assertEqual(join_identity_leaf.name, "I")
        self.assertCountEqual(join_identity_leaf.payload, [{}])

    def test_doomed(self) -> None:
        """Test `LeafRelation.make_doomed and the iteration engine's
        get_doomed_payload."""
        engine = iteration.Engine()
        doomed_leaf = LeafRelation.make_doomed(engine, {self.a, self.b}, ["doomed 1"])
        self.assertEqual(
            doomed_leaf,
            LeafRelation(
                engine,
                frozenset({self.a, self.b}),
                [],
                messages=["doomed 1"],
                name="0",
                min_rows=0,
                max_rows=0,
            ),
        )
        # Messages not important for equality.
        self.assertEqual(doomed_leaf, LeafRelation.make_doomed(engine, {self.a, self.b}, ["doomed 2"]))
        # Columns are important for equality.
        self.assertNotEqual(doomed_leaf, LeafRelation.make_doomed(engine, {self.a}, ["doomed 1"]))
        # Name is important for equality.
        self.assertNotEqual(
            doomed_leaf, LeafRelation.make_doomed(engine, {self.a, self.b}, ["doomed 1"], name="c")
        )
        self.assertEqual(doomed_leaf.engine, engine)
        self.assertEqual(doomed_leaf.columns, {self.a, self.b})
        self.assertEqual(doomed_leaf.min_rows, 0)
        self.assertEqual(doomed_leaf.max_rows, 0)
        self.assertTrue(doomed_leaf.is_locked)
        self.assertFalse(doomed_leaf.is_join_identity)
        self.assertTrue(doomed_leaf.is_trivial)
        self.assertIsNone(doomed_leaf.parameters, None)
        self.assertEqual(doomed_leaf.name, "0")
        self.assertCountEqual(doomed_leaf.payload, [])

    def test_bad_min_max_rows(self) -> None:
        """Test construction checks for inconsistent min_rows / max_rows."""
        engine = iteration.Engine()
        with self.assertRaises(ValueError):
            LeafRelation(engine, frozenset({self.a, self.b}), payload=..., min_rows=2, max_rows=1)

    def test_str(self) -> None:
        """Test str(LeafRelation)."""
        engine = iteration.Engine()
        self.assertEqual(
            str(LeafRelation(engine, frozenset({self.a, self.b}), payload=..., name="leaf1")), "leaf1"
        )
        self.assertEqual(
            str(
                LeafRelation(
                    engine, frozenset({self.a, self.b}), payload=..., name="leaf2", parameters=[1, 2]
                )
            ),
            "leaf2([1, 2])",
        )


if __name__ == "__main__":
    unittest.main()
