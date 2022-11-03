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

import dataclasses
import unittest

from lsst.daf.relation import iteration


@dataclasses.dataclass(frozen=True)
class _TestColumnTag:
    name: str

    @property
    def is_key(self) -> bool:
        return True

    def __repr__(self) -> str:
        return self.name


class RelationConstructionTestCase(unittest.TestCase):
    def setUp(self):
        self.engine = iteration.Engine()
        self.a = _TestColumnTag("a")
        self.b = _TestColumnTag("b")

    def test_leaf(self) -> None:
        columns = {self.a, self.b}
        sequence_payload = iteration.RowSequence(
            [{self.a: 0, self.b: 0}, {self.a: 0, self.b: 1}, {self.a: 1, self.b: 0}, {self.a: 0, self.b: 0}]
        )
        mapping_payload = sequence_payload.to_mapping((self.a, self.b))
        sequence_leaf = self.engine.make_leaf(columns, payload=sequence_payload)
        self.assertEqual(sequence_leaf.engine, self.engine)
        self.assertEqual(sequence_leaf.columns, columns)
        self.assertEqual(sequence_leaf.min_rows, 4)
        self.assertEqual(sequence_leaf.max_rows, 4)
        self.assertCountEqual(sequence_leaf.payload, sequence_payload)
        mapping_leaf = self.engine.make_leaf(columns=columns, payload=mapping_payload)
        self.assertEqual(mapping_leaf.engine, self.engine)
        self.assertEqual(mapping_leaf.columns, columns)
        self.assertEqual(mapping_leaf.min_rows, 3)
        self.assertEqual(mapping_leaf.max_rows, 3)
        self.assertCountEqual(mapping_leaf.payload, mapping_payload)


if __name__ == "__main__":
    unittest.main()
