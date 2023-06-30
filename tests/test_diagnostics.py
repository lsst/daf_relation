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
from collections.abc import Set
from typing import Any

from lsst.daf.relation import (
    ColumnTag,
    Diagnostics,
    GenericConcreteEngine,
    LeafRelation,
    Predicate,
    Relation,
    tests,
)


class EmptyLookupEngine(GenericConcreteEngine[str]):
    """A toy Engine for testing Diagnostics."""

    def __init__(self) -> None:
        self.empty: set[Relation] = set()

    def get_join_identity_payload(self) -> str:
        return "I"

    def get_doomed_payload(self, columns: Set[ColumnTag]) -> str:
        return "0"

    def make_leaf(self, name: str, *columns: ColumnTag, **kwargs: Any) -> Relation:
        return LeafRelation(self, frozenset(columns), name=name, payload=name, **kwargs)

    def is_relation_nonempty(self, relation: Relation) -> bool:
        return relation not in self.empty and relation.max_rows != 0


class DiagnosticsTestCase(tests.RelationTestCase):
    """Tests for the Diagnostics class."""

    def setUp(self) -> None:
        self.maxDiff = None

    def test_static_leaf(self) -> None:
        """Test Diagnostics on LeafRelations with max_rows=0 and
        empty-invariant operations acting on them.
        """
        engine = EmptyLookupEngine()
        leaf = engine.make_leaf("leaf", max_rows=0)
        self.assertEqual(
            Diagnostics.run(leaf),
            Diagnostics(
                is_doomed=True,
                messages=["Relation 'leaf' has no rows (static)."],
            ),
        )
        self.assertEqual(
            Diagnostics.run(leaf, engine.is_relation_nonempty),
            Diagnostics(
                is_doomed=True,
                messages=["Relation 'leaf' has no rows (static)."],
            ),
        )
        self.assertEqual(
            Diagnostics.run(leaf.without_duplicates(), engine.is_relation_nonempty),
            Diagnostics(
                is_doomed=True,
                messages=["Relation 'leaf' has no rows (static)."],
            ),
        )

    def test_executed_leaf(self) -> None:
        """Test Diagnostics on LeafRelations with max_rows != 0 and
        empty-invariant operations acting on them.
        """
        engine = EmptyLookupEngine()
        leaf = engine.make_leaf("leaf")
        self.assertEqual(
            Diagnostics.run(leaf),
            Diagnostics(
                is_doomed=False,
                messages=[],
            ),
        )
        self.assertEqual(
            Diagnostics.run(leaf.without_duplicates()),
            Diagnostics(
                is_doomed=False,
                messages=[],
            ),
        )
        engine.empty.add(leaf)
        self.assertEqual(
            Diagnostics.run(leaf, engine.is_relation_nonempty),
            Diagnostics(
                is_doomed=True,
                messages=["Relation 'leaf' has no rows (executed)."],
            ),
        )
        self.assertEqual(
            Diagnostics.run(leaf.without_duplicates(), engine.is_relation_nonempty),
            Diagnostics(
                is_doomed=True,
                messages=["Relation 'leaf' has no rows (executed)."],
            ),
        )

    def test_slice(self) -> None:
        """Test Diagnostics on Slice operations."""
        engine = EmptyLookupEngine()
        leaf = engine.make_leaf("leaf")
        self.assertEqual(
            Diagnostics.run(leaf[2:2]),
            Diagnostics(
                is_doomed=True,
                messages=["Slice with limit=0 applied to 'leaf'"],
            ),
        )
        sliced = leaf[1:3]
        self.assertEqual(
            Diagnostics.run(sliced),
            Diagnostics(
                is_doomed=False,
                messages=[],
            ),
        )
        engine.empty.add(sliced)
        self.assertEqual(
            Diagnostics.run(sliced, engine.is_relation_nonempty),
            Diagnostics(
                is_doomed=True,
                messages=["Operation slice[1:3] yields no results when applied to 'leaf'"],
            ),
        )

    def test_selection(self) -> None:
        """Test Diagnostics on Selection operations."""
        engine = EmptyLookupEngine()
        a = tests.ColumnTag("a")
        leaf = engine.make_leaf("leaf", a)
        self.assertEqual(
            Diagnostics.run(leaf.with_rows_satisfying(Predicate.literal(False))),
            Diagnostics(
                is_doomed=True,
                messages=["Predicate 'False' is trivially false (applied to 'leaf')"],
            ),
        )
        selected = leaf.with_rows_satisfying(Predicate.reference(a))
        self.assertEqual(
            Diagnostics.run(selected),
            Diagnostics(
                is_doomed=False,
                messages=[],
            ),
        )
        engine.empty.add(selected)
        self.assertEqual(
            Diagnostics.run(selected, engine.is_relation_nonempty),
            Diagnostics(
                is_doomed=True,
                messages=["Operation σ[a] yields no results when applied to 'leaf'"],
            ),
        )

    def test_chain(self) -> None:
        """Test Diagnostics on Chain operations."""
        engine = EmptyLookupEngine()
        a = tests.ColumnTag("a")
        leaf1 = engine.make_leaf("leaf1", a)
        leaf2 = engine.make_leaf("leaf2", a)
        leaf3 = engine.make_leaf("leaf3", a, max_rows=0)
        leaf4 = engine.make_leaf("leaf4", a, max_rows=0)
        self.assertEqual(
            Diagnostics.run(leaf1.chain(leaf2)),
            Diagnostics(
                is_doomed=False,
                messages=[],
            ),
        )
        self.assertEqual(
            Diagnostics.run(leaf1.chain(leaf3)),
            Diagnostics(
                is_doomed=False,
                messages=["Relation 'leaf3' has no rows (static)."],
            ),
        )
        self.assertEqual(
            Diagnostics.run(leaf3.chain(leaf1)),
            Diagnostics(
                is_doomed=False,
                messages=["Relation 'leaf3' has no rows (static)."],
            ),
        )
        self.assertEqual(
            Diagnostics.run(leaf3.chain(leaf4)),
            Diagnostics(
                is_doomed=True,
                messages=[
                    "Relation 'leaf3' has no rows (static).",
                    "Relation 'leaf4' has no rows (static).",
                ],
            ),
        )
        engine.empty.add(leaf1)
        self.assertEqual(
            Diagnostics.run(leaf1.chain(leaf2), engine.is_relation_nonempty),
            Diagnostics(
                is_doomed=False,
                messages=["Relation 'leaf1' has no rows (executed)."],
            ),
        )
        self.assertEqual(
            Diagnostics.run(leaf1.chain(leaf3), engine.is_relation_nonempty),
            Diagnostics(
                is_doomed=True,
                messages=[
                    "Relation 'leaf1' has no rows (executed).",
                    "Relation 'leaf3' has no rows (static).",
                ],
            ),
        )
        self.assertEqual(
            Diagnostics.run(leaf3.chain(leaf1), engine.is_relation_nonempty),
            Diagnostics(
                is_doomed=True,
                messages=[
                    "Relation 'leaf3' has no rows (static).",
                    "Relation 'leaf1' has no rows (executed).",
                ],
            ),
        )
        engine.empty.add(leaf2)
        self.assertEqual(
            Diagnostics.run(leaf1.chain(leaf2), engine.is_relation_nonempty),
            Diagnostics(
                is_doomed=True,
                messages=[
                    "Relation 'leaf1' has no rows (executed).",
                    "Relation 'leaf2' has no rows (executed).",
                ],
            ),
        )

    def test_join(self) -> None:
        """Test Diagnostics on Join operations."""
        engine = EmptyLookupEngine()
        a = tests.ColumnTag("a")
        b = tests.ColumnTag("b")
        c = tests.ColumnTag("c")
        leaf1 = engine.make_leaf("leaf1", a, b)
        leaf2 = engine.make_leaf("leaf2", a, c)
        leaf3 = engine.make_leaf("leaf3", a, b, max_rows=0)
        leaf4 = engine.make_leaf("leaf4", a, c, max_rows=0)
        self.assertEqual(
            Diagnostics.run(leaf1.join(leaf2)),
            Diagnostics(
                is_doomed=False,
                messages=[],
            ),
        )
        self.assertEqual(
            Diagnostics.run(leaf1.join(leaf4)),
            Diagnostics(
                is_doomed=True,
                messages=["Relation 'leaf4' has no rows (static)."],
            ),
        )
        self.assertEqual(
            Diagnostics.run(leaf3.join(leaf2)),
            Diagnostics(
                is_doomed=True,
                messages=["Relation 'leaf3' has no rows (static)."],
            ),
        )
        self.assertEqual(
            Diagnostics.run(leaf3.join(leaf4)),
            Diagnostics(
                is_doomed=True,
                messages=[
                    "Relation 'leaf3' has no rows (static).",
                    "Relation 'leaf4' has no rows (static).",
                ],
            ),
        )

        engine.empty.add(leaf1)
        self.assertEqual(
            Diagnostics.run(leaf1.join(leaf2), engine.is_relation_nonempty),
            Diagnostics(
                is_doomed=True,
                messages=["Relation 'leaf1' has no rows (executed)."],
            ),
        )
        self.assertEqual(
            Diagnostics.run(leaf1.join(leaf4), engine.is_relation_nonempty),
            Diagnostics(
                is_doomed=True,
                messages=[
                    "Relation 'leaf1' has no rows (executed).",
                    "Relation 'leaf4' has no rows (static).",
                ],
            ),
        )
        engine.empty.add(leaf2)
        self.assertEqual(
            Diagnostics.run(leaf1.join(leaf2), engine.is_relation_nonempty),
            Diagnostics(
                is_doomed=True,
                messages=[
                    "Relation 'leaf1' has no rows (executed).",
                    "Relation 'leaf2' has no rows (executed).",
                ],
            ),
        )
        engine.empty.clear()
        self.assertEqual(
            Diagnostics.run(leaf1.join(leaf2, Predicate.literal(False))),
            Diagnostics(
                is_doomed=True,
                messages=["Join predicate 'False' is trivially false in 'leaf1 ⋈ leaf2'."],
            ),
        )
        joined = leaf1.join(leaf2)
        engine.empty.add(joined)
        self.assertEqual(
            Diagnostics.run(joined, engine.is_relation_nonempty),
            Diagnostics(
                is_doomed=True,
                messages=["Operation ⋈ yields no results when executed: 'leaf1 ⋈ leaf2'"],
            ),
        )


if __name__ == "__main__":
    unittest.main()
