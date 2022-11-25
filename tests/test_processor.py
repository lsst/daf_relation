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
from collections.abc import Mapping, Set
from typing import Any

from lsst.daf.relation import (
    BinaryOperationRelation,
    ColumnExpression,
    ColumnTag,
    Engine,
    GenericConcreteEngine,
    LeafRelation,
    Materialization,
    Processor,
    Relation,
    SortTerm,
    Transfer,
    UnaryOperationRelation,
    tests,
)


class StringEngine(GenericConcreteEngine[str]):
    """A test Engine whose payloads are just the `str` of their relations."""

    def get_join_identity_payload(self) -> str:
        return "I"

    def get_doomed_payload(self, columns: Set[ColumnTag]) -> str:
        return "0"

    def make_leaf(self, name: str, *columns: ColumnTag, **kwargs: Any) -> Relation:
        return LeafRelation(self, frozenset(columns), name=name, payload=name, **kwargs)


class StringProcessor(Processor):
    """A test subclass of `Processor` that tracks the calls made to its hook
    methods and attaches the `str` of relations as payloads.
    """

    def __init__(self, test_case: ProcessorTestCase):
        self.test_case = test_case
        self.seen: list[str] = []

    def transfer(self, source: Relation, destination: Engine, materialize_as: str | None) -> str:
        self.test_case.check_upstream_payloads(source)
        relation = source.transferred_to(destination)
        if materialize_as is not None:
            relation = relation.materialized(materialize_as)
        result = str(relation)
        self.seen.append(result)
        return result

    def materialize(self, target: Relation, name: str) -> str:
        self.test_case.check_upstream_payloads(target)
        result = str(target.materialized(name))
        self.seen.append(result)
        return result


class ProcessorTestCase(tests.RelationTestCase):
    """Tests for the Processor class."""

    def check_upstream_payloads(
        self,
        relation: Relation,
        materializations_only: bool = False,
        upstream_of_materialization: str | None = None,
        simplifications: Mapping[str, str] | None = None,
    ) -> None:
        """Check that a relation and its upstream tree have the payloads
        that should be attached by `StringProcessor`.

        Parameters
        ----------
        relation : `Relation`
            Relation to check.
        materializations_only : `bool`, optional
            If `True`, only expect leaf and materialization relations to have
            payloads, not transfers, as expected for a tree passed to (but not
            returned by) a `Processor`.
        upstream_of_materialization : `str` | None, optional
            If not `None`, this relation is just upstream of a materialization
            with this name and needs to adjust its expected `str` accordingly
            to include that materialization.
        simplifications : `~collections.abc.Mapping` [ `str`, `str` ]
            Mappings from the original `str` of a relation subtree and the
            simplified form that should have been used to compute the payload
            by `StringProcessor`.
        """
        if simplifications is None:
            simplifications = {}
        if relation.is_join_identity:
            expected_string = "I"
        elif relation.max_rows == 0:
            expected_string = "0"
        else:
            if upstream_of_materialization is not None:
                expected_string = str(relation.materialized(upstream_of_materialization))
            else:
                expected_string = str(relation)
        expected_string = simplifications.get(expected_string, expected_string)
        match relation:
            case LeafRelation():
                self.assertIsNotNone(relation.payload)
                self.assertEqual(relation.payload, expected_string)
            case Materialization():
                self.assertIsNotNone(relation.payload)
                self.assertEqual(relation.payload, expected_string)
                self.check_upstream_payloads(
                    relation.target,
                    materializations_only=materializations_only,
                    upstream_of_materialization=relation.name,
                    simplifications=simplifications,
                )
            case Transfer():
                if materializations_only:
                    self.assertIsNone(relation.payload)
                else:
                    self.assertIsNotNone(relation.payload)
                    self.assertEqual(relation.payload, expected_string)
                self.check_upstream_payloads(
                    relation.target,
                    materializations_only=materializations_only,
                    simplifications=simplifications,
                )
            case UnaryOperationRelation():
                self.check_upstream_payloads(
                    relation.target,
                    materializations_only=materializations_only,
                    simplifications=simplifications,
                )
            case BinaryOperationRelation():
                self.check_upstream_payloads(
                    relation.lhs,
                    materializations_only=materializations_only,
                    simplifications=simplifications,
                )
                self.check_upstream_payloads(
                    relation.rhs,
                    materializations_only=materializations_only,
                    simplifications=simplifications,
                )

    def test_processor(self) -> None:
        """Test the Processor class."""
        # Cook up a three-engine relation tree with pretty some interesting
        # structure to it including some materializations; start with the
        # ingredients.
        engine1 = StringEngine(name="one")
        engine2 = StringEngine(name="two")
        engine3 = StringEngine(name="three")
        a = tests.ColumnTag("a")
        b = tests.ColumnTag("b")
        c = tests.ColumnTag("c")
        d = tests.ColumnTag("d")
        expression = ColumnExpression.reference(b).method("__neg__")
        predicate = ColumnExpression.reference(c).gt(ColumnExpression.literal(0))
        terms = [SortTerm(ColumnExpression.reference(b))]
        leaf1 = engine1.make_leaf("leaf1", a, b)
        leaf2 = engine2.make_leaf("leaf1", a, c)
        leaf3 = engine3.make_leaf("leaf3", a, b, d)
        leaf4 = engine3.make_leaf("leaf4", a, b, d)
        # Build the tree itself while taking snapshops of its str(...)
        # everywhere there's a transfer and/or materialization.
        snapshots = []
        full_tree = (
            leaf2.with_rows_satisfying(predicate).transferred_to(engine1).materialized("materialization1")
        )
        snapshots.append(str(full_tree))
        full_tree = (
            leaf1.with_calculated_column(d, expression)
            .join(full_tree)
            .with_only_columns({a, b, d})
            .transferred_to(engine3)
        )
        snapshots.append(str(full_tree))
        full_tree = full_tree.chain(leaf3).materialized("materialization2")
        snapshots.append(str(full_tree))

        # Chain the full_tree to a what's ultimately a relation with no rows.
        # The Processor will drop these operations without calling its
        # transfer() and materialize() hooks, so they won't appear in its
        # snapshots or the processed_tree it returns.
        trimmed_tree = full_tree
        full_tree = full_tree.chain(
            engine1.make_leaf("doomed_by_join", a, b)
            .join(engine1.make_doomed_relation({b, d}, ["badness"]))
            .transferred_to(engine3)
        )
        # Add some more chains, one of which will simplify before a
        # materialization.
        full_subtree = leaf4.chain(engine3.make_doomed_relation({a, b, d}, ["badness again"])).materialized(
            "materialization3"
        )
        full_tree = full_tree.chain(full_subtree)
        trimmed_tree = trimmed_tree.chain(leaf4)
        # Add a few more operations to both the full_ and trimmed_trees.
        full_tree = full_tree.without_duplicates().sorted(terms)
        trimmed_tree = trimmed_tree.without_duplicates().sorted(terms)
        # Construct and run the Processor, which itself checks some aspects
        # of the algorithm via calls to check_upstream_payloads.
        processor = StringProcessor(self)
        processed_tree = processor.process(full_tree)
        # Check that the snapshots taken by the processor match the ones we
        # took while creating the tree.
        self.assertEqual(processor.seen, snapshots)
        # Check that the processed tree has the same columns, row bounds, and
        # engine as the original.
        self.assertEqual(full_tree.columns, processed_tree.columns)
        self.assertEqual(full_tree.min_rows, processed_tree.min_rows)
        self.assertEqual(full_tree.max_rows, processed_tree.max_rows)
        # Check that the full tree now has payloads for materializations, but
        # not transfers.
        self.check_upstream_payloads(
            full_tree, materializations_only=True, simplifications={str(full_subtree): str(leaf4)}
        )
        # Check that the returned tree how has payloads for materializations
        # and transfers.
        self.check_upstream_payloads(processed_tree, materializations_only=False)
        # Check that the returned tree has the same structure as the trimmed
        # tree.
        self.assert_relations_equal(trimmed_tree, processed_tree)
        # Process the original tree again, which should short-circuit at the
        # last materializations and not call its hooks at all.
        reprocessor = StringProcessor(self)
        reprocessed_tree = reprocessor.process(full_tree)
        self.assertEqual(reprocessor.seen, [])
        self.assertEqual(full_tree.columns, reprocessed_tree.columns)
        self.assertEqual(full_tree.min_rows, reprocessed_tree.min_rows)
        self.assertEqual(full_tree.max_rows, reprocessed_tree.max_rows)


if __name__ == "__main__":
    unittest.main()
