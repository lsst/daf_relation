# This file is part of daf_relation.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
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
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

"""Utility code to aid in writing unit tests for relations and relation
algorithms.
"""

__all__ = ("ColumnTag", "RelationTestCase", "to_sql_str")

import dataclasses
import re
import unittest
from typing import Any

try:
    from sqlalchemy.dialects.sqlite.pysqlite import dialect as sql_dialect
except ImportError:
    # MyPy doesn't like this trick.
    def sql_dialect() -> Any:  # type: ignore
        raise unittest.SkipTest("sqlalchemy SQLite dialect not available")


from ._operation_relations import BinaryOperationRelation, UnaryOperationRelation
from ._relation import Relation


def to_sql_str(sql: Any) -> str:
    """Convert a SQLAlchemy expression to a string suitable for tests.

    Parameters
    ----------
    sql
        SQLAlchemy object with a `.compile` method.

    Returns
    -------
    string : `str`
        SQL string, with all whitespace converted to single spaces.

    Notes
    -----
    This method uses the "pysqlite3" dialect, since that's the most likely one
    to be available (as it's backed by a module usually shipped with Python
    itself).

    Converting SQLAlchemy expressions to strings that are checked against
    literals is a good way to check that the `.sql` engine works as intended,
    but it's also fragile, as changes to whitespace or automatic simplification
    on the SQLAlchemy side will result in a test failure.

    Raises
    ------
    unittest.SkipTest
        Raised if `sqlalchemy` or its "pysqlite" dialect could not be imported.
    """
    return re.sub(
        r"\s+", " ", str(sql.compile(dialect=sql_dialect(), compile_kwargs={"literal_binds": True}))
    )


@dataclasses.dataclass(frozen=True)
class ColumnTag:
    """A very simple ColumnTag implementation for use in tests.

    Notes
    -----
    This class's ``__hash__`` implementation intentionally avoids the "salting"
    randomization included in the `hash` function's behavior on most built-in
    types, and it should correspond to ASCII order for single-character ASCII
    `qualified_name` values.  This means that sets of these `ColumnTag` objects
    are always deterministically ordered, and that they are *probably* sorted
    (this appears to be true, but it's really relying on CPython implementation
    details).  The former can be assumed in tests that check SQL conversions
    against expected strings (see `to_sql_str`), while the latter just makes it
    easier to write a good "expected string" on the first try.
    """

    qualified_name: str
    is_key: bool = True

    def __repr__(self) -> str:
        return self.qualified_name

    def __hash__(self) -> int:
        return int.from_bytes(self.qualified_name.encode(), byteorder="little")


def diff_relations(a: Relation, b: Relation) -> str | None:
    """Recursively compare relation trees, returning `str` representation of
    their first difference.

    Parameters
    ----------
    a : `Relation`
        Relation to compare.
    b : `Relation`
        Other relation to compare.

    Returns
    -------
    diff : `str` or `None`
        The `str` representations of the operators or relations that first
        differ when traversing the tree, formatted as '{a} != {b}'.  `None`
        if the relations are equal.
    """
    match (a, b):
        case (
            UnaryOperationRelation(operation=op_a, target=target_a),
            UnaryOperationRelation(operation=op_b, target=target_b),
        ):
            if op_a == op_b:
                return diff_relations(target_a, target_b)
            else:
                return f"{op_a} != {op_b}"
        case (
            BinaryOperationRelation(operation=op_a, lhs=lhs_a, rhs=rhs_a),
            BinaryOperationRelation(operation=op_b, lhs=lhs_b, rhs=rhs_b),
        ):
            if op_a == op_b:
                diff_lhs = diff_relations(lhs_a, lhs_b)
                diff_rhs = diff_relations(rhs_a, rhs_b)
                if diff_lhs is not None:
                    if diff_rhs is not None:
                        return f"{diff_lhs}, {diff_rhs}"
                    else:
                        return diff_lhs
                else:
                    if diff_rhs is not None:
                        return diff_rhs
                    else:
                        return None
            else:
                return f"{op_a} != {op_b}"
    if a == b:
        return None
    else:
        return f"{a} != {b}"


class RelationTestCase(unittest.TestCase):
    """An intermediate TestCase base class for relation tests."""

    def check_sql_str(self, text: str, *sql: Any) -> None:
        """Check one or more SQLAlchemy objects against the given SQL string.

        Parameters
        ----------
        text : `str`
            Expected SQL string in the pysqlite dialect, with all whitespace
            replaced by single spaces.

        *sql
            SQLAlchemy queries or column expressions to check.
        """
        for s in sql:
            self.assertEqual(to_sql_str(s), text)

    def assert_relations_equal(self, a: Relation, b: Relation) -> None:
        """Test relations for equality, reporting their difference on failure.

        Parameters
        ----------
        a : `Relation`
            Relation to compare.
        b : `Relation`
            Other relation to compare.
        """
        if diff := diff_relations(a, b):
            assert a != b, "Check that diff_relations is consistent with equality comparison."
            msg = f"{a} != {b}"
            if diff != msg:
                msg = f"{msg}:\n{diff}"
            raise self.failureException(msg)
        else:
            assert a == b, "Check that diff_relations is consistent with equality comparison."
