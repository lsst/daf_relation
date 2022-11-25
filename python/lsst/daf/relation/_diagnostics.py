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

from __future__ import annotations

__all__ = ("Diagnostics",)

import dataclasses
from collections.abc import Callable

from ._leaf_relation import LeafRelation
from ._marker_relation import MarkerRelation
from ._operation_relations import BinaryOperationRelation, UnaryOperationRelation
from ._operations import Chain, Join, Selection, Slice
from ._relation import Relation


@dataclasses.dataclass
class Diagnostics:
    """A relation-processing algorithm that attempts to explain why a relation
    has no rows.

    The `Diagnostics` class itself is just the type returned by its `run` class
    method, which performs a depth-first tree traversal looking for relations
    that are either known in advance to have no rows (`Relation.max_rows` zero)
    or shown to have no rows via an ``executor`` callable; when present, these
    are then propagated downstream to the root.  Only operations that can
    remove all rows (`UnaryOperation.is_empty_invariant` is `False`) are
    executed when no empty leaf relations are found.x
    """

    is_doomed: bool
    """Whether the given relation will have no rows (`bool`).
    """

    messages: list[str]
    """Messages that explain why the relation will have fewer rows that it
    might have (`list` [ `str` ]).

    This is usually non-empty only when the relation is doomed, but in rare
    cases it can also provide information about what's missing when the
    relation is not doomed, such as when only one of the two branches of a
    `Chain` operation are doomed.
    """

    @classmethod
    def run(cls, relation: Relation, executor: Callable[[Relation], bool] | None = None) -> Diagnostics:
        """Report on whether the given relation has no rows, and if so, why.

        Parameters
        ----------
        relation : `Relation`
            Relation to analyze.
        executor : `Callable`, optional
            If provided, a callable that takes a `Relation` and does some
            engine-specific processing to determine whether it has any rows,
            such as a ``LIMIT 1`` query in SQL.  If not provided, diagnostics
            will be based only on relations with `Relation.max_rows` set to
            zero.

        Returns
        -------
        diagnostics : `Diagnostics`
            Struct containing the diagnostics report.
        """
        match relation:
            case LeafRelation(messages=messages):
                messages = list(messages)
                if relation.max_rows == 0:
                    if not messages:
                        messages.append(f"Relation '{relation!s}' has no rows (static).")
                    return cls(True, messages)
                elif executor is not None and not executor(relation):
                    if not messages:
                        messages.append(f"Relation '{relation!s}' has no rows (executed).")
                    return cls(True, messages)
                else:
                    return cls(False, messages)
            case MarkerRelation(target=target):
                return cls.run(target, executor)
            case UnaryOperationRelation(operation=operation, target=target):
                if (result := cls.run(target, executor)).is_doomed:
                    return result
                if relation.max_rows == 0 and operation.applied_max_rows(target) != 0:
                    result.messages.append(f"Unary operation relation {relation} has no rows (static).")
                    return cls(True, result.messages)
                match operation:
                    case Slice(limit=limit):
                        if limit == 0:
                            result.is_doomed = True
                            result.messages.append(f"Slice with limit=0 applied to '{target!s}'")
                            return result
                    case Selection(predicate=predicate):
                        if predicate.as_trivial() is False:
                            result.is_doomed = True
                            result.messages.append(
                                f"Predicate '{predicate}' is trivially false (applied to '{target!s}')"
                            )
                            return result
                if not operation.is_empty_invariant and executor is not None and not executor(relation):
                    result.is_doomed = True
                    result.messages.append(
                        f"Operation {operation} yields no results when applied to '{target!s}'"
                    )
                    return result
                return result
            case BinaryOperationRelation(operation=operation, lhs=lhs, rhs=rhs):
                lhs_result = cls.run(lhs, executor)
                rhs_result = cls.run(rhs, executor)
                messages = lhs_result.messages + rhs_result.messages
                if relation.max_rows == 0 and operation.applied_max_rows(lhs, rhs) != 0:
                    messages.append(f"Binary operation relation '{relation}' has no rows (static).")
                    return cls(True, messages)
                match operation:
                    case Chain():
                        return cls(lhs_result.is_doomed and rhs_result.is_doomed, messages)
                    case Join(predicate=predicate):
                        if lhs_result.is_doomed or rhs_result.is_doomed:
                            return cls(True, messages)
                        if predicate.as_trivial() is False:
                            messages.append(
                                f"Join predicate '{predicate}' is trivially false in '{relation}'."
                            )
                            return cls(True, messages)
                if executor is not None and not executor(relation):
                    messages.append(f"Operation {operation} yields no results when executed: '{relation!s}'")
                    return cls(True, messages)
                return cls(False, messages)
        raise AssertionError("match should be exhaustive and all branches should return")
