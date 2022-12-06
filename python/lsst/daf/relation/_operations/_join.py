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

__all__ = ("Join", "PartialJoin")

import dataclasses
from collections.abc import Set
from typing import TYPE_CHECKING, final

from .._binary_operation import BinaryOperation, IgnoreOne
from .._columns import ColumnTag, Predicate
from .._exceptions import ColumnError, EngineError
from .._operation_relations import UnaryOperationRelation
from .._unary_operation import UnaryCommutator, UnaryOperation

if TYPE_CHECKING:
    from .._engine import Engine
    from .._relation import Relation


@final
@dataclasses.dataclass(frozen=True)
class Join(BinaryOperation):
    """A natural join operation.

    A natural join combines two relations by matching rows with the same values
    in their common columns (and satisfying an optional column expression, via
    a `Predicate`), producing a new relation whose columns are the union of the
    columns of its operands.  This is equivalent to [``INNER``] ``JOIN`` in
    SQL.
    """

    predicate: Predicate = dataclasses.field(default_factory=lambda: Predicate.literal(True))
    """A boolean expression that must evaluate to true for any matched rows
    (`Predicate`).

    This does not include the equality constraint on `common_columns`.
    """

    min_columns: frozenset[ColumnTag] = dataclasses.field(default=frozenset())
    """The minimal set of columns that should be used in the equality
    constraint on `common_columns` (`frozenset` [ `ColumnTag` ]).

    If the relations this operation is applied to have common columsn that are
    not a superset of this set, `ColumnError` will be raised by `apply`.

    This is guaranteed to be equal to `max_columns` on any `Join` instance
    attached to a `BinaryOperationRelation` by `apply`.
    """

    max_columns: frozenset[ColumnTag] | None = dataclasses.field(default=None)
    """The maximal set of columns that should be used in the equality
    constraint on `common_columns` (`frozenset` [ `ColumnTag` ] or
    ``None``).

    If the relations this operation is applied to have more columns in common
    than this set, they will not be included in the equality constraint.

    This is guaranteed to be equal to `min_columns` on any `Join` instance
    attached to a `BinaryOperationRelation` by `apply`.
    """

    def __post_init__(self) -> None:
        if self.max_columns is not None and not self.min_columns <= self.max_columns:
            raise ColumnError(
                f"Join min_columns={self.min_columns} is not a subset of max_columns={self.max_columns}."
            )

    @property
    def common_columns(self) -> frozenset[ColumnTag]:
        """The common columns between relations that will be used as an
        equality constraint (`~collections.abc.Set` [ `ColumnTag` ]).

        This attribute is not available on `Join` instances for which
        `min_columns` is not the same as `max_columns`.  It is always available
        on any `Join` instance attached to a `BinaryOperationRelation` by
        `apply`.
        """
        if self.max_columns == self.min_columns:
            return self.min_columns
        else:
            raise ColumnError(f"Common columns for join {self} have not been resolved.")

    def __str__(self) -> str:
        return "⋈"

    def _begin_apply(self, lhs: Relation, rhs: Relation) -> BinaryOperation:
        # Docstring inherited.
        if not self.predicate.columns_required <= self.applied_columns(lhs, rhs):
            raise ColumnError(
                f"Missing columns {set(self.predicate.columns_required - self.applied_columns(lhs, rhs))} "
                f"for join between {lhs!r} and {rhs!r} with predicate {self.predicate}."
            )
        if self.max_columns != self.min_columns:
            common_columns = self.applied_common_columns(lhs, rhs)
            operation = dataclasses.replace(self, min_columns=common_columns, max_columns=common_columns)
        else:
            if not lhs.columns >= self.common_columns:
                raise ColumnError(
                    f"Missing columns {set(self.common_columns - lhs.columns)} "
                    f"for left-hand side of join between {lhs!r} and {rhs!r}."
                )
            if not rhs.columns >= self.common_columns:
                raise ColumnError(
                    f"Missing columns {set(self.common_columns - rhs.columns)} "
                    f"for right-hand side of join between {lhs!r} and {rhs!r}."
                )
            operation = self
        if lhs.is_join_identity:
            return IgnoreOne(True)
        if rhs.is_join_identity:
            return IgnoreOne(False)
        return operation

    def _finish_apply(self, lhs: Relation, rhs: Relation) -> Relation:
        # Docstring inherited.
        if lhs.is_join_identity:
            return rhs
        if rhs.is_join_identity:
            return lhs
        if lhs.engine != rhs.engine:
            raise EngineError(f"Mismatched join engines: {lhs.engine} != {rhs.engine}.")
        if not self.predicate.is_supported_by(lhs.engine):
            raise EngineError(f"Join predicate {self.predicate} does not support engine {lhs.engine}.")
        return super()._finish_apply(lhs, rhs)

    def applied_columns(self, lhs: Relation, rhs: Relation) -> Set[ColumnTag]:
        # Docstring inherited.
        return lhs.columns | rhs.columns

    def applied_min_rows(self, lhs: Relation, rhs: Relation) -> int:
        # Docstring inherited.
        return 0

    def applied_max_rows(self, lhs: Relation, rhs: Relation) -> int | None:
        # Docstring inherited.
        if lhs.max_rows == 0 or rhs.max_rows == 0:
            return 0
        if lhs.max_rows is None or rhs.max_rows is None:
            return None
        else:
            return lhs.max_rows * rhs.max_rows

    def applied_common_columns(self, lhs: Relation, rhs: Relation) -> frozenset[ColumnTag]:
        """Compute the actual common columns for a `Join` given its targets.

        Parameters
        ----------
        lhs : `Relation`
            One relation to join.
        rhs : `Relation`
            The other relation to join to ``lhs``.

        Returns
        -------
        common_columns : `~collections.abc.Set` [ `ColumnTag` ]
            Columns that are included in all of ``lhs.columns`` and
            ``rhs.columns`` and `max_columns`, checked to be a superset of
            `min_columns`.

        Raises
        ------
        ColumnError
            Raised if the result would not be a superset of `min_columns`.
        """
        # Docstring inherited.
        if self.max_columns != self.min_columns:
            common_columns = {tag for tag in lhs.columns & rhs.columns if tag.is_key}
            if self.max_columns is not None:
                common_columns &= self.max_columns
            if not (common_columns >= self.min_columns):
                raise ColumnError(
                    f"Common columns {common_columns} for join between {lhs} and {rhs} are not a superset "
                    f"of the minimum columns {self.min_columns}."
                )
            return frozenset(common_columns)
        else:
            return self.min_columns

    def partial(self, fix: Relation, is_lhs: bool = False) -> PartialJoin:
        """Return a `UnaryOperation` that represents this join with one operand
        already provided and held fixed.

        Parameters
        ----------
        fix : `Relation`
            Relation to include in the returned unary operation.
        is_lhs : `bool`, optional
            Whether ``fix`` should be considered the ``lhs`` or ``rhs`` side of
            the join (`Join` side is *usually* irrelevant, but `Engine`
            implementations are permitted to make additional guarantees about
            row order or duplicates based on them).

        Returns
        -------
        partial_join : `PartialJoin`
            Unary operation representing a join to a fixed relation.

        Raises
        ------
        ColumnError
            Raised if the given predicate requires columns not present in
            ``lhs`` or ``rhs``.
        RowOrderError
            Raised if ``lhs`` or ``rhs`` is unnecessarily ordered; see
            `Relation.expect_unordered`.

        Notes
        -----
        This method and the class it returns are called "partial" in the spirit
        of `functools.partial`: a callable formed by holding some arguments to
        another callable fixed.
        """
        if not (self.min_columns <= fix.columns):
            raise ColumnError(
                f"Missing columns {set(self.min_columns - fix.columns)} for partial join to {fix}."
            )
        return PartialJoin(self, fix, is_lhs)


@final
@dataclasses.dataclass(frozen=True)
class PartialJoin(UnaryOperation):
    """A `UnaryOperation` that represents this join with one operand already
    provided and held fixed.

    Notes
    -----
    This class and the `Join.partial` used to construct it are called "partial"
    in the spirit of `functools.partial`: a callable formed by holding some
    arguments to another callable fixed.

    `PartialJoin` instances never appear in relation trees; the `apply` method
    will return a `BinaryOperationRelation` with a `Join` operation instead of
    a `UnaryOperationRelation` with a `PartialJoin` (or one of the operands, if
    the other is a `join identity relation <Relation.is_join_identity>`).
    """

    binary: Join
    """The join operation (`Join`) to be applied.
    """

    fixed: Relation
    """The target relation already included in the operation (`Relation`).
    """

    fixed_is_lhs: bool
    """Whether `fixed` should be considered the ``lhs`` or ``rhs`` side of
    the join.

    `Join` side is *usually* irrelevant, but `Engine` implementations are
    permitted to make additional guarantees about row order or duplicates based
    on them.
    """

    @property
    def columns_required(self) -> Set[ColumnTag]:
        # Docstring inherited.
        result = set(self.binary.predicate.columns_required)
        result.difference_update(self.fixed.columns)
        result.update(self.binary.min_columns)
        return result

    @property
    def is_empty_invariant(self) -> bool:
        # Docstring inherited.
        return False

    @property
    def is_count_invariant(self) -> bool:
        # Docstring inherited.
        return False

    def __str__(self) -> str:
        return f"{self.binary!s}[{self.fixed!s}]"

    def _begin_apply(
        self, target: Relation, preferred_engine: Engine | None
    ) -> tuple[UnaryOperation, Engine]:
        # Docstring inherited.
        if self.binary.max_columns != self.binary.min_columns:
            common_columns = self.binary.applied_common_columns(self.fixed, target)
            replacement = dataclasses.replace(
                self,
                binary=dataclasses.replace(
                    self.binary, min_columns=common_columns, max_columns=common_columns
                ),
            )
            return replacement._begin_apply(target, preferred_engine)
        if preferred_engine is None:
            preferred_engine = self.fixed.engine
        if not self.columns_required <= target.columns:
            raise ColumnError(
                f"Join {self} to relation {target} needs columns "
                f"{set(self.columns_required) - target.columns}."
            )
        return super()._begin_apply(target, preferred_engine)

    def _finish_apply(self, target: Relation) -> Relation:
        # Docstring inherited.
        if self.fixed_is_lhs:
            return self.binary.apply(self.fixed, target)
        else:
            return self.binary.apply(target, self.fixed)

    def applied_columns(self, target: Relation) -> Set[ColumnTag]:
        # Docstring inherited.
        if self.fixed_is_lhs:
            return self.binary.applied_columns(self.fixed, target)
        else:
            return self.binary.applied_columns(target, self.fixed)

    def applied_min_rows(self, target: Relation) -> int:
        # Docstring inherited.
        if self.fixed_is_lhs:
            return self.binary.applied_min_rows(self.fixed, target)
        else:
            return self.binary.applied_min_rows(target, self.fixed)

    def applied_max_rows(self, target: Relation) -> int | None:
        # Docstring inherited.
        if self.fixed_is_lhs:
            return self.binary.applied_max_rows(self.fixed, target)
        else:
            return self.binary.applied_max_rows(target, self.fixed)

    def commute(self, current: UnaryOperationRelation) -> UnaryCommutator:
        # Docstring inherited.
        from ._deduplication import Deduplication
        from ._projection import Projection

        match current.operation:
            case Deduplication():
                # A Join only commutes past Deduplication if the fixed relation
                # has unique rows, which is not something we can check right
                # now.
                return UnaryCommutator(
                    first=None,
                    second=current.operation,
                    done=False,
                    messages=("join-deduplication commutation is not supported",),
                )
            case Projection():
                # In order for projection(join(target)) to be equivalent to
                # join(projection(target)), the new outer projection has to
                # include the columns added by the join.  Note that because we
                # require common_columns to be explicit at this point, the
                # projection cannot change them.
                return UnaryCommutator(
                    first=self,
                    second=Projection(frozenset(self.applied_columns(current))),
                )
            case _:
                if not self.columns_required <= current.target.columns:
                    return UnaryCommutator(
                        first=None,
                        second=current.operation,
                        done=False,
                        messages=(
                            f"{current.target} is missing columns "
                            f"{set(self.columns_required - current.target.columns)}",
                        ),
                    )
                if current.operation.is_count_dependent:
                    return UnaryCommutator(
                        first=None,
                        second=current.operation,
                        done=False,
                        messages=(f"{current.operation} is count-dependent",),
                    )
                return UnaryCommutator(first=self, second=current.operation)
