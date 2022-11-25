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

__all__ = ("Engine",)

import dataclasses
import itertools
from collections.abc import Callable, Container, Mapping, Sequence, Set
from operator import attrgetter, itemgetter
from typing import Any

from .._columns import (
    ColumnContainer,
    ColumnExpression,
    ColumnExpressionSequence,
    ColumnFunction,
    ColumnInContainer,
    ColumnLiteral,
    ColumnRangeLiteral,
    ColumnReference,
    ColumnTag,
    LogicalAnd,
    LogicalNot,
    LogicalOr,
    Predicate,
    PredicateFunction,
    PredicateLiteral,
    PredicateReference,
)
from .._engine import Engine as BaseEngine
from .._engine import GenericConcreteEngine
from .._exceptions import EngineError
from .._leaf_relation import LeafRelation
from .._marker_relation import MarkerRelation
from .._materialization import Materialization
from .._operation_relations import BinaryOperationRelation, UnaryOperationRelation
from .._operations import Calculation, Chain, Deduplication, Join, Projection, Selection, Slice, Sort
from .._relation import Relation
from .._transfer import Transfer
from .._unary_operation import UnaryOperation
from ._row_iterable import (
    CalculationRowIterable,
    ChainRowIterable,
    MaterializedRowIterable,
    ProjectionRowIterable,
    RowIterable,
    RowMapping,
    RowSequence,
    SelectionRowIterable,
)


@dataclasses.dataclass(repr=False, eq=False, kw_only=True)
class Engine(GenericConcreteEngine[Callable[..., Any]]):
    """A concrete engine that treats relations as iterables with
    `~collections.abc.Mapping` rows.

    See the `.iteration` module documentation for details.
    """

    name: str = "iteration"

    def __repr__(self) -> str:
        return f"lsst.daf.relation.iteration.Engine({self.name!r})@{id(self):0x}"

    def make_leaf(
        self,
        columns: Set[ColumnTag],
        payload: MaterializedRowIterable,
        *,
        name: str = "",
        messages: Sequence[str] = (),
        name_prefix: str = "leaf",
        parameters: Any = None,
    ) -> LeafRelation:
        """Create a nontrivial leaf relation in this engine.

        This is a convenience method that simply forwards all arguments to
        the `.LeafRelation` constructor; see that class for details.
        """
        return LeafRelation(
            self,
            frozenset(columns),
            payload,
            min_rows=len(payload),
            max_rows=len(payload),
            messages=messages,
            name=name,
            name_prefix=name_prefix,
            parameters=parameters,
        )

    def get_join_identity_payload(self) -> RowIterable:
        # Docstring inherited.
        return RowMapping((), {(): {}})

    def get_doomed_payload(self, columns: Set[ColumnTag]) -> RowIterable:
        # Docstring inherited.
        return RowMapping((), {})

    def backtrack_unary(
        self, operation: UnaryOperation, tree: Relation, preferred: BaseEngine
    ) -> tuple[Relation, bool, tuple[str, ...]]:
        # Docstring inherited.
        if tree.is_locked:
            return tree, False, (f"{tree} is locked",)
        match tree:
            case UnaryOperationRelation(target=target):
                commutator = operation.commute(tree)
                if commutator.first is None:
                    return tree, commutator.done, commutator.messages
                else:
                    upstream, done, messages = self.backtrack_unary(commutator.first, target, preferred)
                    if upstream is not target:
                        result = commutator.second._finish_apply(upstream)
                    else:
                        result = tree
                    return (
                        result,
                        done and commutator.done,
                        commutator.messages + messages,
                    )
            case BinaryOperationRelation():
                return tree, False, ("backtracking through binary operations is not implemented",)
            case Transfer(target=target) as transfer:
                if target.engine == preferred:
                    return transfer.reapply(operation.apply(target)), True, ()
                else:
                    upstream, done, messages = target.engine.backtrack_unary(operation, target, preferred)
                    return (transfer.reapply(upstream), done, messages)
        raise NotImplementedError(f"Unsupported relation type {tree} for engine {self}.")

    def execute(self, relation: Relation) -> RowIterable:
        """Execute a native iteration relation, returning a Python iterable.

        Parameters
        ----------
        relation : `.Relation`
            Relation to execute.

        Returns
        -------
        rows : `RowIterable`
            Iterable over rows, with each row a mapping keyed by `.ColumnTag`.

        Notes
        -----
        This method does not in general iterate over the relation's rows; while
        some operations like `Sort` and `Deduplication` do require processing
        all rows up front (which will happen during a call to `execute`), most
        return lazy iterables that do little or nothing until actually iterated
        over.

        This method requires all relations in the tree to have the same engine
        (``self``).  Use the `.Processor` class to handle trees with multiple
        engines.
        """
        if relation.engine != self:
            raise EngineError(
                f"Engine {self!r} cannot operate on relation {relation} with engine {relation.engine!r}. "
                "Use lsst.daf.relation.process to evaluate transfers first."
            )
        if relation.max_rows == 0:
            return RowSequence([])
        if relation.is_join_identity:
            return RowSequence([{}])
        if (result := relation.payload) is not None:
            return result
        match relation:
            case UnaryOperationRelation(operation=operation, target=target):
                target_rows = self.execute(target)
                match operation:
                    case Calculation(tag=tag, expression=expression):
                        return CalculationRowIterable(
                            target_rows, tag, self.convert_column_expression(expression)
                        )
                    case Deduplication():
                        unique_key = tuple(tag for tag in relation.columns if tag.is_key)
                        return target_rows.to_mapping(unique_key)
                    case Projection(columns=columns):
                        return ProjectionRowIterable(target_rows, columns)
                    case Selection(predicate=predicate):
                        return SelectionRowIterable(target_rows, self.convert_predicate(predicate))
                    case Slice(start=start, stop=stop):
                        return target_rows.sliced(start, stop)
                    case Sort(terms=terms):
                        rows_list = list(target_rows)
                        # Python's built-in sorting methods are stable, but
                        # they don't provide a way to sort some keys in
                        # ascending order and others in descending order.  So
                        # we split the sequence of order-by terms into groups
                        # of consecutive terms with the same
                        # ascending/descending state.  At the same time, we
                        # use the visitor to transform each expression into a
                        # callable we can apply to each row.
                        grouped_by_ascending = [
                            (ascending, [self.convert_column_expression(t.expression) for t in terms])
                            for ascending, terms in itertools.groupby(terms, key=attrgetter("ascending"))
                        ]
                        # Now we can sort the full list of rows once per group,
                        # in reverse so sort terms at the start of the order-by
                        # sequence "win".
                        for ascending, callables in grouped_by_ascending[::-1]:
                            rows_list.sort(
                                key=lambda row: tuple(c(row) for c in callables),
                                reverse=not ascending,
                            )
                        return RowSequence(rows_list)
                    case _:
                        return self.apply_custom_unary_operation(operation, target)
            case BinaryOperationRelation(operation=operation, lhs=lhs, rhs=rhs):
                match operation:
                    case Chain():
                        return ChainRowIterable([self.execute(lhs), self.execute(rhs)])
                    case Join():
                        raise EngineError("Joins are not supported by the iteration engine.")
                raise EngineError(f"Custom binary operation {operation} is not supported.")
            case Materialization(target=target):
                result = self.execute(target).materialized()
                relation.attach_payload(result)
                return result
            case Transfer(destination=destination, target=target):
                if isinstance(target.engine, Engine):
                    # This is a transfer from another iteration engine
                    # (maybe a subclass).  We can handle that without
                    # requiring a Processor, and that's useful for at
                    # least unit testing (though maybe not anything
                    # else; it's not clear why you'd have a transfer
                    # between iteration engines otherwise).
                    return target.engine.execute(target)
                raise EngineError(
                    f"Engine {self!r} cannot handle transfer from "
                    f"{target.engine!r} to {destination!r}; "
                    "use `lsst.daf.relation.Processor` first to handle this operation."
                )
            case MarkerRelation(target=target):
                return self.execute(target)
        raise AssertionError("matches should be exhaustive and all branches should return")

    def convert_column_expression(
        self, expression: ColumnExpression
    ) -> Callable[[Mapping[ColumnTag, Any]], Any]:
        """Convert a `.ColumnExpression` to a Python callable.

        Parameters
        ----------
        expression : `.ColumnExpression`
            Expression to convert.

        Returns
        -------
        callable
            Callable that takes a single `~collections.abc.Mapping` argument
            (with `.ColumnTag` keys and regular Python values, representing a
            row in a relation), returning the evaluated expression as another
            regular Python value.
        """
        match expression:
            case ColumnLiteral(value=value):
                return lambda row: value
            case ColumnReference(tag=tag):
                return itemgetter(tag)
            case ColumnFunction(name=name, args=args):
                function = self.get_function(name)
                if function is not None:
                    arg_callables = [self.convert_column_expression(arg) for arg in args]
                    # MyPy doesn't see 'function' as not-None for some reason.
                    return lambda row: function(*[c(row) for c in arg_callables])  # type: ignore
                first, *rest = [self.convert_column_expression(arg) for arg in args]
                return lambda row: getattr(first(row), name)(*[r(row) for r in rest])
        raise AssertionError("matches should be exhaustive and all branches should return")

    def convert_column_container(
        self, expression: ColumnContainer
    ) -> Callable[[Mapping[ColumnTag, Any]], Container]:
        """Convert a `.ColumnContainer` to a Python callable.

        Parameters
        ----------
        expression : `.ColumnContainer`
            Expression to convert.

        Returns
        -------
        callable
            Callable that takes a single `~collections.abc.Mapping` argument
            (with `.ColumnTag` keys and regular Python values, representing a
            row in a relation), returning the evaluated expression as
            `collections.abc.Container` instance.
        """
        match expression:
            case ColumnRangeLiteral(value=value):
                return lambda row: value
            case ColumnExpressionSequence(items=items):
                item_callables = [self.convert_column_expression(item) for item in items]
                return lambda row: {c(row) for c in item_callables}
        raise AssertionError("matches should be exhaustive and all branches should return")

    def convert_predicate(self, predicate: Predicate) -> Callable[[Mapping[ColumnTag, Any]], bool]:
        """Convert a `Predicate` to a Python callable.

        Parameters
        ----------
        predicate : `Predicate`
            Expression to convert.

        Returns
        -------
        callable
            Callable that takes a single `~collections.abc.Mapping` argument
            (with `.ColumnTag` keys and regular Python values, representing a
            row in a relation), returning the evaluated expression as a `bool`.
        """
        match predicate:
            case PredicateFunction(name=name, args=args):
                function = self.get_function(name)
                if function is not None:
                    arg_callables = [self.convert_column_expression(arg) for arg in args]
                    # MyPy doesn't see 'function' as not-None in the capture
                    # for some reason.
                    return lambda row: function(*[c(row) for c in arg_callables])  # type: ignore
                first, *rest = [self.convert_column_expression(arg) for arg in args]
                return lambda row: getattr(first(row), name)(*[r(row) for r in rest])
            case LogicalAnd(operands=operands):
                operand_callables = [self.convert_predicate(arg) for arg in operands]
                return lambda row: all(c(row) for c in operand_callables)
            case LogicalOr(operands=operands):
                operand_callables = [self.convert_predicate(arg) for arg in operands]
                return lambda row: any(c(row) for c in operand_callables)
            case LogicalNot(operand=operand):
                target_callable = self.convert_predicate(operand)
                return lambda row: not target_callable(row)
            case PredicateReference(tag=tag):
                return itemgetter(tag)
            case PredicateLiteral(value=value):
                return lambda row: value
            case ColumnInContainer(item=item, container=container):
                item_callable = self.convert_column_expression(item)
                container_callable = self.convert_column_container(container)
                return lambda row: item_callable(row) in container_callable(row)
        raise AssertionError("matches should be exhaustive and all branches should return")

    def apply_custom_unary_operation(self, operation: UnaryOperation, target: Relation) -> RowIterable:
        """Convert a custom `.UnaryOperation` to a `RowIterable`.

        This method must be implemented in a subclass engine in order to
        support any custom `.UnaryOperation`.

        Parameters
        ----------
        operation : `.UnaryOperation`
            Operation to apply.  Guaranteed to be a `.Marker`, `.Reordering`,
            or `.RowFilter` subclass.
        target : `.Relation`
            Target of the unary operation.  Typically this will be passed to
            `execute` and the result used to construct a new `RowIterable`.

        Returns
        -------
        rows : `RowIterable`
            Iterable over rows, with each row a mapping keyed by `.ColumnTag`.
        """
        raise EngineError(f"Custom operation {operation} not supported by engine {self}.")
