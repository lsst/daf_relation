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

__all__ = ("Processor",)

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from ._marker_relation import MarkerRelation
from ._materialization import Materialization
from ._operation_relations import BinaryOperationRelation, UnaryOperationRelation
from ._operations import Chain
from ._transfer import Transfer

if TYPE_CHECKING:
    from ._engine import Engine
    from ._relation import Relation


class Processor(ABC):
    """An inheritable framework for processing multi-engine relation trees.

    Individual `Engine` classes have different definitions of what it means to
    process a relation tree, and no single engine can handle a tree with
    engines.  This class provides a recursive algorithm that fills that role,
    with abstract method hooks for implementing `Transfer` and `Materialize`
    operations.

    Notes
    -----
    The `Processor` algorithm walks the tree recursively until it either finds:

    - a `Relation` with a `Relation.payload` that is not `None`, which is
      returned as-is;

    - a `Materialization` operation, for which a payload is computed via a call
      to the `materialize` hook, and then attached to both the original
      relation (modifying it in-place) and the returned one;

    - a `Transfer` operation, for which a payload is computed via a call to
      the `transfer` hook, and then the attached to the returned relation only.

    In addition, `Processor` never calls either hook on
    `trivial <Relation.is_trivial>` methods -
    `Engine.get_join_identity_payload` and `Engine.get_doomed_payload` are
    called instead.  This can (for example) avoid executing asking a database
    to execute a SQL query when the relation tree knows in advance the result
    will have no real content.  It also special-cases `Transfer` operations
    that are followed immediately by a `Materialization`, allowing both
    operations to be handled by a single call.
    """

    def process(self, relation: Relation) -> Relation:
        """Main entry point for processing a relation tree.

        Parameters
        ----------
        relation : `Relation`
            Root of the relation tree to process.  On return, relations that
            hold a `Materialization` relation will have a new
            `~Relation.payload` attached, if they did not have one already.

        Returns
        -------
        processed : `Relation`
            A version of the relation tree in which any relation with a
            `Transfer` operation has a copy of the original `Transfer` that
            has a `~Relation.payload` attached.
        """
        return self._process_recursive(relation, materialize_as=None)[0]

    @abstractmethod
    def transfer(self, source: Relation, destination: Engine, materialize_as: str | None) -> Any:
        """Hook for implementing transfers between engines.

        This method should be called only by the `Processor` base class.

        Parameters
        ----------
        source : `Relation`
            Relation to be transferred.  Any upstream `Transfer` operations in
            this tree are guaranteed to already have a `~Relation.payload`
            already attached (or some intervening relation does), so the
            relation's own engine should be capable of processing it on its
            own.
        destination : `Engine`
            Engine the relation is being transferred to.
        materialize_as : `str` or `None`
            If not `None`, the name of a `Materialization` operation that
            immediately follows the transfer being implemented, in which case
            the returned `~Relation.payload` should be appropriate for caching
            with the `Materialization`.

        Returns
        -------
        payload
            Payload for this relation in the ``destination`` engine.
        """
        raise NotImplementedError()

    @abstractmethod
    def materialize(self, target: Relation, name: str) -> Any:
        """Hook for implementing materialization operations.

        This method should be called only by the `Processor` base class.

        Parameters
        ----------
        target : `Relation`
            Relation to be materialized.  Any upstream `Transfer` operations in
            this tree are guaranteed to already have a `~Relation.payload`
            already attached (or some intervening relation does), so the
            relation's own engine should be capable of processing it on its
            own.
        name : `str`
            The name of the `Materialization` operation, to be used as needed
            in the engine-specific payload.

        Returns
        -------
        payload
            Payload for this relation that should be cached.
        """
        raise NotImplementedError()

    def _process_recursive(self, original: Relation, materialize_as: str | None) -> tuple[Relation, bool]:
        """Recursive implementation for `process`.

        Parameters
        ----------
        original : `Relation`
            Relation from the tree originally passed to `process`.
        materialize_as : `str` | `None`
            The name of the `Materialization` operation just downstream of this
            call, or `None` if the caller was not `_process_recursive` itself
            acting on a a `Materialization` operation.

        Returns
        -------
        processed : `Relation`
            Relation tree with `~Relation.payload` values attached to any
            `Transfer` operations.
        was_materialized : `bool`
            If `True`, `transfer` was called with ``materialize_as`` not
            `None`, and hence the caller (which must have been
            `_process_recursive` acting on a `Materialization` operation) does
            not need to call `materialize` to obtain a payload suitable for
            materialization.
        """
        if original.payload is not None:
            return original, True
        result: Relation
        payload: Any = None
        match original:
            case Transfer(destination=destination, target=target):
                # If the result is a trivial relation, just make a new
                # payload directly in the destination engine.
                if original.is_join_identity:
                    payload = destination.get_join_identity_payload()
                    new_target = target
                elif original.max_rows == 0:
                    payload = destination.get_doomed_payload(original.columns)
                    new_target = target
                else:
                    # Process recursively, ensuring upstream transfers
                    # and materializations happen first.
                    new_target, _ = self._process_recursive(target, materialize_as=None)
                    # Actually execute the transfer.  If materialize_as
                    # is not None, this will also take care of an
                    # immediately-downstream Materialization.
                    payload = self.transfer(new_target, destination, materialize_as)
                # We need to attach this payload to the processed
                # relation we return, but we don't want to attach it to
                # the original, so we reapply the transfer operation to
                # new_target even if new_target is target.
                result = original.reapply(new_target, payload)
                return result, materialize_as is not None
            case Materialization(name=name, target=target):
                assert name is not None, "Guaranteed by Materialization.apply."
                # Process recursively, ensuring upstream transfers and
                # materializations happen first.  Pass name as
                # materialize_as to tell an immediately-upstream
                # transfer to materialize directly.
                new_target, persisted = self._process_recursive(target, materialize_as=name)
                if new_target is not target:
                    result = new_target.materialized(name=name)
                    if result.payload is not None:
                        # This operation has been simplified away
                        # (perhaps it's now a materialization of a
                        # leaf).
                        original.attach_payload(result.payload)
                        return result, True
                else:
                    result = original
                if persisted:
                    payload = new_target.payload
                elif original.is_join_identity:
                    payload = target.engine.get_join_identity_payload()
                elif original.max_rows == 0:
                    payload = target.engine.get_doomed_payload(original.columns)
                else:
                    payload = self.materialize(new_target, name)
                # Attach the payload to the original relation, not just
                # the processed one, so it's used every time that the
                # original relation tree is processed.
                original.attach_payload(payload)
                if result is not original:
                    result.attach_payload(payload)
                return result, True
            case MarkerRelation(target=target):
                new_target, persisted = self._process_recursive(target, materialize_as=materialize_as)
                return original.reapply(new_target), persisted
            case UnaryOperationRelation(operation=operation, target=target):
                new_target, _ = self._process_recursive(target, materialize_as=None)
                if new_target is not target:
                    return operation.apply(new_target), False
                else:
                    return original, False
            case BinaryOperationRelation(operation=operation, lhs=lhs, rhs=rhs):
                new_lhs, lhs_persisted = self._process_recursive(lhs, materialize_as=None)
                new_rhs, rhs_persisted = self._process_recursive(rhs, materialize_as=None)
                if isinstance(operation, Chain):
                    # Simplify out relations with no rows from unions to save
                    # engines from having to handle those do-nothing branches.
                    # We don't do that earlier to the original tree usually
                    # because this is useful diagnostic information.
                    if new_lhs.max_rows == 0:
                        return new_rhs, rhs_persisted
                    if new_rhs.max_rows == 0:
                        return new_lhs, lhs_persisted
                if new_lhs is not lhs or new_rhs is not rhs:
                    return operation.apply(new_lhs, new_rhs), False
                return original, False
        raise AssertionError("Match should be exhaustive and all branches should return.")
