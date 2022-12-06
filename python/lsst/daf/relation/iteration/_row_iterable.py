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

__all__ = (
    "CalculationRowIterable",
    "ChainRowIterable",
    "MaterializedRowIterable",
    "ProjectionRowIterable",
    "RowIterable",
    "RowMapping",
    "RowSequence",
    "SelectionRowIterable",
    "ChainRowIterable",
)

import itertools
from abc import abstractmethod
from collections.abc import Callable, Iterator, Mapping, Sequence, Set
from typing import Any

from .._columns import ColumnTag


class RowIterable:
    """An abstract base class for iterables that use mappings for rows.

    `RowIterable` is the `~.Relation.payload` type for the `.iteration` engine.
    """

    @abstractmethod
    def __iter__(self) -> Iterator[Mapping[ColumnTag, Any]]:
        raise NotImplementedError()

    def to_mapping(self, unique_key: Sequence[ColumnTag]) -> RowMapping:
        """Convert this iterable to a `RowMapping`, unless it already is one.

        Parameters
        ----------
        unique_key : `~collections.abc.Sequence` [ `ColumnTag` ]
            Sequence of columns to extract into a `tuple` to use as keys in the
            mapping, guaranteeing uniqueness over these columns.

        Returns
        -------
        rows : `RowMapping`
            A `RowIterable` backed by a mapping.
        """
        return RowMapping(unique_key, {tuple(row[k] for k in unique_key): row for row in self})

    def to_sequence(self) -> RowSequence:
        """Convert this iterable to a `RowSequence`, unless it already is one.

        Returns
        -------
        rows : `RowSequence`
            A `RowIterable` backed by a sequence.
        """
        return RowSequence(list(self))

    def materialized(self) -> MaterializedRowIterable:
        """Convert this iterable to one that holds its rows in a Python
        collection of some kind, instead of generating them lazily.

        Returns
        -------
        rows : `MaterializedRowIterable`
            A `RowIterable` that isn't lazy.
        """
        return self.to_sequence()

    def sliced(self, start: int, stop: int | None) -> RowIterable:
        """Apply a slice operation to this `RowIterable`.

        Parameters
        ----------
        start : `int`
            Start index.
        stop : `int` or `None`
            Stop index (one-past-the-end), or `None` to include up through the
            last row.

        Returns
        -------
        rows : `RowIterable`
            Iterable representing the slice.  May or may not be lazy.
        """
        return SliceRowIterable(self, start, stop)


class MaterializedRowIterable(RowIterable):
    """A `RowIterable` that is not lazy and has a known length."""

    @abstractmethod
    def __len__(self) -> int:
        raise NotImplementedError()

    def materialized(self) -> MaterializedRowIterable:
        # Docstring inherited.
        return self


class RowMapping(MaterializedRowIterable):
    """A `RowIterable` backed by a `~collections.abc.Mapping`

    Parameters
    ----------
    unique_key : `~collections.abc.Sequence` [ `ColumnTag` ]
        Sequence of columns to extract into a `tuple` to use as keys in the
        mapping, guaranteeing uniqueness over these columns.
    rows : `collections.abc.Mapping`
        Nested mapping with `tuple` keys and row values, where each row is
        (as usual for `RowIterable` types) itself a `Mapping` with `.ColumnTag`
        keys.
    """

    def __init__(self, unique_key: Sequence[ColumnTag], rows: Mapping[tuple, Mapping[ColumnTag, Any]]):
        self.rows = rows
        self.unique_key = unique_key

    def __iter__(self) -> Iterator[Mapping[ColumnTag, Any]]:
        return iter(self.rows.values())

    def __len__(self) -> int:
        return len(self.rows)

    def to_mapping(self, unique_key: Sequence[ColumnTag]) -> RowMapping:
        # Docstring inherited.
        if unique_key == self.unique_key:
            return self
        else:
            return super().to_mapping(unique_key)


class RowSequence(MaterializedRowIterable):
    """A `RowIterable` backed by a `~collections.abc.Sequence`

    Parameters
    ----------
    rows : `Mapping`
        Sequence of rows, where each row is (as usual for `RowIterable` types)
        a `Mapping` with `.ColumnTag` keys.
    """

    def __init__(self, rows: Sequence[Mapping[ColumnTag, Any]]):
        self.rows = rows

    def __iter__(self) -> Iterator[Mapping[ColumnTag, Any]]:
        return iter(self.rows)

    def __len__(self) -> int:
        return len(self.rows)

    def to_sequence(self) -> RowSequence:
        # Docstring inherited.
        return self

    def sliced(self, start: int, stop: int | None) -> RowIterable:
        # Docstring inherited.
        return RowSequence(self.rows[start:stop])


class CalculationRowIterable(RowIterable):
    """A `RowIterable` implementation that implements a calculation operation.

    Parameters
    ----------
    target : `RowIterable`
        Original iterable.
    tag : `ColumnTag`
        Key for the new column in result-row mappings.
    callable : `Callable`
        Callable that takes a single mapping argument and returns a new column
        value.
    """

    def __init__(
        self, target: RowIterable, tag: ColumnTag, callable: Callable[[Mapping[ColumnTag, Any]], Any]
    ):
        self.target = target
        self.tag = tag
        self.callable = callable

    def __iter__(self) -> Iterator[Mapping[ColumnTag, Any]]:
        return ({**row, self.tag: self.callable(row)} for row in self.target)


class ChainRowIterable(RowIterable):
    """A `RowIterable` implementation that wraps `itertools.chain`.

    Parameters
    ----------
    chain : `Sequence` [ `RowIterable` ]
        Sequence of iterables to chain together.
    """

    def __init__(self, chain: Sequence[RowIterable]):
        self.chain = chain

    def __iter__(self) -> Iterator[Mapping[ColumnTag, Any]]:
        return itertools.chain.from_iterable(self.chain)


class ProjectionRowIterable(RowIterable):
    """A `RowIterable` implementation that implements a projection operation.

    Parameters
    ----------
    target : `RowIterable`
        Original iterable to take a column subset from.
    columns : `Set`
        Columns to include in the new iterable.
    """

    def __init__(self, target: RowIterable, columns: Set[ColumnTag]):
        self.target = target
        self.columns = columns

    def __iter__(self) -> Iterator[Mapping[ColumnTag, Any]]:
        return ({k: row[k] for k in self.columns} for row in self.target)


class SelectionRowIterable(RowIterable):
    """A `RowIterable` implementation that implements a selection operation.

    Parameters
    ----------
    target : `RowIterable`
        Original iterable to filter rows from.
    callable : `Callable`
        Callable that takes a single mapping argument and returns a `bool`.
    """

    def __init__(self, target: RowIterable, callable: Callable[[Mapping[ColumnTag, Any]], bool]):
        self.target = target
        self.callable = callable

    def __iter__(self) -> Iterator[Mapping[ColumnTag, Any]]:
        return (row for row in self.target if self.callable(row))


class SliceRowIterable(RowIterable):
    """A `RowIterable` that implements a lazy `Slice` operation.

    Parameters
    ----------
    target : `RowIterable`
        Original iterable.
    start : `int`
        Start index.
    stop : `int` or `None`
        Stop index (one-past-the-end), or `None` to include up through the
        last row.
    """

    def __init__(self, target: RowIterable, start: int, stop: int | None):
        self.target = target
        self.start = start
        self.stop = stop

    def __iter__(self) -> Iterator[Mapping[ColumnTag, Any]]:
        for n, row in enumerate(self.target):
            if self.stop is not None and n == self.stop:
                return
            if n >= self.start:
                yield row
