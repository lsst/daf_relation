.. py:currentmodule:: lsst.daf.relation.iteration

.. _lsst.daf.relation-iteration:

Native iteration (`lsst.daf.relation.iteration`)
------------------------------------------------

The `.iteration` module provides a simple `Engine` intended primarily to serve as a "final transfer destination" for relation trees that are mostly defined in other engines (e.g. `~lsst.daf.relation.sql`), as a way to enable iteration over rows in Python and limited Python-side postprocessing.
That can include:

- applying predicates defined as regular Python callables;
- concatenating, sorting, and deduplicating results in memory.

The iteration engine does not currently support join operations.
The `~Engine.execute` method is the main entry point for evaluating trees of relations that are purely in this engine, transforming them into its payload type, `RowIterable`, which represents a row as a `collections.abc.Mapping` with `.ColumnTag` keys.
All operations other than `.Reordering` preserve order.

Generally, execution is lazy; operations are performed row-by-row by returning `RowIterable` (the engine's `~.Relation.payload` type) instances backed by generators, with a few exceptions:
In particular:

- `.Deduplication` operations gather all unique rows into a `dict` (inside a `RowMapping`;
- `.Sort` operations gather all rows into a `list` (inside a `RowSequence`);
- `.Materialization` operations gather all rows into a `list` unless they are already in a `dict` or `list` (via `RowIterable.materialized`);
- `.Slice` operations on a `RowSequence` are computed directly, creating another `RowSequence` (all other slices are lazy).

All other operations provided by the `lsst.daf.relation` package are guaranteed to only iterate once over their targets, and yield results row-by-row.
Custom unary operations can be supported by implementing `Engine.apply_custom_unary_operation`.

API reference
"""""""""""""

.. automodapi:: lsst.daf.relation.iteration
   :no-heading:
   :no-inheritance-diagram:
   :include-all-objects:
