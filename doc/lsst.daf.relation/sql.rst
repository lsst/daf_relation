.. py:currentmodule:: lsst.daf.relation.sql

.. _lsst.daf.relation-sql:

SQLAlchemy query generation (`lsst.daf.relation.sql`)
-----------------------------------------------------

Overview
""""""""

The `Engine` provided by the `lsst.daf.relation.sql` package translates `.Relation` trees to `SQLAlchemy`_ expressions.
It doesn't actually execute those expressions, however; running a SQL query and fetching its results back to a Python client is really best considered a `~.Transfer` between engines, with a SQL engine like this one as the source and an engine backed by in-memory Python objects (such as `lsst.daf.relation.iteration`) as its destination.
Multi-engine relation trees can be executed by a `.Processor` subclass.

This engine flattens back-to-back `.Join` and `.Chain` operations, and it actually reorders any combination of adjacent `.Join`, `.Selection`, `.Calculation`, and `.Projection` into a single ``SELECT...FROM...WHERE`` statement via the commutation rules for these operations.
It assumes the database query optimizer will reorder at least these operations itself anyway, so the goal is to keep the query as simple as possible to stay out of its way and aid human readers.

The `Engine.to_executable` method transforms a `.Relation` tree to a SQL ``SELECT`` or ``UNION`` thereof, as represented by `SQLAlchemy`_.
The engine's payload type is the `Payload` `~dataclasses.dataclass`, which better maps to SQL tables or very simple subqueries that can be used like tables.

Support for custom `.UnaryOperation` subclasses can be added implementing `~lsst.daf.relation.iteration.Engine.apply_custom_unary_operation`.

.. _lsst.daf.relation-sql-logical-columns:

Logical Columns
"""""""""""""""

The `Engine` and `Payload` classes are generic over a parameter we refer to as the "logical column type".
In the simplest case, the logical column type is just `sqlalchemy.sql.ColumnElement`, and this is what the default implementations of most `Engine` methods assume.
Custom subclasses of the SQL `Engine` can use other types, such as wrappers that hold one or more `sqlalchemy.sql.ColumnElement` objects, as long as they override a few `Engine` methods to handle them.
This allows one column tag and logical column in the `.Relation` representation of a query to map to multiple columns in the SQL representation.

.. _lsst.daf.relation-sql-conformation:

Operation Ordering and `Select`
"""""""""""""""""""""""""""""""

The SQL engine has a non-trivial `Engine.conform` method and overrides other methods to keep relation trees in a certain order when applying new operations.
It uses a custom `Select` marker type to partition the tree into subqueries and mark a tree as conformed.
This order attempts to avoid subqueries whenever possible by pulling `.Projection`, `.Deduplication`, `.Sort`, and `.Slice` operations downstream when commutation rules permit, both to keep the SQL output simple and to reduce the loss of row-ordering imposed by `.Sort` operations whenever possible (i.e. a `.Sort` can only be applied in the outermost query if it is to have any effect).

.. _SQLAlchemy: https://www.sqlalchemy.org/

API reference
"""""""""""""

.. automodapi:: lsst.daf.relation.sql
   :no-heading:
   :no-inheritance-diagram:
   :include-all-objects:
