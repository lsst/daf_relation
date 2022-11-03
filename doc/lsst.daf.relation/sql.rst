.. py:currentmodule:: lsst.daf.relation.sql

.. _lsst.daf.relation-sql:

SQLAlchemy query generation (`lsst.daf.relation.sql`)
-----------------------------------------------------

Overview
""""""""

The `Engine` provided by the `lsst.daf.relation.sql` package translates `.Relation` trees to `SQLAlchemy`_ expressions.
It doesn't actually execute those expressions, however; running a SQL query and fetching its results back to a Python client is really best considered a `~.Transfer` between engines, with a SQL engine like this one as the source and an engine backed by in-memory Python objects (such as `lsst.daf.relation.iteration`) as its destination.
Multi-engine relation trees can be executed by a `.Processor` subclass.

This engine flattens back-to-back `.Join` and `.Chain` operations, and it actually reorders any combination of adjacent `.Join`, `.Selection`, and `.Projection` into a single ``SELECT...FROM...WHERE`` statement via the commutation rules for these operations.
It assumes the database query optimizer will reorder at least these operations itself anyway, so the goal is to keep the query as simple as possible to stay out of its way and aid human readers.
Most SQL engine operations do not preserve order, since SQL does not preserve order through joins, subqueries, or unions.

The `Engine.to_executable` method transforms a `.Relation` tree to a SQL ``SELECT`` or ``UNION`` thereof, as represented by `SQLAlchemy_`.
The engine's payload type is the `SelectParts` `~dataclasses.dataclass`, which better maps to SQL tables or very simple subqueries that can be used like tables.

Support for custom `.UnaryOperation` subclasses can be added implementing `Engine.apply_custom_unary_operation`.

.. _lsst.daf.relation-sql-logical-columns:

Logical Columns
"""""""""""""""

The `Engine` and `SelectParts` classes are generic over a parameter we refer to as the "logical column type".
In the simplest case, the logical column type is just `sqlalchemy.sql.ColumnElement`, and this is what the default implementations of most `Engine` methods assume.
Custom subclasses of the SQL `Engine` can use other types, such as wrappers that hold one or more `sqlalchemy.sql.ColumnElement` objects, as long as they override a few `Engine` methods to handle them.
This allows one column tag and logical column in the `.Relation` representation of a query to map to multiple columns in the SQL representation.

.. _SQLAlchemy: https://www.sqlalchemy.org/

API reference
"""""""""""""

.. automodapi:: lsst.daf.relation.sql
   :no-heading:
   :no-inheritance-diagram:
   :include-all-objects:
