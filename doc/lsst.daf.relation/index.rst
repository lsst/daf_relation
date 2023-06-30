.. py:currentmodule:: lsst.daf.relation

.. _lsst.daf.relation:

#################
lsst.daf.relation
#################

.. _lsst.daf.relation-overview:

Overview
========

The `Relation` class represents the core concept of `relational algebra`_: a table with a well-defined set of columns and unique rows.
A `Relation` instance does not necessarily correspond to a concrete in-memory or on-disk table, however; most derived Relation types actually represent an operation on some other "target" relation or relations, forming an expression tree.

Operations on relations are represented by subclasses of `UnaryOperation` and `BinaryOperation`, which are associated with a target relation to form a new relation via the `UnaryOperationRelation` and `BinaryOperationRelation` classes.
The `LeafRelation` class handles relations that represent direct storage of rows (and in some cases actually do store rows themselves).
The `Relation` interface provides factory methods for constructing and applying operations to relation that should be used instead of directly interacting with operation classes when possible.

The fourth and final `Relation` class is `MarkerRelation`, which adds some information or context to a target relation without changing the relation's
actual content.
Unlike other relation types, `MarkerRelation` can be inherited from freely.

Concrete `Relation` classes (including extensions) should be frozen dataclasses, ensuring that they are immutable (with the exception of `Relation.payload`), equality comparable, hashable, and that they have a useful (but in still lossy) `repr`.
`Relation` classes also provide a `str` representation that is much more concise, for cases where seeing the overall tree is more important than the details of any particular relation.

.. _lsst.daf.relation-overview-engines:

Engines
-------

Relations are associated with "engines": systems that may hold the actual data a relation (at least) conceptually represents and can perform operations on them to obtain the derived data.
These are represented by `Engine` instances held by the relation objects themselves, and the `sql` and `iteration` subpackages provide at least partial implementations of engines for relations backed by SQL databases (via `SQLAlchemy`_) and native Python iterables, respectively.
A relation tree can include multiple engines, by using a `Transfer` relation class to mark the boundaries between them.
The `Processor` class can be used to execute multiple-engine trees.

It is up to an engine how strictly its operations adhere to relational algebra operation definition.
SQL is formally defined in terms of operations on "bags" or "multisets", whose rows are not unique and sometimes ordered, while formal relations are always unordered and unique.
The `Relation` interface has more a more permissive view of uniqueness to facilitate interaction with SQL: a `Relation` *may* have non-unique rows, but any duplicates are not meaningful, and hence most operations may remove or propagate duplicates at their discretion, though engines may make stronger guarantees and most relations are not permitted to introduce duplication when applied to a base relation with unique rows.
It is also up to engines to determine whether an operations maintains the order of rows; SQL engine operations often do not, while the iteration engine's operations always do.

`LeafRelation` and `MarkerRelation` objects can have an engine-specific `~Relation.payload` attribute that either holds the actual relation state for that engine or a reference to it.
The `iteration` engine's payloads are instances of the `iteration.RowIterable` interface, while the SQL engine's payloads are `sql.Payload` instances, which can represent a SQLAlchemy table or subquery expression.
`LeafRelation` objects always have a payload that is not `None`.
`Materialization` markers indicate that a payload should be attached when the relation is first "executed" by an engine or the `Processor` class, allowing subsequent executions to reuse that payload and avoid repeating upstream execution.
Attaching a payload is the only way a relation can be modified after construction, and a payload that is not `None` can never be replaced.

.. _lsst.daf.relation-overview-operations:

Operations
----------

The full set of unary and binary operations provided is given below, along with the `Relation` factory methods that can be used to apply certain operations directly.
Applying an operation to a relation always returns a new relation (unless the operation is a no-op, in which case the original may be returned unchanged), and always acts lazily: applying an operation is not the same as processing or executing a relation tree that contains that operation.

`Calculation` (`UnaryOperation`) / `Relation.with_calculated_column`
   Add a new column whose values are calculated from one or more existing columns, via by a `column expression <ColumnExpression>`.
`Chain` (`BinaryOperation`)  / `Relation.chain`
   Concatenate the rows of two relations that have the same columns.
   This is equivalent to ``UNION ALL`` in SQL or `itertools.chain` in Python.
`Deduplication` (`UnaryOperation`) / `Relation.without_duplicates`
   Remove duplicate rows.
   This is equivalent to ``SELECT DISTINCT`` in SQL or filtering through `set` or `dict` in Python.
`Identity` (`UnaryOperation`)
   Do nothing.
   This operation never actually appears in `Relation` trees; `Identity.apply`
   always returns the operand relation passed to it.
`Join`  (`BinaryOperation`) / `Relation.join`
   Perform a natural join: combine two relations by matching rows with the same values in their common columns (and satisfying an optional column expression, via a `Predicate`), producing a new relation whose columns are the union of the columns of its operands.
   This is equivalent to [``INNER``] ``JOIN`` in SQL.
`Projection` (`UnaryOperation`) / `Relation.with_only_columns`
   Remove some columns from a relation.
`Reordering` (`UnaryOperation`)
   An intermediate abstract base class for unary operations that only change the order of rows.
`RowFilter` (`UnaryOperation`)
   An intermediate abstract base class for unary operations that only remove rows.
`Selection` (`RowFilter`) / `Relation.with_rows_satisfying`
   Remove rows that satisfy a `boolean column expression <Predicate>`.
   This is equivalent to the ``WHERE`` clause in SQL.
`Slice` (`RowFilter`) / `Relation.__getitem__`
   Remove rows outside an integer range of indices.
   This is equivalent to ``OFFSET`` and ``LIMIT`` in SQL, or indexing with `slice` object or ``start:stop`` syntax in Python.
`Sort` (`Reordering`) / `Relation.sorted`
   Sort rows according to a `column expression <ColumnExpression>`.

.. _lsst.daf.relation-overview-column_expressions:

Column Expressions
------------------

Many relation operations involve column expressions, such as the boolean filter used in a `Selection` or the sort keys used in a `Sort`.
These are represented by the `ColumnExpression` (for general scalar-valued expressions), `Predicate` (for boolean expressions), and `ColumnContainer`  (for expressions that hold multiple values) abstract base classes.
These base classes provide factory methods for all derived classes, making it generally unnecessary to refer to those types directly (except when writing an algorithm or engine that processes a relation tree; see :ref:`lsst.daf.relation-overview-extensibility`).
Column expression objects can in general support multiple engines; some types are required to be supported by all engines, while others can hold a list of engine types that support them.
The concrete column expression classes provided by ``lsst.daf.relation`` are given below, with their factory methods:

`ColumnLiteral` / `ColumnExpression.literal`
   A constant scalar, non-boolean Python value.
`ColumnReference` / `ColumnExpression.reference`
   A reference to a scalar, non-boolean column in a relation.
`ColumnFunction` ` / `ColumnExpression.function`, `ColumnExpression.method`
   A named function that returns a scalar, non-boolean value given scalar, non-boolean arguments.
   It is up to each `Engine` how and whether it supports a `ColumnFunction`; this could include looking up the name in some module or treating it as a method that should be present on some object that represents a column value more directly in that engine.
`ColumnExpressionSequence` / `ColumnContainer.sequence`
   A sequence of one or more `ColumnExpression` objects, representing the same
   column type (but not neecessarily the same expression type.
`ColumnRangeLiteral` / `ColumnContainer.range_literal`
   A virtual sequence of literal integer values represented by a Python `range` object.
`PredicateLiteral` / `Predicate.literal`
   A constant `True` or `False` value.
`PredicateReference` / `Predicate.reference`
   A reference to a boolean-valued column in a relation.
`PredicateFunction` / `ColumnExpression.predicate_function`, `ColumnExpression.predicate_method`
   A named function that returns a boolean, given scalar, non-boolean arguments.
   Like `ColumnFunction`, implementation and support are engine-specific.
   `ColumnExpression` also has `~ColumnExpression.eq`, `~ColumnExpression.ne`,  `~ColumnExpression.lt`, `~ColumnExpression.le`, `~ColumnExpression.gt`, and `~ColumnExpression.ge` methods for the common case of `PredicateFunction`
   objects that represent comparison operators.
`LogicalNot` / `Predicate.logical_not`
   Boolean expression that is `True` if its (single, boolean) argument is `False`, and vice versa.
`LogicalAnd` / `Predicate.logical_and`
   Boolean expression that is `True` only if all (boolean) arguments are `True`.
`LogicalOr` / `Predicate.logical_or`
   Boolean expression that is `True` if any (boolean) argument is `True`.
`ColumnInContainer` / `ColumnContainer.contains`
   Boolean expression that tests whether a scalar `ColumnExpression` is included in a `ColumnContainer`.

.. _lsst.daf.relation-overview-column_tags:

Column Tags
-----------

The `ColumnTag` protocol class defines an interface for objects that represent a column in a relation (not just an expression).
This package does not provide any concrete `ColumnTag` implementations itself (outside of test code), as it is expected that libraries that use this package will instead define one or more domain-specific classes to play this role.

Relations intentionally do not support column renaming, and instead expect column tags to represent all columns in all relations in an absolute sense: if a column tag in one relation is equal to a column tag in some other relation, they are expected to mean the same thing in several ways:

- equality constraints on columns present in both relations in a join are  automatically included in that join, and which relation's values are "used" in the join relation's column is unspecified and unimportant;
- relation rows may only be `chained <Chain>` together if they have the same columns;
- `ColumnExpression` objects depend only on sets of columns via tags, and do not care which relations actually provide those columns.

It is not required that any particular engine use a `.ColumnTag` or its `~ColumnTag.qualified_name` form as its own internal identifier, though this often convenient.
For example, the provided `sql` engine allows `LeafRelation` payloads (which are
usually tables) to have arbitrary column names, but it uses the `ColumnTag.qualified_name` value for names in all SELECT queries that represent operations on those tables.

.. _lsst.daf.relation-overview-extensibility:

Extensibility
-------------

Ideally, this library would be extensible in three different ways:

- external `Relation` or operation types could be defined, representing new kinds of nodes in a relation expression tree;
- external `Engine` types could be defined, representing different ways of storing and manipulating tabular data;
- external algorithms on relation trees could be defined.

Unfortunately, any custom `Engine` or relation-tree algorithm in practice needs to be able to enumerate all possible `Relation` and operation types (or at least reject any trees with `Relation` or operation types it does not recognize).
Similarly, any custom `Relation` or operation type would need to be able to enumerate the algorithms and engines it supports, in order to provide its own implementations for those algorithms and engines.
This is a fragile multiple-dispatch problem.

To simplify things, this package chooses to prohibit most kinds of `Relation` and operation extensibility:

- custom `Relation` subclasses must be `MarkerRelation` subclasses;
- custom `BinaryOperation` and column expression subclasses are not permitted;
- custom subclasses of `UnaryOperation` are restricted to subclasses of the more limited `RowFilter` and `Reordering` intermediate interfaces.

These prohibitions are enforced by ``__init_subclass__`` checks in the abstract base classes.

.. note::
   `Relation` is actually a `typing.Protocol`, not (just) an ABC, and the concrete `LeafRelation`, `UnaryOperationRelation`, `BinaryOperationRelation`, and `MarkerRelation` classes actually inherit from `BaseRelation` while satisfying the `Relation` interface only in a structural subtyping sense.
   This allows various `Relation` attribute interfaces (e.g. `Relation.engine`) to be implemented as either true properties or dataclass fields, and it should be invisible to users except in the rare case that they need to perform a runtime `isinstance` check with the `Relation` type itself, not just a specific concrete `Relation` subclass: in this case `BaseRelation` must be used instead of `Relation`.

The standard approach to designs like this in object oriented programming is the Visitor Pattern, which in this case would involve a base class or suite of base classes for algorithms or engines with a method for each possible relation-tree node type (relations, operations, column expressions); these would be invoked by a method on each node interface whose concrete implementations call the corresponding algorithm or engine method.
This implicitly restricts the set of node tree types to those with methods in the algorithm/engine base class.

In languages with functional programming aspects, a much more direct approach involving enumerated types and pattern-matching syntax is often possible.
With the introduction of the `match` statement in Python 3.10, this now includes Python, and this is the approach taken in here.
This results in much more readable and concise code than the boilerplate-heavy visitor-pattern alternative, but it comes with a significant drawback: there are no checks (either at runtime or via static type checkers like MyPy) that all necessary `case` branches of a `match` are present.
This is in part by design - many algorithms on relation trees can act generically on most operation types, and hence need to only explicitly match one or two - but it requires considerable discipline from algorithm and engine authors to ensure that match logic is correct and well-tested.
Another (smaller) drawback is that it can occasionally yield code that in other contexts might be considered antipatterns (e.g. `isinstance` is often a good alternative to a single-branch `match`).

.. _relational algebra: https://en.wikipedia.org/wiki/Relational_algebra
.. _SQLAlchemy: https://www.sqlalchemy.org/


.. _lsst.daf.relation-engines:

Provided Engines
================

.. toctree::
   :maxdepth: 1

   iteration.rst
   sql.rst

Contributing
============

``lsst.daf.relation`` is developed at https://github.com/lsst-dm/daf_relation.
You can find Jira issues for this module under the `daf_relation <https://jira.lsstcorp.org/issues/?jql=project%20%3D%20DM%20AND%20component%20%3D%20daf_relation>`_ component.

.. If there are topics related to developing this module (rather than using it), link to this from a toctree placed here.

.. .. toctree::
..    :maxdepth: 1

.. .. _lsst.daf.relation-pyapi:

Python API reference
====================

.. automodapi:: lsst.daf.relation
   :no-main-docstr:
   :include-all-objects:
