Relation v26.0.0 2023-09-22
===========================

Bug Fixes
---------

- Fixed bug in SQL engine relation reordering that caused Selections and Calculations on top of Chains to fail.

  This manifested most prominently in butler queries involving multiple collections that could not be optimized away at query generation time using summary information. (`DM-37504 <https://jira.lsstcorp.org/browse/DM-37504>`_)


Miscellaneous Changes of Minor Interest
---------------------------------------

- Made compatible with SQLAlchemy 2.0.
  This retains compatibility with SQLAlchemy 1.4.x.(`DM-367738 <https://jira.lsstcorp.org/browse/DM-367738>`_)
