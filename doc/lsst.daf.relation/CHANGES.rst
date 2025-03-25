lsst-daf-relation v29.0.0 2025-03-25
====================================

No user-visible changes.

lsst-daf-relation v28.0.0 2024-11-20
====================================

No user-visible changes.

lsst-daf-relation v27.0.0 2024-06-05
====================================

Validated with Python 3.12.

- Fixed min/max rows logic for deduplication (`DM-42324 <https://rubinobs.atlassian.net/browse/DM-42324>`_)

lsst-daf-relation v26.0.0 2023-09-22
====================================

Bug Fixes
---------

- Fixed bug in SQL engine relation reordering that caused Selections and Calculations on top of Chains to fail.

  This manifested most prominently in butler queries involving multiple collections that could not be optimized away at query generation time using summary information. (`DM-37504 <https://rubinobs.atlassian.net/browse/DM-37504>`_)


Miscellaneous Changes of Minor Interest
---------------------------------------

- Made compatible with SQLAlchemy 2.0.
  This retains compatibility with SQLAlchemy 1.4.x.(`DM-367738 <https://rubinobs.atlassian.net/browse/DM-367738>`_)
