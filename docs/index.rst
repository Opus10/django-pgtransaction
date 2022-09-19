django-pgtransaction
=======================================================================

django-pgtransaction offers a drop-in replacement for the
default ``django.db.transaction`` module which, when used on top of a PostgreSQL
database, extends the functionality of that module with Postgres-specific features.

At present, ``django-pgtransaction`` offers an extension of the
``django.db.transaction.atomic`` context manager/decorator which allows one to
dynamically set the isolation level when opening a transaction, as well as specifying
a retry policy for when an operation in that transaction results in a Postgres locking
exception. See :ref:`package` and :ref:`example` for more.
