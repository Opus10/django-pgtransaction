django-pgtransaction
====================

django-pgtransaction offers a drop-in replacement for the
default ``django.db.transaction`` module which, when used on top of a PostgreSQL
database, extends the functionality of that module with Postgres-specific features.

At present, django-pgtransaction offers an extension of the
``django.db.transaction.atomic`` context manager/decorator which allows one to
dynamically set the `isolation level <https://www.postgresql.org/docs/current/transaction-iso.html>`__
when opening a transaction, as well as specifying
a retry policy for when an operation in that transaction results in a Postgres locking
exception. See :ref:`package` and the quickstart below for examples.

Quickstart
----------

After :ref:`installation`, set the isolation level of a transaction by
using `pgtransaction.atomic`:

.. code-block:: python

    import pgtransaction

    with pgtransaction.atomic(isolation_level=pgtransaction.SERIALIZABLE):
        # Do queries...

There are three isolation levels: ``pgtransaction.READ_COMMITTED``, ``pgtransaction.REPEATABLE_READ``,
and ``pgtransaction.SERIALIZABLE``. By default it inherits the parent isolation level, which is Django's
default of "READ COMMITTED".

When using stricter isolation levels like ``pgtransaction.SERIALIZABLE``, Postgres will throw
serialization errors upon concurrent updates to rows. Use the ``retry`` argument with the decorator
to retry these failures:

.. code-block:: python

	@pgtransaction.atomic(isolation_level=pgtransaction.SERIALIZABLE, retry=3)
	def do_queries():
        # Do queries...

.. note::

	The ``retry`` argument will not work when used as a context manager. A ``RuntimeError``
	will be thrown.

By default, retries are only performed when ``psycopg2.errors.SerializationError`` or
``psycopg2.errors.DeadlockDetected`` errors are raised. Configure retried psycopg2 errors with
``settings.PGTRANSACTION_RETRY_EXCEPTIONS``. You can set a default retry amount with
``settings.PGTRANSACTION_RETRY``.

`pgtransaction.atomic` can be nested, but keep the following in mind:

1. The isolation level cannot be changed once a query has been performed.
2. The retry argument only works on the outermost invocation as a decorator, otherwise ``RuntimeError`` is raised.

Other Reading
-------------

Check out the `Postgres docs <https://www.postgresql.org/docs/current/transaction-iso.html>`__
to learn about transaction isolation in Postgres. 