# django-pgtransaction

`django-pgtransaction` offers a drop-in replacement for the default `django.db.transaction` module which, when used on top of a PostgreSQL database, extends the functionality of that module with Postgres-specific features.

At present, django-pgtransaction offers an extension of the `django.db.transaction.atomic` context manager/decorator which allows one to dynamically set the [isolation level](https://www.postgresql.org/docs/current/transaction-iso.html) when opening a transaction, as well as specifying a retry policy for when an operation in that transaction results in a Postgres locking exception. See the quickstart below or [the docs](https://django-pgtransaction.readthedocs.io/) for examples.

## Quickstart

Set the isolation level of a transaction by using `pgtransaction.atomic`:

```python
import pgtransaction

with pgtransaction.atomic(isolation_level=pgtransaction.SERIALIZABLE):
    # Do queries...
```

There are three isolation levels: `pgtransaction.READ_COMMITTED`, `pgtransaction.REPEATABLE_READ`, and `pgtransaction.SERIALIZABLE`. By default it inherits the parent isolation level, which is Django's default of "READ COMMITTED".

When using stricter isolation levels like `pgtransaction.SERIALIZABLE`, Postgres will throw serialization errors upon concurrent updates to rows. Use the `retry` argument with the decorator to retry these failures:

```python
@pgtransaction.atomic(isolation_level=pgtransaction.SERIALIZABLE, retry=3)
def do_queries():
    # Do queries...
```

Note that the `retry` argument will not work when used as a context manager. A `RuntimeError` will be thrown.

By default, retries are only performed when `psycopg.errors.SerializationError` or `psycopg.errors.DeadlockDetected` errors are raised. Configure retried psycopg errors with `settings.PGTRANSACTION_RETRY_EXCEPTIONS`. You can set a default retry amount with `settings.PGTRANSACTION_RETRY`.

`pgtransaction.atomic` can be nested, but keep the following in mind:

1. The isolation level cannot be changed once a query has been performed.
2. The retry argument only works on the outermost invocation as a decorator, otherwise `RuntimeError` is raised.

## Compatibility

`django-pgtransaction` is compatible with Python 3.9 - 3.13, Django 4.2 - 5.1, Psycopg 2 - 3, and Postgres 13 - 17.

## Documentation

Check out the [Postgres docs](https://www.postgresql.org/docs/current/transaction-iso.html) to learn about transaction isolation in Postgres. 

[View the django-pgtransaction docs here](https://django-pgtransaction.readthedocs.io/)

## Installation

Install `django-pgtransaction` with:

    pip3 install django-pgtransaction
After this, add `pgtransaction` to the `INSTALLED_APPS` setting of your Django project.

## Contributing Guide

For information on setting up django-pgtransaction for development and contributing changes, view [CONTRIBUTING.md](CONTRIBUTING.md).

## Creators

- [Paul Gilmartin](https://github.com/PaulGilmartin)
- [Wes Kendall](https://github.com/wesleykendall)

