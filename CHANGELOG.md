# Changelog
## 1.1.0 (2023-06-08)
### Feature
  - Added Python 3.11, Django 4.2, and Psycopg 3 support [Wesley Kendall, 6c032bb]

    Adds Python 3.11, Django 4.2, and Psycopg 3 support along with tests for multiple Postgres versions. Drops support for Django 2.2.

## 1.0.0 (2022-09-20)
### Api-Break
  - Initial release of django-pgtransaction [Paul Gilmartin, 09bca27]

    django-pgtransaction offers a drop-in replacement for the
    default ``django.db.transaction`` module which, when used on top of a PostgreSQL
    database, extends the functionality of that module with Postgres-specific features.

    V1 of django-pgtransaction provides the ``atomic`` decorator/context manager, which
    provides the following additional arguments to Django's ``atomic``:

    1. ``isolation_level``: For setting the isolation level of the transaction.
    2. ``retry``: For retrying when deadlock or serialization errors happen.

