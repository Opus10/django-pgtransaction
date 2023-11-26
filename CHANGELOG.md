# Changelog

## 1.3.0 (2023-11-26)

### Feature

  - Django 5.0 compatibility [Wesley Kendall, 129331b]

    Support and test against Django 5 with psycopg2 and psycopg3.

## 1.2.1 (2023-10-09)

### Trivial

  - Added Opus10 branding to docs [Wesley Kendall, 4a0b78c]

## 1.2.0 (2023-10-08)

### Feature

  - Add Python 3.12 support and use Mkdocs for documentation [Wesley Kendall, ed0d18e]

    Python 3.12 and Postgres 16 are supported now, along with having revamped docs using Mkdocs and the Material theme.

    Python 3.7 support was dropped.

## 1.1.0 (2023-06-09)

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
