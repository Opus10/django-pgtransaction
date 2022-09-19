Installation
============

Install django-pgtransaction with::

    pip3 install django-pgtransaction


When used as a decorator, ``pgtransaction.atomic`` can be configured so
that transactions are retried a certain number of times after encountering
certain specified exceptions. One can configure custom defaults for these
values in the their project's settings file:

* To specify a default retry count, set ``PGTRANSACTION_RETRY`` to non-negative integer.
  If not specified, ``PGTRANSACTION_RETRY`` defaults to 0.

* To specify default exceptions to retry upon encountering, set ``PGTRANSACTION_RETRY_EXCEPTIONS``
  to a tuple of exception classes. If not specified, ``PGTRANSACTION_RETRY_EXCEPTIONS``
  defaults to ``(psycopg2.errors.SerializationFailure, psycopg2.errors.DeadlockDetected)``
