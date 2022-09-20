from django.conf import settings
import psycopg2.errors


def retry_exceptions():
    """The default errors caught when retrying.

    Note that these must be psycopg2 errors.
    """
    return getattr(
        settings,
        "PGTRANSACTION_RETRY_EXCEPTIONS",
        (psycopg2.errors.SerializationFailure, psycopg2.errors.DeadlockDetected),
    )


def retry():
    """The default retry amount"""
    return getattr(settings, "PGTRANSACTION_RETRY", 0)
