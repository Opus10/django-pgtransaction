from django.conf import settings
from django.core.exceptions import ImproperlyConfigured

try:
    import psycopg.errors as psycopg_errors
except ImportError:
    import psycopg2.errors as psycopg_errors
except Exception as exc:  # pragma: no cover
    raise ImproperlyConfigured("Error loading psycopg2 or psycopg module") from exc


def retry_exceptions():
    """The default errors caught when retrying.

    Note that these must be psycopg errors.
    """
    return getattr(
        settings,
        "PGTRANSACTION_RETRY_EXCEPTIONS",
        (psycopg_errors.SerializationFailure, psycopg_errors.DeadlockDetected),
    )


def retry():
    """The default retry amount"""
    return getattr(settings, "PGTRANSACTION_RETRY", 0)
