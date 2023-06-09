try:
    import psycopg.errors as psycopg_errors
except ImportError:
    import psycopg2.errors as psycopg_errors

from pgtransaction import config


def test_retry_exceptions(settings):
    assert config.retry_exceptions() == (
        psycopg_errors.SerializationFailure,
        psycopg_errors.DeadlockDetected,
    )

    settings.PGTRANSACTION_RETRY_EXCEPTIONS = [psycopg_errors.DeadlockDetected]
    assert config.retry_exceptions() == [psycopg_errors.DeadlockDetected]


def test_retry(settings):
    assert config.retry() == 0

    settings.PGTRANSACTION_RETRY = 1
    assert config.retry() == 1
