import psycopg2.errors

from pgtransaction import config


def test_retry_exceptions(settings):
    assert config.retry_exceptions() == (
        psycopg2.errors.SerializationFailure,
        psycopg2.errors.DeadlockDetected,
    )

    settings.PGTRANSACTION_RETRY_EXCEPTIONS = [psycopg2.errors.DeadlockDetected]
    assert config.retry_exceptions() == [psycopg2.errors.DeadlockDetected]


def test_retry(settings):
    assert config.retry() == 0

    settings.PGTRANSACTION_RETRY = 1
    assert config.retry() == 1
