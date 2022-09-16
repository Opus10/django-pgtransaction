from functools import wraps

from django.db import DEFAULT_DB_ALIAS, Error, transaction
import psycopg2.errors


class PGAtomicConfigurationError(Exception):
    pass


class Atomic(transaction.Atomic):
    def __init__(
        self,
        using,
        savepoint,
        durable,
        isolation_level,
        retry,
    ):
        super().__init__(using, savepoint, durable)
        self.isolation_level = isolation_level
        self.retry = retry
        self._used_as_context_manager = True
        self._validate()

    def _validate(self):
        connection = transaction.get_connection(self.using)

        if self.isolation_level:
            if connection.vendor != "postgresql":  # pragma: no cover
                raise PGAtomicConfigurationError(
                    f"pgtransaction.atomic cannot be used with {connection.vendor}"
                )

            if self.isolation_level.upper() not in (
                "READ COMMITTED",
                "REPEATABLE READ",
                "SERIALIZABLE",
            ):  # pragma: no cover
                raise PGAtomicConfigurationError(
                    f"Isolation level {self.isolation_level} not recognised"
                )

            if connection.in_atomic_block:
                raise PGAtomicConfigurationError(
                    "Setting the isolation level inside in a nested atomic "
                    "transaction is not permitted. Nested atomic transactions "
                    "inherit the isolation level from their parent transaction "
                    "automatically."
                )

        if self.retry and connection.in_atomic_block:  # pragma: no cover
            raise PGAtomicConfigurationError(
                "Retries are not permitted within a nested atomic transaction"
            )

    def __call__(self, func):
        self._used_as_context_manager = False

        @wraps(func)
        def inner(*args, **kwds):
            num_retries = 0

            while True:  # pragma: no branch
                try:
                    with self._recreate_cm():
                        return func(*args, **kwds)
                except Error as error:
                    if (
                        error.__cause__.__class__
                        not in (
                            psycopg2.errors.SerializationFailure,
                            psycopg2.errors.DeadlockDetected,
                        )
                        or num_retries >= self.retry
                    ):
                        raise

                num_retries += 1

        return inner

    def __enter__(self):
        connection = transaction.get_connection(self.using)

        if self.retry != 0 and self._used_as_context_manager:
            raise PGAtomicConfigurationError(
                "Cannot use pgtransaction.atomic as a context manager "
                "when retry is non-zero. Use as a decorator instead."
            )

        super().__enter__()

        if self.isolation_level:
            connection.cursor().execute(
                f"SET TRANSACTION ISOLATION LEVEL {self.isolation_level.upper()}"
            )


def atomic(
    using=None,
    savepoint=True,
    durable=False,
    isolation_level=None,
    retry=0,
):
    # Copies structure of django.db.transaction.atomic
    if callable(using):
        return Atomic(
            DEFAULT_DB_ALIAS,
            savepoint,
            durable,
            isolation_level,
            retry,
        )(using)
    else:
        return Atomic(
            using,
            savepoint,
            durable,
            isolation_level,
            retry,
        )
