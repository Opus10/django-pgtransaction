from functools import wraps

from django.db import DEFAULT_DB_ALIAS, Error, transaction
from django.db.utils import NotSupportedError
import psycopg2.errors


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

        if self.isolation_level:  # pragma: no cover
            connection = transaction.get_connection(self.using)

            if connection.vendor != "postgresql":
                raise NotSupportedError(
                    f"pgtransaction.atomic cannot be used with {connection.vendor}"
                )

            if self.isolation_level.upper() not in (
                "READ COMMITTED",
                "REPEATABLE READ",
                "SERIALIZABLE",
            ):
                raise ValueError(f'Invalid isolation level "{self.isolation_level}"')

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

        if connection.in_atomic_block:
            if self.isolation_level:
                raise RuntimeError(
                    "Setting the isolation level inside in a nested atomic "
                    "transaction is not permitted. Nested atomic transactions "
                    "inherit the isolation level from their parent transaction "
                    "automatically."
                )

            if self.retry:  # pragma: no cover
                raise RuntimeError("Retries are not permitted within a nested atomic transaction")

        if self.retry and self._used_as_context_manager:
            raise RuntimeError(
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
