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
            if self.connection.vendor != "postgresql":
                raise NotSupportedError(
                    f"pgtransaction.atomic cannot be used with {self.connection.vendor}"
                )

            if self.isolation_level.upper() not in (
                "READ COMMITTED",
                "REPEATABLE READ",
                "SERIALIZABLE",
            ):
                raise ValueError(f'Invalid isolation level "{self.isolation_level}"')

    @property
    def connection(self):
        # Don't set this property on the class, otherwise it won't be thread safe
        return transaction.get_connection(self.using)

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

    def execute_set_isolation_level(self):
        with self.connection.cursor() as cursor:
            cursor.execute(f"SET TRANSACTION ISOLATION LEVEL {self.isolation_level.upper()}")

    def __enter__(self):
        in_nested_atomic_block = self.connection.in_atomic_block

        if in_nested_atomic_block and self.retry:
            raise RuntimeError("Retries are not permitted within a nested atomic transaction")

        if self.retry and self._used_as_context_manager:
            raise RuntimeError(
                "Cannot use pgtransaction.atomic as a context manager "
                "when retry is non-zero. Use as a decorator instead."
            )

        # If we're already in a nested atomic block, try setting the isolation
        # level before any check points are made when entering the atomic decorator.
        # This helps avoid errors and allow people to still nest isolation levels
        # when applicable
        if in_nested_atomic_block and self.isolation_level:
            self.execute_set_isolation_level()

        super().__enter__()

        # If we weren't in a nested atomic block, set the isolation level for the first
        # time after the transaction has been started
        if not in_nested_atomic_block and self.isolation_level:
            self.execute_set_isolation_level()


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
