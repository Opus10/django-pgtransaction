import threading
import time

import ddf
import pytest
from django.db import transaction
from django.db.utils import InternalError, OperationalError

import pgtransaction
from pgtransaction.tests.models import Trade
from pgtransaction.transaction import atomic

try:
    import psycopg.errors as psycopg_errors
except ImportError:
    import psycopg2.errors as psycopg_errors


@pytest.mark.django_db()
def test_atomic_read_committed():
    with atomic(isolation_level=pgtransaction.READ_COMMITTED):
        ddf.G(Trade)
    assert 1 == Trade.objects.count()


@pytest.mark.django_db()
def test_atomic_repeatable_read():
    with atomic(isolation_level=pgtransaction.REPEATABLE_READ):
        ddf.G(Trade)
    assert 1 == Trade.objects.count()


@pytest.mark.django_db(transaction=True)
def test_atomic_repeatable_read_with_select():
    ddf.G(Trade, price=1)
    with atomic(isolation_level="REPEATABLE READ"):
        trade = Trade.objects.last()
        trade.price = 2
        trade.save()
    assert 1 == Trade.objects.count()


@pytest.mark.django_db()
def test_atomic_serializable():
    with atomic(isolation_level=pgtransaction.SERIALIZABLE):
        ddf.G(Trade)
    assert 1 == Trade.objects.count()


@pytest.mark.django_db()
def test_atomic_decorator():
    @atomic(isolation_level="REPEATABLE READ")
    def f():
        ddf.G(Trade)

    f()
    assert 1 == Trade.objects.count()


@pytest.mark.django_db(transaction=True)
def test_atomic_decorator_with_args():
    @atomic(isolation_level="REPEATABLE READ")
    def f(trade_id):
        trade = Trade.objects.get(id=trade_id)
        trade.price = 2
        trade.save()

    trade = ddf.G(Trade, price=1)
    f(trade.pk)
    assert 1 == Trade.objects.count()


@pytest.mark.django_db(transaction=True)
def test_atomic_nested_isolation_levels():
    # This is permitted because no statements have been issued
    with transaction.atomic():
        with atomic(isolation_level="SERIALIZABLE"):
            pass

    # You can't change the isolation levels after issuing
    # a statement
    with pytest.raises(InternalError):
        with atomic(isolation_level="REPEATABLE READ"):
            ddf.G(Trade)
            with atomic(isolation_level="SERIALIZABLE"):
                pass

    # This is permitted because the isolation levels remain the same
    with atomic(isolation_level="REPEATABLE READ"):
        ddf.G(Trade)
        with atomic(isolation_level="REPEATABLE READ"):
            pass

    # Final sanity check
    with pytest.raises(InternalError):
        with atomic(isolation_level="REPEATABLE READ"):
            ddf.G(Trade)
            with atomic(isolation_level="REPEATABLE READ"):
                with atomic(isolation_level="SERIALIZABLE"):
                    pass


@pytest.mark.django_db()
def test_atomic_with_nested_atomic():
    with atomic(isolation_level="REPEATABLE READ"):
        ddf.G(Trade)
        with atomic():
            ddf.G(Trade)
    assert 2 == Trade.objects.count()


@pytest.mark.django_db()
def test_atomic_rollback():
    with pytest.raises(Exception, match="Exception thrown"):
        with atomic(isolation_level="REPEATABLE READ"):
            ddf.G(Trade)
            raise Exception("Exception thrown")

    assert not Trade.objects.exists()


@pytest.mark.django_db()
def test_pg_atomic_nested_atomic_rollback():
    with atomic(isolation_level="REPEATABLE READ"):
        ddf.G(Trade)
        try:
            with atomic():
                ddf.G(Trade)
                raise RuntimeError
        except RuntimeError:
            pass
    assert 1 == Trade.objects.count()


@pytest.mark.django_db(transaction=True)
def test_atomic_retries_context_manager_not_allowed():
    with pytest.raises(RuntimeError, match="as a context manager"):
        with atomic(isolation_level="REPEATABLE READ", retry=1):
            pass


@pytest.mark.django_db()
def test_atomic_nested_retries_not_permitted():
    with pytest.raises(RuntimeError, match="Retries are not permitted"):
        with transaction.atomic():
            with atomic(isolation_level="REPEATABLE READ", retry=1):
                pass

    @atomic(isolation_level="REPEATABLE READ", retry=1)
    def decorated():
        pass

    with pytest.raises(RuntimeError, match="Retries are not permitted"):
        with transaction.atomic():
            decorated()


@pytest.mark.django_db(transaction=True)
def test_atomic_retries_all_retries_fail():
    assert not Trade.objects.exists()
    attempts = []

    @atomic(isolation_level="REPEATABLE READ", retry=2)
    def func(retries):
        attempts.append(True)
        ddf.G(Trade)
        raise OperationalError from psycopg_errors.SerializationFailure

    with pytest.raises(OperationalError):
        func(attempts)

    assert not Trade.objects.exists()
    assert len(attempts) == 3

    # Ensure the decorator tries again
    with pytest.raises(OperationalError):
        func(attempts)

    assert not Trade.objects.exists()
    assert len(attempts) == 6


@pytest.mark.django_db(transaction=True)
def test_atomic_retries_decorator_first_retry_passes():
    assert not Trade.objects.exists()
    attempts = []

    @atomic(isolation_level="REPEATABLE READ", retry=1)
    def func(attempts):
        attempts.append(True)
        ddf.G(Trade)
        if len(attempts) == 1:
            raise OperationalError from psycopg_errors.SerializationFailure

    func(attempts)
    assert 1 == Trade.objects.all().count()
    assert len(attempts) == 2


@pytest.mark.django_db(transaction=True)
def test_pg_atomic_retries_with_nested_atomic_failure():
    assert not Trade.objects.exists()
    attempts = []

    @atomic(isolation_level="REPEATABLE READ", retry=2)
    def outer(attempts):
        ddf.G(Trade)

        @atomic
        def inner(attempts):
            attempts.append(True)
            ddf.G(Trade)
            raise psycopg_errors.SerializationFailure

        try:
            inner(attempts)
        except psycopg_errors.SerializationFailure:
            pass

    outer(attempts)
    assert 1 == Trade.objects.all().count()
    assert len(attempts) == 1


@pytest.mark.django_db(transaction=True)
def test_atomic_retries_with_run_time_failure():
    assert not Trade.objects.exists()
    attempts = []

    @atomic(isolation_level="REPEATABLE READ", retry=2)
    def outer(attempts):
        attempts.append(True)
        ddf.G(Trade)
        raise RuntimeError

    with pytest.raises(RuntimeError):
        outer(attempts)

    assert not Trade.objects.all().exists()
    assert len(attempts) == 1


@pytest.mark.django_db(transaction=True)
def test_atomic_retries_with_nested_atomic_and_outer_retry():
    assert not Trade.objects.exists()
    attempts = []

    @atomic(isolation_level="REPEATABLE READ", retry=1)
    def outer(attempts):
        ddf.G(Trade)

        @atomic
        def inner(attempts):
            attempts.append(True)
            ddf.G(Trade)

        inner(attempts)

        if len(attempts) == 1:
            raise OperationalError from psycopg_errors.SerializationFailure

    outer(attempts)
    assert 2 == Trade.objects.all().count()
    assert len(attempts) == 2


@pytest.mark.django_db(transaction=True)
def test_concurrent_serialization_error():
    """
    Simulate a concurrency issue that will throw a serialization error.
    Ensure that a retry is successful
    """

    def concurrent_update(barrier, trade, calls):
        # We have to instantiate the decorator inside the function, otherwise
        # it is shared among threads and causes the test to hang. It's uncertain
        # what causes it to hang.
        @pgtransaction.atomic(isolation_level="SERIALIZABLE", retry=3)
        def inner_update(trade, calls):
            calls.append(True)
            trade = Trade.objects.get(id=trade.id)
            trade.price = 2
            trade.save()
            time.sleep(1)

        barrier.wait()
        inner_update(trade, calls)

    barrier = threading.Barrier(2)
    trade = ddf.G(Trade, price=1)
    calls = []
    t1 = threading.Thread(target=concurrent_update, args=[barrier, trade, calls])
    t2 = threading.Thread(target=concurrent_update, args=[barrier, trade, calls])

    t1.start()
    t2.start()

    t1.join()
    t2.join()

    # We should have at least had three attempts. It's highly unlikely we would have four,
    # but the possibility exists.
    assert 3 <= len(calls) <= 4
