import threading
import time

import ddf
from django.db.utils import IntegrityError, OperationalError
import psycopg2.errors
import pytest

from pgtransaction.atomic import atomic, PGAtomicConfigurationError
from pgtransaction.tests.models import Trade


@pytest.mark.django_db(transaction=True)
def test_atomic_read_committed():
    with atomic(isolation_level="READ COMMITTED"):
        ddf.G(Trade)
    assert 1 == Trade.objects.count()


@pytest.mark.django_db(transaction=True)
def test_atomic_repeatable_read():
    with atomic(isolation_level="REPEATABLE READ"):
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


@pytest.mark.django_db(transaction=True)
def test_atomic_serializable():
    with atomic(isolation_level="SERIALIZABLE"):
        ddf.G(Trade)
    assert 1 == Trade.objects.count()


@pytest.mark.django_db(transaction=True)
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
def test_atomic_nested_isolation_level_not_allowed():
    with pytest.raises(PGAtomicConfigurationError):
        with atomic(isolation_level="REPEATABLE READ"):
            ddf.G(Trade)
            with atomic(isolation_level="REPEATABLE READ"):
                pass


@pytest.mark.django_db(transaction=True)
def test_atomic_with_nested_atomic():
    with atomic(isolation_level="REPEATABLE READ"):
        ddf.G(Trade)
        with atomic():
            ddf.G(Trade)
    assert 2 == Trade.objects.count()


@pytest.mark.django_db(transaction=True)
def test_atomic_rollback():
    with pytest.raises(Exception, match="Exception thrown"):
        with atomic(isolation_level="REPEATABLE READ"):
            ddf.G(Trade)
            raise Exception("Exception thrown")

    assert not Trade.objects.exists()


@pytest.mark.django_db(transaction=True)
def test_pg_atomic_nested_atomic_rollback():
    with atomic(isolation_level="REPEATABLE READ"):
        trade = ddf.G(Trade, company="Coca Cola")
        try:
            with atomic():  # pragma: no branch
                trade.id = None
                trade.save()
        except IntegrityError:
            pass
    assert 1 == Trade.objects.count()


@pytest.mark.django_db(transaction=True)
def test_atomic_retries_context_manager_not_allowed():
    with pytest.raises(PGAtomicConfigurationError, match="as a context manager"):
        with atomic(isolation_level="REPEATABLE READ", retry=1):
            pass


@pytest.mark.django_db(transaction=True)
def test_atomic_retries_all_retries_fail():
    assert not Trade.objects.exists()
    attempts = []

    @atomic(isolation_level="REPEATABLE READ", retry=2)
    def func(retries):
        attempts.append(True)
        ddf.G(Trade)
        raise OperationalError from psycopg2.errors.SerializationFailure

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
            raise OperationalError from psycopg2.errors.SerializationFailure

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
            raise psycopg2.errors.SerializationFailure

        try:
            inner(attempts)
        except psycopg2.errors.SerializationFailure:
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
            raise OperationalError from psycopg2.errors.SerializationFailure

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
        @atomic(isolation_level="SERIALIZABLE", retry=3)
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
