import ddf
from django.db.utils import IntegrityError
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
    @atomic(isolation_level='REPEATABLE READ')
    def f():
        ddf.G(Trade)
    f()
    assert 1 == Trade.objects.count()


@pytest.mark.django_db(transaction=True)
def test_atomic_decorator_with_args():
    @atomic(isolation_level='REPEATABLE READ')
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
    class MockError(Exception):
        pass
    with pytest.raises(MockError):
        with atomic(isolation_level="REPEATABLE READ"):
            ddf.G(Trade)
            raise MockError
    assert not Trade.objects.exists()


@pytest.mark.django_db(transaction=True)
def test_pg_atomic_nested_atomic_rollback():
    with atomic(isolation_level="REPEATABLE READ"):
        trade = ddf.G(Trade, company='Coca Cola')
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
        with atomic(isolation_level='REPEATABLE READ', retry=1):
            pass


@pytest.mark.django_db(transaction=True)
def test_atomic_retries_all_retries_fail():
    dec = atomic(isolation_level='REPEATABLE READ', retry=2)
    assert not Trade.objects.exists()
    assert 2 == dec.retry

    @dec
    def func():
        ddf.G(Trade)
        raise psycopg2.errors.SerializationFailure

    with pytest.raises(psycopg2.errors.SerializationFailure):
        func()

    assert not Trade.objects.exists()
    assert 0 == dec.retry


@pytest.mark.django_db(transaction=True)
def test_atomic_retries_decorator_first_retry_passes():
    dec = atomic(isolation_level='REPEATABLE READ', retry=1)
    assert not Trade.objects.exists()
    assert 1 == dec.retry

    @dec
    def func():
        ddf.G(Trade)
        if dec.retry == 1:
            raise psycopg2.errors.SerializationFailure

    func()
    assert 1 == Trade.objects.all().count()
    assert 0 == dec.retry


@pytest.mark.django_db(transaction=True)
def test_pg_atomic_retries_with_nested_atomic_failure():
    dec = atomic(isolation_level='REPEATABLE READ', retry=2)

    assert not Trade.objects.exists()
    assert 2 == dec.retry

    @dec
    def outer():
        ddf.G(Trade)

        @atomic
        def inner():
            ddf.G(Trade)
            raise psycopg2.errors.SerializationFailure
        try:
            inner()
        except psycopg2.errors.SerializationFailure:
            pass

    outer()
    assert 1 == Trade.objects.all().count()
    assert 2 == dec.retry


@pytest.mark.django_db(transaction=True)
def test_atomic_retries_with_run_time_failure():
    dec = atomic(isolation_level='REPEATABLE READ', retry=2)

    assert not Trade.objects.exists()
    assert 2 == dec.retry

    @dec
    def outer():
        ddf.G(Trade)
        raise RuntimeError

    with pytest.raises(RuntimeError):
        outer()

    assert not Trade.objects.all().exists()
    assert 2 == dec.retry  # We shouldn't retry on RuntimeErrors


@pytest.mark.django_db(transaction=True)
def test_atomic_retries_with_nested_atomic_and_outer_retry():
    dec = atomic(isolation_level='REPEATABLE READ', retry=1)

    assert 0 == Trade.objects.all().count()
    assert 1 == dec.retry

    @dec
    def outer():
        ddf.G(Trade)

        @atomic
        def inner():
            ddf.G(Trade)
        inner()
        if dec.retry == 1:
            raise psycopg2.errors.SerializationFailure

    outer()
    assert 2 == Trade.objects.all().count()
    assert 0 == dec.retry
