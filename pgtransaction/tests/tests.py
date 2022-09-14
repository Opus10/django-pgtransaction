import psycopg2.errors
import pytest

from pgtransaction.atomic import atomic, PGAtomicConfigurationError
from pgtransaction.tests.models import Trade


@pytest.mark.django_db(transaction=True)
def test_atomic_read_committed():
    with atomic(isolation_level="READ COMMITTED"):
        Trade.objects.create(company='Coca Cola', price=1)
    assert 1 == Trade.objects.count()


@pytest.mark.django_db(transaction=True)
def test_atomic_repeatable_read():
    with atomic(isolation_level="REPEATABLE READ"):
        Trade.objects.create(company='Coca Cola', price=1)
    assert 1 == Trade.objects.count()


@pytest.mark.django_db(transaction=True)
def test_atomic_repeatable_read_with_select():
    Trade.objects.create(company='Coca Cola', price=1)
    with atomic(isolation_level="REPEATABLE READ"):
        trade = Trade.objects.last()
        trade.price = 2
        trade.save()
    assert 1 == Trade.objects.count()


@pytest.mark.django_db(transaction=True)
def test_atomic_serializable():
    with atomic(isolation_level="SERIALIZABLE"):
        Trade.objects.create(company='Coca Cola', price=1)
    assert 1 == Trade.objects.count()


@pytest.mark.django_db(transaction=True)
def test_atomic_serializable():
    with atomic(isolation_level="SERIALIZABLE"):
        Trade.objects.create(company='Coca Cola', price=1)
    assert 1 == Trade.objects.count()


@pytest.mark.django_db(transaction=True)
def test_atomic_decorator():
    @atomic(isolation_level='REPEATABLE READ')
    def f():
        Trade.objects.create(company='Coca Cola', price=1)
    f()
    assert 1 == Trade.objects.count()


@pytest.mark.django_db(transaction=True)
def test_atomic_decorator_with_args():
    @atomic(isolation_level='REPEATABLE READ')
    def f(trade_id):
        trade = Trade.objects.get(id=trade_id)
        trade.price = 2
        trade.save()
    trade = Trade.objects.create(company='Coca Cola', price=1)
    f(trade.pk)
    assert 1 == Trade.objects.count()


@pytest.mark.django_db(transaction=True)
def test_atomic_nested_isolation_level_not_allowed():
    with pytest.raises(PGAtomicConfigurationError):
        with atomic(isolation_level="REPEATABLE READ"):
            Trade.objects.create(company='Coca Cola', price=1)
            with atomic(isolation_level="REPEATABLE READ"):
                pass


@pytest.mark.django_db(transaction=True)
def test_atomic_with_nested_atomic():
    with atomic(isolation_level="REPEATABLE READ"):
        Trade.objects.create(company='Coca Cola', price=1)
        with atomic():
            Trade.objects.create(company='Coca Cola 2', price=1)
    assert 2 == Trade.objects.count()


@pytest.mark.django_db(transaction=True)
def test_atomic_rollback():
    class MockError(Exception):
        pass
    with pytest.raises(MockError):
        with atomic(isolation_level="REPEATABLE READ"):
            Trade.objects.create(company='Coca Cola', price=1)
            raise MockError
    assert 0 == Trade.objects.count()


@pytest.mark.django_db(transaction=True)
def test_pg_atomic_nested_atomic_rollback():
    with atomic(isolation_level="REPEATABLE READ"):
        Trade.objects.create(company='Coca Cola', price=1)
        try:
            with atomic():
                Trade.objects.create(company='Coca Cola 2', price=1)
                raise RuntimeError
        except RuntimeError:
            pass
    assert 1 == Trade.objects.count()


@pytest.mark.django_db(transaction=True)
def test_atomic_retries_context_manager_not_allowed():
    with pytest.raises(PGAtomicConfigurationError):
        with atomic(isolation_level='REPEATABLE READ', retry=1):
            Trade.objects.create(company='Coca Cola', price=1)


@pytest.mark.django_db(transaction=True)
def test_atomic_retries_all_retries_fail():
    dec = atomic(isolation_level='REPEATABLE READ', retry=2)
    assert 0 == Trade.objects.all().count()
    assert 2 == dec.retry

    @dec
    def func():
        Trade.objects.create(company='Coca Cola', price=1)
        raise psycopg2.errors.SerializationFailure

    with pytest.raises(psycopg2.errors.SerializationFailure):
        func()

    assert 0 == Trade.objects.all().count()
    assert 0 == dec.retry


@pytest.mark.django_db(transaction=True)
def test_atomic_retries_decorator_first_retry_passes(self):
    dec = atomic(isolation_level='REPEATABLE READ', retries=1)
    assert 0 == Trade.objects.all().count()
    assert 1 == dec.retry

    @dec
    def func():
        Trade.objects.create(company='Coca Cola', price=1)
        if dec.retries == 1:
            raise psycopg2.errors.SerializationFailure

    with pytest.raises(psycopg2.errors.SerializationFailure):
        func()

    assert 1 == Trade.objects.all().count()
    assert 0 == dec.retry


@pytest.mark.django_db(transaction=True)
def test_atomic_retries_decorator_first_retry_passes():
    dec = atomic(isolation_level='REPEATABLE READ', retry=1)
    assert 0 == Trade.objects.all().count()
    assert 1 == dec.retry

    @dec
    def func():
        Trade.objects.create(company='Coca Cola', price=1)
        if dec.retry == 1:
            raise psycopg2.errors.SerializationFailure

    func()
    assert 1 == Trade.objects.all().count()
    assert 0 == dec.retry


@pytest.mark.django_db(transaction=True)
def test_pg_atomic_retries_with_nested_atomic_failure():
    dec = atomic(isolation_level='REPEATABLE READ', retry=2)

    assert 0 == Trade.objects.all().count()
    assert 2 == dec.retry

    @dec
    def outer():
        Trade.objects.create(company='Coca Cola', price=1)

        @atomic
        def inner():
            Trade.objects.create(company='Coca Cola 2', price=1)
            raise psycopg2.errors.SerializationFailure
        try:
            inner()
        except psycopg2.errors.SerializationFailure:
            pass

    outer()
    assert 1 == Trade.objects.all().count()
    assert 2 == dec.retry


@pytest.mark.django_db(transaction=True)
def test_atomic_retries_with_nested_atomic_run_time_failure():
    dec = atomic(isolation_level='REPEATABLE READ', retry=2)

    assert 0 == Trade.objects.all().count()
    assert 2 == dec.retry

    @dec
    def outer():
        Trade.objects.create(company='Coca Cola', price=1)

        @atomic
        def inner():
            Trade.objects.create(company='Coca Cola 2', price=1)
            raise RuntimeError
        try:
            inner()
        except RuntimeError:
            pass

    outer()
    assert 1 == Trade.objects.all().count()
    assert 2 == dec.retry


@pytest.mark.django_db(transaction=True)
def test_atomic_retries_with_nested_atomic_and_outer_retry():
    dec = atomic(isolation_level='REPEATABLE READ', retry=1)

    assert 0 == Trade.objects.all().count()
    assert 1 == dec.retry

    @dec
    def outer():
        Trade.objects.create(company='Coca Cola', price=1)

        @atomic
        def inner():
            Trade.objects.create(company='Coca Cola 2', price=1)
        inner()
        if dec.retry == 1:
            raise psycopg2.errors.SerializationFailure

    outer()
    assert 2 == Trade.objects.all().count()
    assert 0 == dec.retry
    assert 1 == Trade.objects.filter(company='Coca Cola').count()
    assert 1 == Trade.objects.filter(company='Coca Cola 2').count()
