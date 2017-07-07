import asyncio
import random
import uuid
from unittest import TestCase, main

from urllib3 import Retry

from acapella.kv import Session
from acapella.kv.utils.errors import CasError

retry = Retry(
    total=3,
    connect=3,
    read=3,
    backoff_factor=0.3
)
session = Session(port=12000, max_retries=retry)


def async_test(f):
    def wrapper(*args, **kwargs):
        coro = asyncio.coroutine(f)
        future = coro(*args, **kwargs)
        loop = asyncio.get_event_loop()
        loop.run_until_complete(future)
    return wrapper


def random_tree():
    c = random.randint(1, 3)
    return [str(uuid.uuid4()) for _ in range(c)]


def random_key():
    c = random.randint(1, 3)
    return [str(uuid.uuid4()) for _ in range(c)]


def random_value():
    return str(uuid.uuid4())


# Для тестов необходимы запущенные KV (127.0.0.1:10000) и HTTP (127.0.0.1:12000) ноды


class TestKvNonTx(TestCase):
    @async_test
    async def test_get(self):
        await session.get_entry(random_key())

    @async_test
    async def test_set(self):
        await session.entry(random_key()).set(random_value())

    @async_test
    async def test_set_none(self):
        await session.entry(random_key()).set(None)

    @async_test
    async def test_return_set_value(self):
        key = random_key()
        value = random_value()
        await session.entry(key).set(value)
        assert (await session.get_entry(key)).value == value

    @async_test
    async def test_cas_success(self):
        key = random_key()
        value = random_value()
        await session.entry(key).cas(value)
        assert (await session.get_entry(key)).value == value

    @async_test
    async def test_cas_failed(self):
        key = random_key()
        value = random_value()
        entry = await session.get_entry(key)
        with self.assertRaises(CasError):
            await entry.cas(value, entry.version + 1)

    @async_test
    async def test_get_version_returns_valid_version(self):
        key = random_key()
        value = random_value()
        entry = session.entry(key)
        await entry.set(value)
        version = await session.get_version(key)
        assert entry.version == version


class TestKvTx(TestCase):
    @async_test
    async def test_create_tx(self):
        async with session.transaction():
            pass

    @async_test
    async def test_tx_rollback(self):
        async with session.transaction() as tx:
            await tx.rollback()

    @async_test
    async def test_old_value_if_rollback(self):
        key = random_key()
        async with session.transaction() as tx:
            e = await tx.get_entry(key)
            value = e.value
            await e.set(random_value())
            await tx.rollback()

        async with session.transaction() as tx:
            e = await tx.get_entry(key)
            assert value == e.value

    @async_test
    async def test_rollback_if_error(self):
        key = random_key()
        value = None
        try:
            async with session.transaction() as tx:
                e = await tx.get_entry(key)
                value = e.value
                await e.set(random_value())
                raise Exception()
        except Exception:
            pass

        async with session.transaction() as tx:
            e = await tx.get_entry(key)
            assert value == e.value

    @async_test
    async def test_see_set_in_other_tx(self):
        key = random_key()
        value = random_value()

        async with session.transaction() as tx:
            await tx.entry(key).set(value)

        async with session.transaction() as tx:
            e = await tx.get_entry(key)
            assert value == e.value


class TestDtNonTx(TestCase):
    @async_test
    async def test_get(self):
        await session.tree(random_tree()).get_cursor(random_key())

    @async_test
    async def test_set(self):
        await session.tree(random_tree()).cursor(random_key()).set(random_value())

    @async_test
    async def test_return_set_value(self):
        tree = session.tree(random_tree())
        key = random_key()
        value = random_value()
        await tree.cursor(key).set(value)
        assert value == (await tree.get_cursor(key)).value

    @async_test
    async def test_next(self):
        tree = session.tree(random_tree())
        await tree.cursor(['A', 'A']).set('foo')
        await tree.cursor(['A', 'B']).set('bar')
        await tree.cursor(['B', 'A']).set('baz')

        c = await tree.get_cursor(['A', 'A'])

        c = await c.next()
        assert c.key == ['A', 'B']
        assert c.value == 'bar'

        c = await c.next()
        assert c.key == ['B', 'A']
        assert c.value == 'baz'

        c = await c.next()
        assert c is None

    @async_test
    async def test_prev(self):
        tree = session.tree(random_tree())
        await tree.cursor(['A', 'A']).set('foo')
        await tree.cursor(['A', 'B']).set('bar')
        await tree.cursor(['B', 'A']).set('baz')

        c = await tree.get_cursor(['B', 'A'])

        c = await c.prev()
        assert c.key == ['A', 'B']
        assert c.value == 'bar'

        c = await c.prev()
        assert c.key == ['A', 'A']
        assert c.value == 'foo'

        c = await c.prev()
        assert c is None


class TestDtTx(TestCase):
    @async_test
    async def test_see_set_in_other_tx(self):
        tree = session.tree(random_tree())

        async with session.transaction() as tx:
            await tree.cursor(['A'], tx).set('foo')
            await tree.cursor(['B'], tx).set('bar')

        async with session.transaction() as tx:
            assert (await tree.get_cursor(['A'], tx)).value == 'foo'
            assert (await tree.get_cursor(['B'], tx)).value == 'bar'

    @async_test
    async def test_old_value_if_rollback(self):
        tree = session.tree(random_tree())

        async with session.transaction() as tx:
            await tree.cursor(['A'], tx).set('foo')
            await tree.cursor(['B'], tx).set('bar')
            await tx.rollback()

        async with session.transaction() as tx:
            assert (await tree.get_cursor(['A'], tx)).value is None
            assert (await tree.get_cursor(['B'], tx)).value is None


class TestDtRange(TestCase):
    @async_test
    async def test_range_all_keys(self):
        tree = session.tree(random_tree())

        await tree.cursor(['A', 'A']).set('foo')
        await tree.cursor(['A', 'B']).set('bar')
        await tree.cursor(['B', 'A']).set('baz')

        result = await tree.range()
        assert len(result) == 3
        assert result[0].key == ['A', 'A']
        assert result[0].value == 'foo'
        assert result[1].key == ['A', 'B']
        assert result[1].value == 'bar'
        assert result[2].key == ['B', 'A']
        assert result[2].value == 'baz'

    @async_test
    async def test_range_first(self):
        tree = session.tree(random_tree())

        await tree.cursor(['A', 'A']).set('foo')
        await tree.cursor(['A', 'B']).set('bar')
        await tree.cursor(['B', 'A']).set('baz')

        result = await tree.range(first=['A', 'A'])
        assert len(result) == 2
        assert result[0].key == ['A', 'B']
        assert result[0].value == 'bar'
        assert result[1].key == ['B', 'A']
        assert result[1].value == 'baz'

    @async_test
    async def test_range_last(self):
        tree = session.tree(random_tree())

        await tree.cursor(['A', 'A']).set('foo')
        await tree.cursor(['A', 'B']).set('bar')
        await tree.cursor(['B', 'A']).set('baz')

        result = await tree.range(last=['A', 'B'])
        assert len(result) == 2
        assert result[0].key == ['A', 'A']
        assert result[0].value == 'foo'
        assert result[1].key == ['A', 'B']
        assert result[1].value == 'bar'

    @async_test
    async def test_range_limit(self):
        tree = session.tree(random_tree())

        await tree.cursor(['A', 'A']).set('foo')
        await tree.cursor(['A', 'B']).set('bar')
        await tree.cursor(['B', 'A']).set('baz')

        result = await tree.range(limit=2)
        assert len(result) == 2
        assert result[0].key == ['A', 'A']
        assert result[0].value == 'foo'
        assert result[1].key == ['A', 'B']
        assert result[1].value == 'bar'


if __name__ == '__main__':
    main()
