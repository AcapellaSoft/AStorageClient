import asyncio
from asyncio import AbstractEventLoop
from typing import List
from urllib.parse import quote

from requests import Response
from requests import Session

from acapella.kv.utils.errors import CasError, TransactionNotFoundError, TransactionCompletedError, KvError


def key_to_str(key: List[str]) -> str:
    return ':'.join(quote(part) for part in key)


def raise_if_error(code: int):
    if code == 200:
        return
    if code == 408:
        raise TimeoutError()
    if code == 409:
        raise CasError()
    if code == 410:
        raise TransactionNotFoundError()
    if code == 412:
        raise TransactionCompletedError()
    raise KvError(f'Unexpected server error with code {code}')


class AsyncSession(object):
    def __init__(self, session: Session = None, loop: AbstractEventLoop = None, base_url: str = ''):
        self._session = session or Session()
        self._loop = loop or asyncio.get_event_loop()
        self._base_url = base_url

    def _async(self, fn, *args, **kwargs):
        return self._loop.run_in_executor(None, lambda: fn(*args, **kwargs))

    async def get(self, url, **kwargs) -> Response:
        return await self._async(self._session.get, self._base_url + url, **kwargs)

    async def options(self, url, **kwargs) -> Response:
        return await self._async(self._session.options, self._base_url + url, **kwargs)

    async def head(self, url, **kwargs) -> Response:
        return await self._async(self._session.head, self._base_url + url, **kwargs)

    async def post(self, url, data=None, json=None, **kwargs) -> Response:
        return await self._async(self._session.post, self._base_url + url, data=data, json=json, **kwargs)

    async def put(self, url, data=None, **kwargs) -> Response:
        return await self._async(self._session.put, self._base_url + url, data=data, **kwargs)

    async def patch(self, url, data=None, **kwargs) -> Response:
        return await self._async(self._session.patch, self._base_url + url, data=data, **kwargs)

    async def delete(self, url, **kwargs) -> Response:
        return await self._async(self._session.delete, self._base_url + url, **kwargs)
