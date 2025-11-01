import asyncio
from pathlib import Path
from typing import (
    AsyncGenerator,
    Awaitable,
    Coroutine,
    Dict,
    List,
    Optional,
    Tuple,
    TypeAlias,
    TypeVar,
)

import aiohttp
from result import Err, Ok, Result

from ..constants import (
    CHUNK_SIZE,
    CONCURRENCY_LIMIT,
    MAX_COMPRESSION_RATIO,
    MAX_FILE_SIZE,
    TIMEOUT,
)
from ..func import extract, load, save
from .base import Promise

T = TypeVar("T")

AsyncResultStream: TypeAlias = AsyncGenerator[Result[T, BaseException], None]


class SavePromise(
    Promise[[AsyncResultStream[Tuple[str | Path, bytes]]], AsyncResultStream[Path]]
):
    __locks: Dict[Path, asyncio.Lock] = {}

    def __init__(
        self,
        base_path: str | Path,
        *,
        flatten: bool = False,
        concurrency_limit: int = CONCURRENCY_LIMIT,
    ) -> None:
        self._base_path = Path(base_path).resolve(strict=False)
        self._flatten = flatten
        self.__semaphore = asyncio.Semaphore(concurrency_limit)
        super().__init__(self.__save)

    @classmethod
    def get_file_lock(cls, path: str | Path) -> asyncio.Lock:
        path = Path(path).resolve(strict=False)
        return cls.__locks.setdefault(path, asyncio.Lock())

    async def __save(
        self, stream: AsyncResultStream[Tuple[str | Path, bytes]]
    ) -> AsyncResultStream[Path]:
        async def task(path: str | Path, content: bytes) -> Path:
            async with self.get_file_lock(path):
                async with self.__semaphore:
                    return await save(
                        path, content, base_path=self._base_path, flatten=self._flatten
                    )

        tasks: List[Awaitable[Path]] = []
        async for result in stream:
            match result:
                case Ok(args):
                    tasks.append(task(*args))
                case Err(err):
                    yield Err(err)

        for result in await asyncio.gather(*tasks, return_exceptions=True):
            if isinstance(result, Exception):
                yield Err(result)
            elif isinstance(result, Path):
                yield Ok(result)


class ExtractPromise(
    Promise[
        [AsyncResultStream[Tuple[str | Path, bytes]]],
        AsyncResultStream[Tuple[str | Path, bytes]],
    ]
):
    def __init__(
        self,
        *extensions: str,
        unwrap: bool = False,
        max_file_size: int = MAX_FILE_SIZE,
        max_compression_ratio: float = MAX_COMPRESSION_RATIO,
        timeout: float = TIMEOUT,
        semaphore: Optional[asyncio.Semaphore] = None,
    ) -> None:
        super().__init__(self.__extract)
        self._extensions = {e.lower() for e in extensions}
        self._unwrap = unwrap
        self._max_file_size = max_file_size
        self._max_compression_ratio = max_compression_ratio
        self._timeout = timeout
        self.__semaphore = (
            asyncio.Semaphore(CONCURRENCY_LIMIT) if semaphore is None else semaphore
        )

    async def __extract(
        self, stream: AsyncResultStream[Tuple[str | Path, bytes]]
    ) -> AsyncResultStream[Tuple[str | Path, bytes]]:
        queue: asyncio.Queue[Ok | Err] = asyncio.Queue()

        async def task(path: str | Path, content: bytes) -> None:
            async with self.__semaphore:
                stream = extract(
                    path,
                    content,
                    *self._extensions,
                    max_file_size=self._max_file_size,
                    max_compression_ratio=self._max_compression_ratio,
                )
                while True:
                    try:
                        item = await asyncio.wait_for(
                            anext(stream), timeout=self._timeout
                        )
                    except StopAsyncIteration:
                        break
                    except BaseException as err:
                        await queue.put(Err(err))
                        break
                    else:
                        await queue.put(Ok(item))

        tasks: List[Coroutine[None, None, None]] = []
        async for result in stream:
            match result:
                case Ok(args):
                    tasks.append(task(*args))
                case Err(err):
                    yield Err(err)

        await asyncio.gather(*tasks)

        while not queue.empty():
            yield await queue.get()

    def save(self, path: str | Path = Path.cwd(), flatten: bool = False) -> SavePromise:
        promise = SavePromise(path, flatten=flatten)
        return self.chain(promise)


class LoadPromise(Promise[[str], AsyncResultStream[Tuple[str | Path, bytes]]]):
    def __init__(
        self,
        session: aiohttp.ClientSession,
        *,
        chunk_size: int = CHUNK_SIZE,
        max_file_size: int = MAX_FILE_SIZE,
        timeout: float = TIMEOUT,
        semaphore: Optional[asyncio.Semaphore] = None,
    ) -> None:
        super().__init__(self.__load)

        self._session = session
        self._timeout = timeout
        self._max_file_size = max_file_size
        self._chunk_size = chunk_size
        self.__semaphore = (
            asyncio.Semaphore(CONCURRENCY_LIMIT) if semaphore is None else semaphore
        )

    async def __load(self, url: str) -> AsyncResultStream[Tuple[str | Path, bytes]]:
        async def task() -> AsyncGenerator[Tuple[str | Path, bytes], None]:
            async with self.__semaphore:
                return load(
                    url,
                    session=self._session,
                    timeout=aiohttp.ClientTimeout(self._timeout),
                    max_file_size=self._max_file_size,
                    chunk_size=self._chunk_size,
                )

        for result in await asyncio.gather(task(), return_exceptions=True):
            if isinstance(result, BaseException):
                yield Err(result)
            else:
                async for path_content in result:
                    yield Ok(path_content)

    def extract(
        self, *extensions: str, max_compression_ratio: float = MAX_COMPRESSION_RATIO
    ) -> "ExtractPromise":
        promise = ExtractPromise(
            *extensions,
            max_file_size=self._max_file_size,
            max_compression_ratio=max_compression_ratio,
        )
        return self.chain(promise)

    def save(
        self, base_path: str | Path = Path.cwd(), *, flatten: bool = False
    ) -> "SavePromise":
        promise = SavePromise(base_path, flatten=flatten)
        return self.chain(promise)
