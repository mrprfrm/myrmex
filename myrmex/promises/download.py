import asyncio
from pathlib import Path
from typing import AsyncGenerator, Awaitable, Dict, List, Tuple, TypeAlias

import aiohttp
import constants
from func import download, extract, save
from result import Err, Ok, Result

from .base import Promise

PathContent: TypeAlias = Tuple[str | Path, bytes]

AsyncResultPathStream: TypeAlias = AsyncGenerator[Result[Path, BaseException], None]
AsyncResultPathContentStream: TypeAlias = AsyncGenerator[
    Result[PathContent, BaseException], None
]

AsyncPathStream: TypeAlias = AsyncGenerator[Path, None]
AsyncPathContentStream: TypeAlias = AsyncGenerator[PathContent, None]


class SavePromise(Promise[[AsyncResultPathContentStream], AsyncResultPathStream]):
    __locks: Dict[Path, asyncio.Lock] = {}

    def __init__(
        self,
        base_path: str | Path,
        *,
        concurrency_limit: int = constants.CONCURRENCY_LIMIT,
    ) -> None:
        self._base_path = Path(base_path).resolve(strict=False)
        self.__semaphore = asyncio.Semaphore(concurrency_limit)
        super().__init__(self.__save)

    @classmethod
    def get_file_lock(cls, path: str | Path) -> asyncio.Lock:
        path = Path(path).resolve(strict=False)
        return cls.__locks.setdefault(path, asyncio.Lock())

    async def __save(
        self, stream: AsyncResultPathContentStream
    ) -> AsyncResultPathStream:
        async def task(path: str | Path, content: bytes) -> Path:
            async with self.get_file_lock(path):
                async with self.__semaphore:
                    return await save(path, content, base_path=self._base_path)

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
    Promise[[AsyncResultPathContentStream], AsyncResultPathContentStream]
):
    def __init__(
        self,
        base_path: str | Path,
        *extensions: str,
        unwrap: bool = False,
        max_file_size: int = constants.MAX_FILE_SIZE,
        max_compression_ratio: float = constants.MAX_COMPRESSION_RATIO,
        concurrency_limit: int = constants.CONCURRENCY_LIMIT,
    ) -> None:
        self._base_path = Path(base_path).resolve(strict=False)
        self._extensions = {e.lower() for e in extensions}
        self._unwrap = unwrap
        self._max_file_size = max_file_size
        self._max_compression_ratio = max_compression_ratio
        self.__semaphore = asyncio.Semaphore(concurrency_limit)

        super().__init__(self.__extract)

    async def __extract(
        self, stream: AsyncResultPathContentStream
    ) -> AsyncResultPathContentStream:
        async def task(path: str | Path, content: bytes) -> AsyncPathContentStream:
            async with self.__semaphore:
                return extract(
                    path,
                    content,
                    *self._extensions,
                    base_path=self._base_path,
                    unwrap=self._unwrap,
                    max_file_size=self._max_file_size,
                    max_compression_ratio=self._max_compression_ratio,
                )

        tasks: List[Awaitable[AsyncPathContentStream]] = []
        async for result in stream:
            match result:
                case Ok(args):
                    tasks.append(task(*args))
                case Err(err):
                    yield Err(err)

        for result in await asyncio.gather(*tasks, return_exceptions=True):
            if isinstance(result, BaseException):
                yield Err(result)
            else:
                async for st in result:
                    yield Ok(st)

    def save(self) -> SavePromise:
        promise = SavePromise(self._base_path)
        return self.chain(promise)


class DownloadPromise(Promise[[str], AsyncResultPathContentStream]):
    def __init__(
        self,
        base_path: str | Path,
        session: aiohttp.ClientSession,
        *,
        chunk_size: int = constants.CHUNK_SIZE,
        max_file_size: int = constants.MAX_FILE_SIZE,
        timeout: float = constants.TIMEOUT,
    ) -> None:
        self._base_path = Path(base_path).resolve(strict=False)
        self._session = session
        self._timeout = timeout
        self._max_file_size = max_file_size
        self._chunk_size = chunk_size
        super().__init__(self.__download)

    def __download(self, url: str) -> AsyncResultPathContentStream:
        return download(
            url,
            base_path=self._base_path,
            session=self._session,
            timeout=self._timeout,
            max_file_size=self._max_file_size,
            chunk_size=self._chunk_size,
        )

    def extract(
        self,
        *extensions: str,
        unwrap: bool = False,
        max_compression_ratio: float = constants.MAX_COMPRESSION_RATIO,
    ) -> "ExtractPromise":
        promise = ExtractPromise(
            self._base_path,
            *extensions,
            unwrap=unwrap,
            max_file_size=self._max_file_size,
            max_compression_ratio=max_compression_ratio,
        )
        return self.chain(promise)

    def save(self) -> "SavePromise":
        promise = SavePromise(self._base_path)
        return self.chain(promise)
