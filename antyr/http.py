import dataclasses
import os
from io import BytesIO
from pathlib import Path
from types import TracebackType
from typing import (
    IO,
    Any,
    AsyncGenerator,
    Callable,
    Coroutine,
    Dict,
    Mapping,
    Sequence,
    Tuple,
    Type,
    TypeAlias,
)

import httpx
import trio
from typing_extensions import Self

from .constants import CONCURRENCY_LIMIT
from .execution import LazyExecutionChain, LazyExecutionNode
from .func import detect_response_filename, normalize_filename
from .utils.zip import AsyncZipFile


@dataclasses.dataclass
class Chunk:
    path: str
    content: bytes
    offset: int = 0
    length: int = 0


ContentStream: TypeAlias = AsyncGenerator[Chunk, None]


class Writer:
    def __init__(
        self,
        base_path: str | Path,
        *,
        concurrency_limit: int = CONCURRENCY_LIMIT,
    ) -> None:
        self._semaphore = trio.Semaphore(concurrency_limit)
        self._base_path = Path(base_path).resolve(strict=False)
        self._pool: dict[Path, int] = {}

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self, exc_type: Type[BaseException], exc_value: BaseException, traceback: TracebackType
    ) -> None:
        for fd in self._pool.values():
            os.close(fd)

    async def save(self, stream: ContentStream) -> None:
        sender, receiver = trio.open_memory_channel[Tuple[Path, bytes, int]](10)

        def save_function(path: Path, content: bytes, offset: int) -> None:
            if path not in self._pool:
                self._pool[path] = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)

            fd = self._pool[path]
            os.pwrite(fd, content, offset)

        async def save_processor() -> None:
            async for path, content, offset in receiver:
                async with self._semaphore:
                    await trio.to_thread.run_sync(save_function, path, content, offset)

        async with trio.open_nursery() as nursery:
            nursery.start_soon(save_processor)

            async for chunk in stream:
                target_path = (self._base_path / Path(chunk.path)).resolve(strict=False)

                if not target_path.is_relative_to(self._base_path):
                    raise PermissionError(
                        f"The file path: {target_path} leads outside of the root directory: {self._base_path}"
                    )

                target_path.parent.mkdir(parents=True, exist_ok=True)

                await sender.send((target_path, chunk.content, chunk.offset))

            await sender.aclose()


class Extractor:
    def __init__(self, file: IO[bytes]) -> None:
        self._file = AsyncZipFile(file)

    async def __aenter__(self) -> "Extractor":
        await self._file.open()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback) -> None:
        await self._file.close()

    async def extract(
        self,
        *,
        file_extensions: Sequence[str] | None = None,
        max_compression_ratio: float | None = None,
        max_file_size: int | None = None,
        chunk_size: int | None = None,
    ) -> ContentStream:
        async for member in self._file.files():
            filename = normalize_filename(member.filename)

            if file_extensions is not None and not any(
                filename.lower().endswith(ext) for ext in file_extensions
            ):
                continue

            if member.compress_size > 0:
                ratio = member.file_size / member.compress_size
                if max_compression_ratio is not None and ratio > max_compression_ratio:
                    raise ValueError(
                        f"File {filename} exceeds the maximum compression ratio: {ratio:.2f}"
                    )

            if max_file_size is not None and member.file_size > max_file_size:
                raise ValueError(f"File is too large: {filename}")

            async with member:
                total = 0

                async for chunk in member.chunks(chunk_size):
                    offset, length = total, len(chunk)
                    total += length
                    yield Chunk(
                        path=str(filename),
                        content=bytes(chunk),
                        offset=offset,
                        length=length,
                    )


class ExtractPromise(LazyExecutionChain[..., ContentStream]):
    def __init__(self, fn: Callable[[IO[bytes]], Coroutine[None, None, ContentStream]]) -> None:
        super().__init__(fn)

    def save(self, base_path: str | Path = Path.cwd()) -> LazyExecutionNode[[ContentStream], None]:
        async def save_function(stream: ContentStream) -> None:
            async with Writer(base_path) as writer:
                await writer.save(stream)

        return self.then(save_function)


class FetchResult(LazyExecutionChain[..., httpx.Response]):
    def __init__(self, fn: Callable[..., Coroutine[None, None, httpx.Response]]) -> None:
        super().__init__(fn)

    async def __process_content_stream(
        self,
        response: httpx.Response,
        *,
        max_content_length: int | None = None,
        chunk_size: int | None = None,
    ) -> ContentStream:
        if max_content_length is not None:
            if content_length := response.headers.get("Content-Length"):
                if content_length.isdigit() and int(content_length) > max_content_length:
                    await response.aclose()
                    raise ValueError(f"File is too large: {content_length} bytes")

        async def generator() -> ContentStream:
            try:
                total = 0
                filename = detect_response_filename(response)
                async for chunk in response.aiter_bytes(chunk_size):
                    offset, length = total, len(chunk)
                    total += length
                    if max_content_length is not None and total > max_content_length:
                        await response.aclose()
                        raise ValueError(f"File is too large: {total} bytes")
                    yield Chunk(
                        path=filename,
                        content=chunk,
                        offset=offset,
                        length=length,
                    )
            finally:
                await response.aclose()

        return generator()

    async def __process_bytesio(
        self,
        response: httpx.Response,
        *,
        max_content_length: int | None = None,
        chunk_size: int | None = None,
    ) -> IO[bytes]:
        if max_content_length is not None:
            if content_length := response.headers.get("Content-Length"):
                if content_length.isdigit() and int(content_length) > max_content_length:
                    await response.aclose()
                    raise ValueError(f"File is too large: {content_length} bytes")

        buf = BytesIO()
        try:
            total = 0
            async for chunk in response.aiter_bytes(chunk_size):
                total += len(chunk)
                if max_content_length is not None and total > max_content_length:
                    await response.aclose()
                    raise ValueError(f"File is too large: {total} bytes")
                buf.write(chunk)
            buf.seek(0)
        finally:
            await response.aclose()
        return buf

    def init(
        self,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        cookies: Dict[str, str] | None = None,
        auth: httpx.Auth | None = None,
        follow_redirects: bool = True,
        timeout: float | None = None,
        extensions: Mapping[str, Any] | None = None,
    ) -> Self:
        super().init(
            url,
            params=params,
            headers=headers,
            cookies=cookies,
            auth=auth,
            follow_redirects=follow_redirects,
            timeout=timeout,
            extensions=extensions,
        )
        return self

    async def content_stream(
        self, *, max_content_length: int | None = None, chunk_size: int | None = None
    ) -> ContentStream:
        return await self.then(
            self.__process_content_stream,
            max_content_length=max_content_length,
            chunk_size=chunk_size,
        )

    async def bytesio(
        self, *, max_content_length: int | None = None, chunk_size: int | None = None
    ) -> IO[bytes]:
        return await self.then(
            self.__process_bytesio,
            max_content_length=max_content_length,
            chunk_size=chunk_size,
        )

    def extract(
        self,
        file_extensions: Sequence[str] | None = None,
        *,
        max_content_length: int | None = None,
        chunk_size: int | None = None,
        max_file_size: int | None = None,
        max_compression_ratio: float | None = None,
    ) -> ExtractPromise:
        nxt = self.then(
            self.__process_bytesio, max_content_length=max_content_length, chunk_size=chunk_size
        )

        async def extract_function(content: IO[bytes]) -> ContentStream:
            async def generator() -> ContentStream:
                async with Extractor(content) as extractor:
                    async for chunk in extractor.extract(
                        file_extensions=file_extensions,
                        max_file_size=max_file_size,
                        max_compression_ratio=max_compression_ratio,
                        chunk_size=chunk_size,
                    ):
                        yield chunk

            return generator()

        return ExtractPromise(extract_function).attach(nxt)

    def save(
        self,
        base_path: str | Path = Path.cwd(),
        *,
        max_content_length: int | None = None,
        chunk_size: int | None = None,
    ) -> LazyExecutionNode[[ContentStream], None]:
        nxt = self.then(
            self.__process_content_stream,
            max_content_length=max_content_length,
            chunk_size=chunk_size,
        )

        async def save_function(stream: ContentStream) -> None:
            async with Writer(base_path) as writer:
                await writer.save(stream)

        return nxt.then(save_function)
