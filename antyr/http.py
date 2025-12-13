import dataclasses
import os
from io import BytesIO
from pathlib import Path
from typing import Any, AsyncGenerator, Dict, Mapping, Sequence, Tuple, TypeAlias, cast

import httpx
import trio
from typing_extensions import Self

from .constants import (
    CHUNK_SIZE,
    CONCURRENCY_LIMIT,
    MAX_COMPRESSION_RATIO,
    MAX_CONTENT_LENGTH,
    MAX_FILE_SIZE,
    TIMEOUT,
)
from .func import detect_response_filename, normalize_filename
from .promises import Promise
from .utils.zip import AsyncZipFile


@dataclasses.dataclass
class Chunk:
    path: str
    content: bytes
    offset: int = 0
    length: int = 0


ContentStream: TypeAlias = AsyncGenerator[Chunk, None]


class SavePromise(Promise[[ContentStream], ContentStream]):
    def __init__(
        self,
        base_path: str | Path,
        *,
        concurrency_limit: int = CONCURRENCY_LIMIT,
        workers_limit: int = 1,
        timeout: float = TIMEOUT,
        scope: trio.CancelScope | None = None,
    ) -> None:
        self._base_path = Path(base_path).resolve(strict=False)
        self._timeout = timeout
        self._processors_limit = workers_limit
        self._semaphore = trio.Semaphore(concurrency_limit)
        super().__init__(self.__process_save, scope=scope)

    async def __process_save(self, stream: ContentStream) -> ContentStream:
        sender, receiver = trio.open_memory_channel[Tuple[Path, bytes, int]](10)

        async def save_processor() -> None:
            async for path, content, offset in receiver:
                async with self._semaphore:
                    with trio.fail_after(self._timeout):
                        fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
                        try:
                            await trio.to_thread.run_sync(
                                os.pwrite, fd, content, offset
                            )
                        finally:
                            os.close(fd)

        async def generator() -> ContentStream:
            async with trio.open_nursery() as nursery:
                for _ in range(self._processors_limit):
                    nursery.start_soon(save_processor)

                async for chunk in stream:
                    target_path = (self._base_path / Path(chunk.path)).resolve(
                        strict=False
                    )

                    if not target_path.is_relative_to(self._base_path):
                        raise PermissionError(
                            f"The file path: {target_path} leads outside of the root directory: {self._base_path}"
                        )

                    target_path.parent.mkdir(parents=True, exist_ok=True)

                    await sender.send((target_path, chunk.content, chunk.offset))
                    yield chunk

                await sender.aclose()

        return generator()


class ExtractPromise(Promise[..., ContentStream]):
    def __init__(
        self,
        *,
        file_extensions: Sequence[str] | None = None,
        timeout: float = TIMEOUT,
        scope: trio.CancelScope | None = None,
    ) -> None:
        self._file_extensions = file_extensions
        self._timeout = timeout

        super().__init__(self.__process_extract, scope=scope)

    async def __process_extract(
        self,
        content: BytesIO,
        chunk_size: int = CHUNK_SIZE,
        *,
        max_file_size: int = MAX_FILE_SIZE,
        max_compression_ratio: float = MAX_COMPRESSION_RATIO,
        timeout: float | None = None,
    ) -> ContentStream:

        async def generator() -> ContentStream:
            async with AsyncZipFile(content) as zip_ref:
                with trio.fail_after(timeout or self._timeout):
                    for info in await zip_ref.infolist():
                        filename = normalize_filename(info.filename)

                        if self._file_extensions is not None and not any(
                            filename.lower().endswith(ext)
                            for ext in self._file_extensions
                        ):
                            continue

                        if info.compress_size > 0:
                            ratio = info.file_size / info.compress_size
                            if ratio > max_compression_ratio:
                                raise ValueError(
                                    f"File {filename} exceeds the maximum compression ratio: {ratio:.2f}"
                                )

                        if info.file_size > max_file_size:
                            raise ValueError(f"File is too large: {filename}")

                        async with zip_ref.extract(info) as file:
                            total = 0
                            while chunk := await file.read(chunk_size):
                                offset, length = total, len(chunk)
                                total += length
                                yield Chunk(
                                    path=str(filename),
                                    content=bytes(chunk),
                                    offset=offset,
                                    length=length,
                                )

        return generator()

    def init(
        self,
        content: BytesIO,
        chunk_size: int = CHUNK_SIZE,
        *,
        max_file_size: int = MAX_FILE_SIZE,
        max_compression_ratio: float = MAX_COMPRESSION_RATIO,
    ) -> Self:
        super().init(
            content,
            chunk_size=chunk_size,
            max_file_size=max_file_size,
            max_compression_ratio=max_compression_ratio,
        )
        return self

    def save(
        self, base_path: str | Path = Path.cwd(), *, timeout: float | None = None
    ) -> SavePromise:
        promise = SavePromise(base_path, timeout=timeout or self._timeout)
        return cast(SavePromise, self.then(promise))


class FetchPromise(Promise[..., httpx.Response]):
    def __init__(
        self,
        base_url: str = "",
        *,
        proxy: str | httpx.URL | httpx.Proxy | None = None,
        chunk_size: int = CHUNK_SIZE,
        timeout: float = TIMEOUT,
        scope: trio.CancelScope | None = None,
    ) -> None:
        self._timeout = timeout
        self._chunk_size = chunk_size
        self._client = httpx.AsyncClient(
            timeout=timeout, base_url=base_url, proxy=proxy
        )
        super().__init__(self.__process_fetch, scope=scope)

    async def __process_fetch(
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
    ) -> httpx.Response:
        response = await self._client.get(
            url,
            params=params,
            headers=headers,
            cookies=cookies,
            auth=auth,
            follow_redirects=follow_redirects,
            timeout=timeout if timeout is not None else self._timeout,
            extensions=extensions,
        )
        try:
            response.raise_for_status()
        except BaseException:
            await response.aclose()
            raise
        return response

    async def __process_content_stream(
        self, response: httpx.Response, *, max_content_length: int = MAX_CONTENT_LENGTH
    ) -> ContentStream:
        if content_length := response.headers.get("Content-Length"):
            if content_length.isdigit() and int(content_length) > max_content_length:
                await response.aclose()
                raise ValueError(f"File is too large: {content_length} bytes")

        async def generator() -> ContentStream:
            try:
                total = 0
                filename = detect_response_filename(response)
                async for chunk in response.aiter_bytes(self._chunk_size):
                    offset, length = total, len(chunk)
                    total += length
                    if total > max_content_length:
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
        self, response: httpx.Response, *, max_content_length: int = MAX_CONTENT_LENGTH
    ) -> BytesIO:
        if content_length := response.headers.get("Content-Length"):
            if content_length.isdigit() and int(content_length) > max_content_length:
                await response.aclose()
                raise ValueError(f"File is too large: {content_length} bytes")

        content = BytesIO()
        try:
            total = 0
            async for chunk in response.aiter_bytes(self._chunk_size):
                total += len(chunk)
                if total > max_content_length:
                    await response.aclose()
                    raise ValueError(f"File is too large: {total} bytes")
                content.write(chunk)
            content.seek(0)
        finally:
            await response.aclose()
        return content

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

    async def content_stream(self) -> ContentStream:
        return await self.then(self.__process_content_stream)

    async def bytesio(self) -> BytesIO:
        return await self.then(self.__process_bytesio)

    def extract(
        self,
        file_extensions: Sequence[str] | None = None,
        *,
        max_content_length: int = MAX_CONTENT_LENGTH,
        chunk_size: int = CHUNK_SIZE,
        max_file_size: int = MAX_FILE_SIZE,
        max_compression_ratio: float = MAX_COMPRESSION_RATIO,
        timeout: float | None = None,
    ) -> ExtractPromise:
        promise = ExtractPromise(
            file_extensions=file_extensions, timeout=timeout or self._timeout
        )
        nxt = self.then(self.__process_bytesio, max_content_length=max_content_length)
        return cast(
            ExtractPromise,
            nxt.then(
                promise,
                chunk_size=chunk_size,
                max_file_size=max_file_size,
                max_compression_ratio=max_compression_ratio,
            ),
        )

    def save(
        self,
        base_path: str | Path = Path.cwd(),
        *,
        max_content_length: int = MAX_CONTENT_LENGTH,
        timeout: float | None = None,
    ) -> SavePromise:
        promise = SavePromise(base_path, timeout=timeout or self._timeout)
        nxt = self.then(
            self.__process_content_stream, max_content_length=max_content_length
        )
        return cast(SavePromise, nxt.then(promise))
