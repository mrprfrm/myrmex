from io import BytesIO
from pathlib import Path
from typing import IO, Any, Callable, Coroutine, Dict, Mapping, Sequence

import httpx
from typing_extensions import Self

from .execution import LazyExecutionChain, LazyExecutionNode
from .files import Writer
from .func import detect_response_filename
from .streaming import Chunk, ContentStream
from .zip import Extractor


class ExtractResult(LazyExecutionChain[..., ContentStream]):
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
    ) -> ExtractResult:
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

        return ExtractResult(extract_function).attach(nxt)

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
