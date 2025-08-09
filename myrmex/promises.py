import asyncio
import inspect
import mimetypes
import re
import zipfile
from io import BytesIO
from pathlib import Path
from typing import (
    AsyncGenerator,
    Awaitable,
    Callable,
    Dict,
    Generator,
    List,
    Optional,
    Protocol,
    Tuple,
    TypeAlias,
)
from urllib.parse import unquote_to_bytes, urlparse

import aiohttp
from result import Err, Ok, Result

MAX_FILE_SIZE = 10 * 1024 * 1024  # 10 MB
CHUNK_SIZE = 100 * 1024  # 100 KB
CONCURRENCY_LIMIT = 10
MAX_COMPRESSION_RATIO = 100.0
TIMEOUT = 10  # 10 seconds

AsyncPathContentStream: TypeAlias = AsyncGenerator[Tuple[str | Path, bytes], None]

FILENAME_PATTERN = re.compile(r'filename="?([^"]+)"?', re.IGNORECASE)
CHARSET_FILENAME_PATTERN = re.compile(
    r"filename\*\s*=\s*([^']*)'[^']*'([^;]+)", re.IGNORECASE
)


class PromiseLike(Protocol):
    def __await__(self) -> Generator[None, None, List[Result[Path, Exception]]]: ...

    def execute(self, path: str | Path, content: bytes) -> AsyncPathContentStream: ...

    def then(
        self,
        fn: Callable[
            [Tuple[str | Path, bytes]],
            AsyncPathContentStream | Awaitable[AsyncPathContentStream],
        ],
    ) -> "PromiseLike": ...


def sanitize_filename(filename: str) -> str:
    filename = re.sub(r'[\\/:*?"<>|]', "_", filename)
    filename = re.sub(r"[\x00-\x1f\x7f]", "_", filename)
    while filename.startswith("."):
        filename = "_" + filename[1:]
    return filename


class SavePromise(PromiseLike):
    __locks: Dict[Path, asyncio.Lock] = {}

    def __init__(
        self,
        path: str | Path,
        *,
        concurrency_limit: int = CONCURRENCY_LIMIT,
        future: Optional[asyncio.Future[AsyncPathContentStream]] = None,
    ) -> None:
        self._base_path = Path(path).resolve(strict=False)

        self.__semaphore = asyncio.Semaphore(concurrency_limit)

        if future is not None:
            self._future = future
            self.then(lambda x: self.execute(*x))
        else:
            self._future: asyncio.Future[AsyncPathContentStream] = (
                asyncio.get_running_loop().create_future()
            )
            self._future.set_result(self.execute("untitled", b""))

    async def execute(self, path: str | Path, content: bytes) -> AsyncPathContentStream:
        yield path, content

    @property
    def base_path(self) -> Path:
        return self._base_path

    def __await__(self) -> Generator[None, None, List[Result[Path, Exception]]]:
        async def save(path: str | Path, content: bytes) -> Path:
            path = Path(path).resolve(strict=False)
            if not path.is_relative_to(self.base_path):
                raise PermissionError(
                    f"The file path: {path} leads outside of the root directory: {self.base_path}"
                )

            async with self.get_file_lock(path):
                async with self.__semaphore:
                    path.parent.mkdir(parents=True, exist_ok=True)
                    with path.open("wb") as file:
                        file.write(content)
            return path

        async def execute() -> List[Result[Path, Exception]]:
            results: List[Result[Path, Exception]] = []
            try:
                tasks = [
                    save(path, content) async for path, content in self.generator()
                ]
                for result in await asyncio.gather(*tasks, return_exceptions=True):
                    if isinstance(result, Exception):
                        results.append(Err(result))
                    elif isinstance(result, Path):
                        results.append(Ok(result))
            except Exception as exc:
                results.append(Err(exc))
            return results

        return execute().__await__()

    @classmethod
    def get_file_lock(cls, path: str | Path) -> asyncio.Lock:
        path = Path(path).resolve(strict=False)
        return cls.__locks.setdefault(path, asyncio.Lock())

    def then(
        self,
        fn: Callable[
            [Tuple[str | Path, bytes]],
            AsyncPathContentStream | Awaitable[AsyncPathContentStream],
        ],
    ) -> "PromiseLike":
        nxt = asyncio.get_running_loop().create_future()

        async def execute(result: AsyncPathContentStream) -> AsyncPathContentStream:
            async for x in result:
                stream = fn(x)
                if inspect.isawaitable(stream):
                    stream = await stream
                if not hasattr(stream, "__aiter__"):
                    raise TypeError(
                        "then(fn) must return an async-iterable (async generator)"
                    )
                # INFO: alternative for async yield from
                async for y in stream:
                    yield y

        def callback(fut: asyncio.Future[AsyncPathContentStream]) -> None:
            try:
                if exc := fut.exception():
                    nxt.set_exception(exc)
                else:
                    result = execute(fut.result())
                    nxt.set_result(result)
            except Exception as exc:
                nxt.set_exception(exc)

        self._future.add_done_callback(callback)
        self._future = nxt
        return self

    async def generator(self) -> AsyncPathContentStream:
        async for item in await self._future:
            yield item


class LoadPromise(SavePromise):
    def __init__(
        self,
        url: str,
        path: str | Path,
        session: aiohttp.ClientSession,
        *,
        max_file_size: int = MAX_FILE_SIZE,
        chunk_size: int = CHUNK_SIZE,
        concurrency_limit: int = CONCURRENCY_LIMIT,
        timeout: float = TIMEOUT,
        future: Optional[asyncio.Future[AsyncPathContentStream]] = None,
    ) -> None:
        self._url = url
        self._session = session
        self._max_file_size = max_file_size
        self._chunk_size = chunk_size
        self._timeout = aiohttp.ClientTimeout(timeout)

        super().__init__(path, concurrency_limit=concurrency_limit, future=future)

    async def execute(self, *args, **kwargs) -> AsyncPathContentStream:
        async with self._session.get(self._url, timeout=self._timeout) as response:
            response.raise_for_status()
            filename = None
            if disposition := response.headers.get("Content-Disposition"):
                # try to extract filename with RFC 5987
                if match := CHARSET_FILENAME_PATTERN.search(disposition):
                    charset, value = match.groups()
                    raw = unquote_to_bytes(value)
                    try:
                        filename = sanitize_filename(
                            raw.decode(charset or "utf-8", errors="replace")
                        )
                    except LookupError:
                        filename = sanitize_filename(
                            raw.decode("utf-8", errors="replace")
                        )
                # fallback to simple filename extraction
                elif match := FILENAME_PATTERN.search(disposition):
                    filename = sanitize_filename(match.group(1))

            # fallback to URL path extraction
            if filename is None:
                url_name = urlparse(str(response.url)).path.rsplit("/", 1)[-1] or ""
                filename = sanitize_filename(url_name.strip()) if url_name else None

            # fallback to default filename with content type
            if filename is None:
                content_type = response.headers.get(
                    "Content-Type", "application/octet-stream"
                )
                content_type = content_type.split(";", 1)[0].strip()
                extension = mimetypes.guess_extension(content_type, strict=False)
                filename = f"unknown{extension or ''}"

            # size preflight if server provided it
            if response.content_length is not None and self._max_file_size is not None:
                if response.content_length > self._max_file_size:
                    raise ValueError(
                        f"File is too large: {filename} ({response.content_length} bytes)"
                    )

            with BytesIO() as buffer:
                buffer_size = 0
                async for chunk in response.content.iter_chunked(self._chunk_size):
                    buffer_size += len(chunk)
                    if buffer_size > self._max_file_size:
                        raise ValueError(
                            f"File is too large: {filename} ({buffer_size} bytes)"
                        )
                    buffer.write(chunk)

                path = (self.base_path / Path(filename)).resolve(strict=False)
                yield path, buffer.getvalue()

    def extract(
        self,
        *extensions: str,
        unwrap: bool = False,
        max_compression_ratio: float = MAX_COMPRESSION_RATIO,
    ) -> "ExtractPromise":
        return ExtractPromise(
            self.base_path,
            *extensions,
            unwrap=unwrap,
            max_file_size=self._max_file_size,
            max_compression_ratio=max_compression_ratio,
            future=self._future,
        )


class ExtractPromise(SavePromise):
    def __init__(
        self,
        path: Path,
        *extensions: str,
        unwrap: bool = False,
        max_file_size: int = MAX_FILE_SIZE,
        concurrency_limit: int = CONCURRENCY_LIMIT,
        max_compression_ratio: float = MAX_COMPRESSION_RATIO,
        future: Optional[asyncio.Future[AsyncPathContentStream]] = None,
    ) -> None:
        self._extensions = {e.lower() for e in extensions}
        self._unwrap = unwrap
        self._max_file_size = max_file_size
        self._max_compression_ratio = max_compression_ratio

        super().__init__(path, concurrency_limit=concurrency_limit, future=future)

    async def execute(self, path: str | Path, content: bytes) -> AsyncPathContentStream:
        path = Path(path).resolve(strict=False)
        if not path.is_relative_to(self.base_path):
            raise PermissionError(
                f"The file path: {path} leads outside of the root directory: {self.base_path}"
            )

        base_path = self.base_path
        if not self._unwrap:
            base_path = path
            while base_path.suffix:
                base_path = base_path.with_suffix("")

        with zipfile.ZipFile(BytesIO(content), "r") as zip_ref:
            for info in zip_ref.infolist():
                filename = sanitize_filename(info.filename)
                target_path = (base_path / Path(filename)).resolve(strict=False)
                if not target_path.is_relative_to(base_path):
                    raise PermissionError(
                        f"The file path: {target_path} leads outside of the root directory: {self.base_path}"
                    )

                if info.compress_size > 0:
                    ratio = info.file_size / info.compress_size
                    if ratio > self._max_compression_ratio:
                        raise ValueError(
                            f"File {filename} exceeds the maximum compression ratio: {ratio:.2f}"
                        )

                if info.file_size > self._max_file_size:
                    raise ValueError(f"File is too large: {filename}")

                if len(self._extensions) < 1 or any(
                    filename.lower().endswith(ext) for ext in self._extensions
                ):
                    with zip_ref.open(info) as file:
                        yield target_path, file.read()
