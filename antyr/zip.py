import zipfile
from types import TracebackType
from typing import IO, AsyncGenerator, Sequence, Type

import trio
from typing_extensions import Self

from .func import normalize_filename
from .streaming import Chunk, ContentStream


class OpeningAbortError(Exception):
    """
    Raised when an attempt to open a zip file is aborted.
    """

    pass


class AsyncZipMember:
    def __init__(self, info: zipfile.ZipInfo, file: zipfile.ZipFile) -> None:
        self._info = info
        self._file = file
        self._lock = trio.Lock()

        self._buffer: IO[bytes] | None = None

        self._is_open_called = False
        self._opened = trio.Event()
        self._aborted = trio.Event()

    async def __aenter__(self) -> Self:
        return await self.open()

    async def __aexit__(
        self, exc_type: Type[BaseException], exc_value: BaseException, traceback: TracebackType
    ) -> None:
        await self.close()

    async def __wait_opened(self) -> None:
        async with trio.open_nursery() as nursery:

            async def wait_state_changed(event: trio.Event) -> None:
                await event.wait()
                nursery.cancel_scope.cancel()

            nursery.start_soon(wait_state_changed, self._opened)
            nursery.start_soon(wait_state_changed, self._aborted)

        if self._aborted.is_set():
            self._is_open_called = False
            raise OpeningAbortError("Opening zip member was aborted")

    @property
    def filename(self) -> str:
        return self._info.filename

    @property
    def compress_size(self) -> int:
        return self._info.compress_size

    @property
    def file_size(self) -> int:
        return self._info.file_size

    async def open(self) -> Self:
        if self._is_open_called:
            raise RuntimeError("File is already opened or opening in progress")

        self._is_open_called = True
        self._aborted = trio.Event()

        try:
            buffer = await trio.to_thread.run_sync(self._file.open, self._info)
            self._buffer = buffer
            self._opened.set()
        except BaseException:
            self._aborted.set()
            raise
        return self

    async def close(self) -> None:
        if not self._is_open_called:
            return

        await self.__wait_opened()

        if self._buffer is None:
            raise RuntimeError("File buffer was never opened")

        with trio.CancelScope(shield=True):
            await trio.to_thread.run_sync(self._buffer.close)
            self._is_open_called = False
            self._opened = trio.Event()
            self._buffer = None

    async def chunks(self, chunk_size: int | None = None) -> AsyncGenerator[bytes, None]:
        if not self._is_open_called:
            raise RuntimeError("File was not opened")

        await self.__wait_opened()

        if self._buffer is None:
            raise RuntimeError("File buffer was never opened")

        while True:
            async with self._lock:
                data = await trio.to_thread.run_sync(self._buffer.read, chunk_size or -1)

            if not data:
                break

            yield data


class AsyncZipFile:
    def __init__(
        self,
        file: IO[bytes],
        *,
        timeout: float | None = None,
    ) -> None:
        """
        Initialize an AsyncZipFile instance.

        Args:
            file (IO[bytes]): The file-like object representing the zip file.
            concurrency_limit (int): Maximum number of concurrent operations. Default is CONCURRENCY_LIMIT.
            timeout (float | None): Optional timeout for operations. If None, no timeout is used.
        """

        self._file = file
        self._timeout = timeout

        self._zip_file: zipfile.ZipFile | None = None

        self._is_open_called = False
        self._opened = trio.Event()
        self._aborted = trio.Event()

    async def __aenter__(self) -> Self:
        await self.open()
        return self

    async def __aexit__(
        self, exc_type: Type[BaseException], exc_value: BaseException, traceback: TracebackType
    ) -> None:
        await self.close()

    async def __wait_opened(self) -> None:
        async with trio.open_nursery() as nursery:

            async def wait_state_changed(event: trio.Event) -> None:
                await event.wait()
                nursery.cancel_scope.cancel()

            nursery.start_soon(wait_state_changed, self._opened)
            nursery.start_soon(wait_state_changed, self._aborted)

        if self._aborted.is_set():
            raise OpeningAbortError("Opening zip file was aborted")

    async def open(self) -> Self:
        """
        Opens the zip file in an asynchronous manner.

        Raises:
            RuntimeError: If the zip file is already in the process of being opened.
            BaseException: If an unexpected error occurs during initialization.
        """

        if self._is_open_called:
            raise RuntimeError("Zip file is already opened or opening in progress")

        self._is_open_called = True
        self._aborted = trio.Event()

        def open_zip() -> zipfile.ZipFile:
            return zipfile.ZipFile(self._file, "r")

        try:
            file = await trio.to_thread.run_sync(open_zip)
            self._zip_file = file
            self._opened.set()
        except BaseException:
            self._is_open_called = False
            self._aborted.set()
            raise
        return self

    async def close(self) -> None:
        if not self._is_open_called:
            return

        await self.__wait_opened()

        if self._zip_file is None:
            raise RuntimeError("Zip file was never opened")

        with trio.CancelScope(shield=True):
            await trio.to_thread.run_sync(self._zip_file.close)
            self._is_open_called = False
            self._opened = trio.Event()
            self._zip_file = None

    async def files(self) -> AsyncGenerator[AsyncZipMember, None]:
        if not self._is_open_called:
            raise RuntimeError("Zip file was not opened")

        await self.__wait_opened()

        if self._zip_file is None:
            raise RuntimeError("Zip file was never opened")

        infolist = await trio.to_thread.run_sync(self._zip_file.infolist)
        for info in infolist:
            if info.is_dir():
                continue
            yield AsyncZipMember(info, self._zip_file)


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
