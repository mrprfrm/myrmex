import zipfile
from typing import IO, Sequence

import trio
from typing_extensions import Self


class AsyncIOBytes:
    def __init__(self, file: zipfile.ZipFile, info: zipfile.ZipInfo) -> None:
        self._file = file
        self._info = info
        self._buffer: IO[bytes] | None = None

    async def __aenter__(self) -> Self:
        await self.open()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback) -> None:
        await self.close()

    async def open(self) -> None:
        if self._buffer is not None:
            raise RuntimeError("Buffer is already opened")

        buffer = await trio.to_thread.run_sync(self._file.open, self._info)
        self._buffer = buffer

    async def close(self) -> None:
        buffer, self._buffer = self._buffer, None
        if buffer is None:
            return

        with trio.CancelScope(shield=True):
            await trio.to_thread.run_sync(buffer.close)

    async def read(self, size: int = -1) -> bytes:
        if self._buffer is None:
            raise RuntimeError("Buffer is not opened")
        return await trio.to_thread.run_sync(self._buffer.read, size)


class AsyncZipFile:
    def __init__(self, file: IO[bytes]) -> None:
        self._file = file

        self._zip_file: zipfile.ZipFile | None = None

    async def __aenter__(self) -> Self:
        await self.open()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback) -> None:
        await self.close()

    async def open(self) -> None:
        if self._zip_file is not None:
            raise RuntimeError("Zip file is already opened")

        def open_zip() -> zipfile.ZipFile:
            return zipfile.ZipFile(self._file, "r")

        file = await trio.to_thread.run_sync(open_zip)
        self._zip_file = file

    async def close(self) -> None:
        file, self._zip_file = self._zip_file, None
        if file is None:
            return

        with trio.CancelScope(shield=True):
            await trio.to_thread.run_sync(file.close)

    async def infolist(self) -> Sequence[zipfile.ZipInfo]:
        if self._zip_file is None:
            raise RuntimeError("Zip file is not opened")
        return await trio.to_thread.run_sync(self._zip_file.infolist)

    def extract(self, info: zipfile.ZipInfo) -> AsyncIOBytes:
        if self._zip_file is None:
            raise RuntimeError("Zip file is not opened")
        return AsyncIOBytes(self._zip_file, info)
