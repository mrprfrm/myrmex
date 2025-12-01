import zipfile
from typing import IO, Optional, Sequence

import trio
from typing_extensions import Self


class AsyncIOBytes:
    def __init__(self, file: zipfile.ZipFile, info: zipfile.ZipInfo) -> None:
        self._file = file
        self._info = info
        self._buffer: Optional[IO[bytes]] = None

    async def __aenter__(self) -> Self:
        self._buffer = await trio.to_thread.run_sync(self._file.open, self._info)
        return self

    async def __aexit__(self, exc_type, exc_value, traceback) -> None:
        if self._buffer is not None:
            await trio.to_thread.run_sync(self._buffer.close)

    async def read(self, size: int = -1) -> bytes:
        if self._buffer is None:
            raise RuntimeError("Buffer is not opened")
        return await trio.to_thread.run_sync(self._buffer.read, size)


class AsyncZipFile:
    def __init__(self, file: IO[bytes]) -> None:
        self._file = file

        self._zip_file: zipfile.ZipFile | None = None

    async def __aenter__(self) -> Self:
        def open_zip() -> zipfile.ZipFile:
            return zipfile.ZipFile(self._file, "r")

        self._zip_file = await trio.to_thread.run_sync(open_zip)
        return self

    async def __aexit__(self, exc_type, exc_value, traceback) -> None:
        if self._zip_file is not None:
            await trio.to_thread.run_sync(self._zip_file.close)

    async def infolist(self) -> Sequence[zipfile.ZipInfo]:
        if self._zip_file is None:
            raise RuntimeError("Zip file is not opened")
        return await trio.to_thread.run_sync(self._zip_file.infolist)

    def open(self, info: zipfile.ZipInfo) -> AsyncIOBytes:
        if self._zip_file is None:
            raise RuntimeError("Zip file is not opened")
        return AsyncIOBytes(self._zip_file, info)
