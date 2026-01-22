import os
from pathlib import Path
from types import TracebackType
from typing import Tuple, Type

import trio
from typing_extensions import Self

from .constants import CONCURRENCY_LIMIT
from .streaming import ContentStream


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
