from dataclasses import dataclass
from typing import AsyncGenerator, TypeAlias


@dataclass
class Chunk:
    path: str
    content: bytes
    offset: int = 0
    length: int = 0


ContentStream: TypeAlias = AsyncGenerator[Chunk, None]
