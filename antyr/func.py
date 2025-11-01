import mimetypes
import re
import zipfile
from io import BytesIO
from pathlib import Path
from typing import AsyncGenerator, Optional, Tuple
from urllib.parse import unquote_to_bytes, urlparse

import aiohttp

from .constants import CHUNK_SIZE, MAX_COMPRESSION_RATIO, MAX_FILE_SIZE, TIMEOUT

FILENAME_PATTERN = re.compile(r'filename="?([^"]+)"?', re.IGNORECASE)
CHARSET_FILENAME_PATTERN = re.compile(
    r"filename\*\s*=\s*([^']*)'[^']*'([^;]+)", re.IGNORECASE
)


def sanitize_filename(filename: str) -> str:
    filename = re.sub(r'[\\/:*?"<>|]', "_", filename)
    filename = re.sub(r"[\x00-\x1f\x7f]", "_", filename)
    while filename.startswith("."):
        filename = "_" + filename[1:]
    return filename


async def save(
    path: str | Path,
    content: bytes,
    *,
    base_path: str | Path = Path.cwd(),
    flatten: bool = False,
) -> Path:
    path = Path(path).resolve(strict=False)
    if flatten:
        while path.suffix:
            path = path.with_suffix("")

    path = (base_path / Path(path)).resolve(strict=False)
    if not path.is_relative_to(base_path):
        raise PermissionError(
            f"The file path: {path} leads outside of the root directory: {base_path}"
        )

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as file:
        file.write(content)

    return path


async def extract(
    path: str | Path,
    content: bytes,
    *extensions: str,
    max_compression_ratio: float = MAX_COMPRESSION_RATIO,
    max_file_size: int = MAX_FILE_SIZE,
) -> AsyncGenerator[Tuple[str | Path, bytes], None]:
    path = Path(path).resolve(strict=False)

    with zipfile.ZipFile(BytesIO(content), "r") as zip_ref:
        for info in zip_ref.infolist():
            filename = sanitize_filename(info.filename)
            target_path = Path(filename).resolve(strict=False)

            if info.compress_size > 0:
                ratio = info.file_size / info.compress_size
                if ratio > max_compression_ratio:
                    raise ValueError(
                        f"File {filename} exceeds the maximum compression ratio: {ratio:.2f}"
                    )

            if info.file_size > max_file_size:
                raise ValueError(f"File is too large: {filename}")

            if len(extensions) < 1 or any(
                filename.lower().endswith(ext) for ext in extensions
            ):
                with zip_ref.open(info) as file:
                    yield target_path, file.read()


async def load(
    url: str,
    *,
    session: aiohttp.ClientSession,
    timeout: Optional[aiohttp.ClientTimeout] = None,
    max_file_size: int = MAX_FILE_SIZE,
    chunk_size: int = CHUNK_SIZE,
) -> AsyncGenerator[Tuple[str | Path, bytes], None]:
    timeout = aiohttp.ClientTimeout(TIMEOUT) if timeout else timeout
    async with session.get(url, timeout=timeout) as response:
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
                    filename = sanitize_filename(raw.decode("utf-8", errors="replace"))
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
        if response.content_length is not None and max_file_size is not None:
            if response.content_length > max_file_size:
                raise ValueError(
                    f"File is too large: {filename} ({response.content_length} bytes)"
                )

        with BytesIO() as buffer:
            buffer_size = 0
            async for chunk in response.content.iter_chunked(chunk_size):
                buffer_size += len(chunk)
                if buffer_size > max_file_size:
                    raise ValueError(
                        f"File is too large: {filename} ({buffer_size} bytes)"
                    )
                buffer.write(chunk)

            path = Path(filename).resolve(strict=False)
            yield path, buffer.getvalue()
