import mimetypes
import re
from urllib import parse

import httpx

FILENAME_PATTERN = re.compile(r'filename="?([^"]+)"?', re.IGNORECASE)
CHARSET_FILENAME_PATTERN = re.compile(
    r"filename\*\s*=\s*([^']*)'[^']*'([^;]+)", re.IGNORECASE
)


def normalize_filename(filename: str) -> str:
    filename = re.sub(r'[\\/:*?"<>|]', "_", filename)
    filename = re.sub(r"[\x00-\x1f\x7f]", "_", filename)
    while filename.startswith("."):
        filename = "_" + filename[1:]
    return filename


def detect_response_filename(response: httpx.Response) -> str:
    if disposition := response.headers.get("Content-Disposition"):
        # try to extract filename with RFC 5987
        if match := CHARSET_FILENAME_PATTERN.search(disposition):
            charset, value = match.groups()
            raw = parse.unquote_to_bytes(value)
            try:
                return normalize_filename(
                    raw.decode(charset or "utf-8", errors="replace")
                )
            except LookupError:
                return normalize_filename(raw.decode("utf-8", errors="replace"))
        elif match := FILENAME_PATTERN.search(disposition):
            return normalize_filename(match.group(1))

    # fallback to URL path extraction
    if url_name := parse.urlparse(str(response.url)).path.rsplit("/", 1)[-1] or "":
        return normalize_filename(url_name.strip())

    # fallback to default filename with content type
    content_type = response.headers.get("Content-Type", "application/octet-stream")
    content_type = content_type.split(";", 1)[0].strip()
    extension = mimetypes.guess_extension(content_type)
    return f"unknown{extension or ''}"
