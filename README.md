<img src="https://github.com/user-attachments/assets/fdcf9749-a847-474c-a03d-e6c92f630635" alt="Alt Text" max-height="250">

# antyr

The compact web crawling toolkit.

Unlike full-featured frameworks, `antyr` does not implement an entire scraping pipeline. Instead, it focuses exclusively on core crawling functionality. Higher-level scraping logic is left to the specific implementation of your crawler or processing pipeline.

> If you're looking for a complete scraping framework, consider Scrapy.

`antyr` provides a minimal interface through a single crawler class — `HttpCrawler` — responsible for HTTP fetching with optional proxy support.

## Key Capabilities

- Asynchronous context management for explicit resource handling
- Built on **Trio** for structured concurrency
- Uses **httpx** for HTTP requests
- Native SOCKS / proxy support via `httpx[socks]`
- Minimal API surface with no framework-level assumptions

## Installation

Install via pip:

```bash
pip install antyr
```

Or using [uv](https://github.com/astral-sh/uv):

```bash
uv add antyr
```

Please note that the following libraries will be installed alongside `antyr`:

- `trio` – structured async runtime
- `httpx[socks]` – HTTP client with SOCKS proxy support
- `stem` – Tor control integration (optional)

## Configuration

`HttpCrawler` accepts the following options:

| Parameter  | Type                                      | Default | Description                                   |
| ---------- | ----------------------------------------- | ------- | --------------------------------------------- |
| `base_url` | `str`                                     | `""`    | Base URL for resolving relative request paths |
| `timeout`  | `float`                                   | `10`    | Timeout (in seconds) for HTTP requests        |
| `proxy`    | `str \| httpx.URL \| httpx.Proxy \| None` | `None`  | Optional HTTP or SOCKS proxy                  |

## Usage Example

The example below demonstrates fetching a resource using a base URL and relative path:

```python
import trio
from antyr import HttpCrawler

async def main():
    async with HttpCrawler("http://httpbin.org") as crawler:
        result = await crawler.fetch("/ip", follow_redirects=True).save()
        print(result)

trio.run(main)
```
