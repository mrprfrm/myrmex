<img src="https://github.com/user-attachments/assets/fdcf9749-a847-474c-a03d-e6c92f630635" alt="Alt Text" max-height="250">

# myrmex

The compact web crawling toolkit.

Unlike full-featured frameworks, `myrmex` does not implement an entire scraping pipeline. Instead, it focuses exclusively on core crawling functionality. Higher-level scraping logic is left to the specific implementation of your scraper.

> If you're looking for a complete scraping framework, consider [Scrapy](https://github.com/scrapy/scrapy).

`myrmex` provides a minimal interface through two crawler classes — `Crawler` and `TorCrawler` — for regular HTTP crawling and Tor-based anonymous crawling, respectively.

### Key Capabilities

- Asynchronous context management for automatic resource handling
- Built on `aiohttp` for HTTP requests
- Executes synchronous operations using the native asyncio thread pool (non-blocking)
- Functional-style error handling via [Result](https://github.com/rustedpy/result)
- Configurable per-operation timeouts for robust request management

## Installation

Install via pip:

```bash
pip install myrmex
```

Or using [uv](https://github.com/astral-sh/uv):

```bash
uv add myrmex
```

Please note that the following libraries will be installed alongside `myrmex`:

- `aiohttp` – for HTTP requests
- `aiohttp-socks` – for SOCKS5 proxy support
- `stem` – for Tor control port integration
- `result` – for functional-style error handling

## Configuration

`Crawler` accepts the following options:

| Parameter | Type   | Default | Description                                |
| --------- | ------ | ------- | ------------------------------------------ |
| `timeout` | `int`  | `10`    | Timeout (in seconds) for HTTP requests.    |
| `headers` | `dict` | `None`  | HTTP headers to include with each request. |

…and `TorCrawler` accepts the following options during initialization:

| Parameter  | Type   | Default | Description                                                  |
| ---------- | ------ | ------- | ------------------------------------------------------------ |
| `address`  | `str`  | `None`  | SOCKS5 proxy address for routing traffic through Tor.        |
| `password` | `str`  | `None`  | Control port password for authenticating with the Tor proxy. |
| `timeout`  | `int`  | `10`    | Timeout (in seconds) for HTTP requests.                      |
| `headers`  | `dict` | `None`  | HTTP headers to include with each request.                   |

## Usage Example

The example below demonstrates how to fetch your current IP address over the Tor network:

```python
import asyncio
from myrmex import TorCrawler

async def main():
    async with TorCrawler("socks5h://127.0.0.1:9050", password="password") as crawler:
        await crawler.rotate_ip()  # optional: rotates IP before request
        result = await crawler.fetch("http://httpbin.org/ip")
        if result.is_ok():
            print("Current IP:", result.unwrap())

asyncio.run(main())
```

## Tor Setup

Since `TorCrawler` is strictly associated with Tor network usage, ensure that you have a configured and running Tor instance before using it.

Update your `torrc` configuration file with the following:

```torrc
SocksPort 0.0.0.0:9050
ControlPort 0.0.0.0:9051
HashedControlPassword ***
```

To generate a hashed password:

```bash
tor --hash-password your_password
```

Start Tor manually in the background:

```bash
tor &
```
