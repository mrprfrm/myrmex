import socket
from types import TracebackType
from typing import Any, Dict, Mapping, Type

import httpx
import trio
from stem import Signal
from stem.control import Controller

from .constants import TIMEOUT
from .http import FetchResult


class HttpCrawler:
    def __init__(
        self,
        base_url: str = "",
        *,
        timeout: float = TIMEOUT,
        proxy: str | httpx.URL | httpx.Proxy | None = None,
    ):
        """
        A simple crawler context manager using httpx.

        Args:
            timeout: request timeout in seconds
            headers: optional HTTP headers to send (e.g., User-Agent)
        """

        self._base_url = base_url
        self._proxy = proxy
        self._timeout = timeout
        self._client = httpx.AsyncClient(base_url=base_url, proxy=proxy, timeout=timeout)

    async def __aenter__(self) -> "HttpCrawler":
        await self._client.__aenter__()
        return self

    async def __aexit__(
        self, exc_type: Type[BaseException], exc: BaseException, tb: TracebackType
    ) -> None:
        await self._client.__aexit__(exc_type, exc, tb)

    def fetch(
        self,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        cookies: Dict[str, str] | None = None,
        auth: httpx.Auth | None = None,
        follow_redirects: bool = True,
        timeout: float | None = None,
        extensions: Mapping[str, Any] | None = None,
    ) -> FetchResult:
        """Performs an HTTP GET request."""

        return FetchResult(self._client.get).init(
            url,
            params=params,
            headers=headers,
            cookies=cookies,
            auth=auth,
            follow_redirects=follow_redirects,
            timeout=timeout if timeout is not None else self._timeout,
            extensions=extensions,
        )

    async def rotate_ip(self, host: str, password: str) -> None:
        """
        Requests a new IP address from the Tor network by sending a `NEWNYM` signal
        to the Tor control port.
        """

        def send_reset_ip_signal():
            host_ip = socket.gethostbyname(host)
            with Controller.from_port(address=host_ip) as controller:
                controller.authenticate(password=password)
                controller.signal(Signal.NEWNYM)  # type: ignore[attr-defined]

        await trio.to_thread.run_sync(send_reset_ip_signal)
