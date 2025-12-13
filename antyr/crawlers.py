import socket
from typing import Any, Dict, Mapping

import httpx
import trio
from result import Err, Ok, Result
from stem import Signal
from stem.control import Controller

from .constants import CHUNK_SIZE, TIMEOUT
from .http import FetchPromise


class HttpCrawler:
    def __init__(self, base_url: str = "", *, timeout: float = TIMEOUT):
        """
        A simple crawler context manager using aiohttp.

        Args:
            timeout: request timeout in seconds
            headers: optional HTTP headers to send (e.g., User-Agent)
        """

        self._base_url = base_url
        self._timeout = timeout
        self._scope = trio.CancelScope()

    def fetch(
        self,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        cookies: Dict[str, str] | None = None,
        auth: httpx.Auth | None = None,
        proxy: str | httpx.URL | httpx.Proxy | None = None,
        follow_redirects: bool = True,
        timeout: float | None = None,
        extensions: Mapping[str, Any] | None = None,
        chunk_size: int = CHUNK_SIZE,
    ) -> FetchPromise:
        """Performs an HTTP GET request."""
        return FetchPromise(
            base_url=self._base_url,
            proxy=proxy,
            chunk_size=chunk_size,
            timeout=timeout or self._timeout,
            scope=self._scope,
        ).init(
            url,
            params=params,
            headers=headers,
            cookies=cookies,
            auth=auth,
            follow_redirects=follow_redirects,
            extensions=extensions,
        )

    async def rotate_ip(
        self, host: str, password: str, *, timeout: float | None = None
    ) -> Result[None, Exception]:
        """
        Requests a new IP address from the Tor network by sending a `NEWNYM` signal
        to the Tor control port.
        """

        def send_reset_ip_signal():
            host_ip = socket.gethostbyname(host)
            with Controller.from_port(address=host_ip) as controller:
                controller.authenticate(password=password)
                controller.signal(Signal.NEWNYM)  # type: ignore[attr-defined]

        try:
            with trio.fail_after(timeout or self._timeout):
                await trio.to_thread.run_sync(send_reset_ip_signal)
            return Ok(None)
        except Exception as e:
            return Err(e)

    async def abort(self) -> None:
        """Aborts all ongoing requests."""
        self._scope.cancel()
