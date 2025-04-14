import asyncio
import socket
from typing import Any, Hashable, Mapping, Optional, Protocol
from urllib import parse

import aiohttp
from aiohttp import ClientTimeout
from aiohttp_socks import ProxyConnector
from result import Err, Ok, Result
from stem.control import Controller, Signal


class Myrmex(Protocol):
    async def close(self) -> Result[None, Exception]: ...
    async def fetch(
        self,
        base_url: str,
        *,
        headers: Optional[Mapping[str, str]] = None,
        params: Optional[Mapping[Hashable, Any]] = None,
        timeout: Optional[int] = None,
    ) -> Result[str, Exception]: ...


class Crawler(Myrmex):
    def __init__(
        self,
        timeout: int = 10,
        headers: Optional[Mapping[str, str]] = None,
    ):
        """
        A simple crawler context manager using aiohttp.

        Args:
            timeout: request timeout in seconds
            headers: optional HTTP headers to send (e.g., User-Agent)
        """
        self._headers = headers or {}
        self._timeout = timeout
        self._session = aiohttp.ClientSession()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self._session.close()

    async def close(self) -> Result[None, Exception]:
        """
        Closes the aiohttp session.
        """
        return Ok(await self._session.close())

    async def fetch(
        self,
        base_url: str,
        *,
        headers: Optional[Mapping[str, str]] = None,
        params: Optional[Mapping[Hashable, Any]] = None,
        timeout: Optional[int] = None,
    ) -> Result[str, Exception]:
        """
        Performs an HTTP GET request.
        """
        try:
            url = f"{base_url}?{parse.urlencode(params or {})}"
            async with self._session.get(
                url,
                headers=headers or self._headers,
                timeout=ClientTimeout(timeout or self._timeout),
            ) as response:
                response.raise_for_status()
                return Ok(await response.text())
        except Exception as e:
            return Err(e)


class TorCrawler(Myrmex):
    def __init__(
        self,
        address: str,
        password: str,
        timeout: int = 10,
        headers: Optional[Mapping[str, str]] = None,
    ):
        """
        A crawler context manager using SOCKS5 proxy (e.g., Tor),
        with optional per-request IP rotation.

        Args:
            address: str - proxy URL (e.g., 'socks5h://127.0.0.1:9050')
            password: str - password for Tor control port (used for IP rotation)
            timeout: request timeout in seconds
            headers: optional HTTP headers to send (e.g., User-Agent)
        """

        self._address = address
        self._password = password
        self._host = address.split("//")[1].split(":")[0]

        self._headers = headers or {}
        self._timeout = timeout

        connector = ProxyConnector.from_url(self._address)
        self._session = aiohttp.ClientSession(connector=connector)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self._session.close()

    async def close(self) -> Result[None, Exception]:
        """
        Closes the aiohttp session.
        """
        return Ok(await self._session.close())

    async def fetch(
        self,
        base_url: str,
        *,
        headers: Optional[Mapping[str, str]] = None,
        params: Optional[Mapping[Hashable, Any]] = None,
        timeout: Optional[int] = None,
    ) -> Result[str, Exception]:
        """
        Performs an HTTP GET request through the configured Tor SOCKS5 proxy.

        Constructs a full URL by combining the `base_url` with optional query parameters,
        and executes the GET request using optional custom headers and timeout settings.
        """

        try:
            url = f"{base_url}?{parse.urlencode(params or {})}"
            async with self._session.get(
                url,
                headers=headers or self._headers,
                timeout=ClientTimeout(timeout or self._timeout),
            ) as response:
                response.raise_for_status()
                return Ok(await response.text())
        except Exception as e:
            return Err(e)

    async def rotate_ip(
        self, *, timeout: Optional[int] = None
    ) -> Result[None, Exception]:
        """
        Requests a new IP address from the Tor network by sending a `NEWNYM` signal
        to the Tor control port.
        """

        def send_reset_ip_signal(host: str):
            host_ip = socket.gethostbyname(host)
            with Controller.from_port(address=host_ip) as controller:
                controller.authenticate(password=self._password)
                controller.signal(Signal.NEWNYM)  # type: ignore[attr-defined]

        try:
            result = await asyncio.wait_for(
                asyncio.to_thread(send_reset_ip_signal, self._host),
                timeout=timeout or self._timeout,
            )
            return Ok(result)
        except Exception as e:
            return Err(e)
