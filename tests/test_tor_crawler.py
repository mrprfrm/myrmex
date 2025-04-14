from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from myrmex import TorCrawler

ADDRESS = "socks5://localhost:9050"
PASSWORD = "my_password"


@pytest.mark.asyncio
async def test_fetch_success():
    mock_response = AsyncMock()
    mock_response.__aenter__.return_value.text = AsyncMock(return_value="Hello")
    mock_response.__aenter__.return_value.raise_for_status = MagicMock()

    with patch("aiohttp.ClientSession.get", return_value=mock_response):
        async with TorCrawler(ADDRESS, PASSWORD) as crawler:
            result = await crawler.fetch("http://example.com")
            assert result.is_ok()
            assert result.ok() == "Hello"


@pytest.mark.asyncio
async def test_fetch_error():
    with patch("aiohttp.ClientSession.get", side_effect=Exception("Failed")):
        async with TorCrawler(ADDRESS, PASSWORD) as crawler:
            result = await crawler.fetch("http://example.com")
            assert result.is_err()
            assert isinstance(result.err(), Exception)


@pytest.mark.asyncio
async def test_rotate_ip_success():
    fake_controller = MagicMock()
    fake_controller.__enter__.return_value = fake_controller
    fake_controller.authenticate.return_value = True
    fake_controller.signal.return_value = None

    with patch("stem.control.Controller.from_port", return_value=fake_controller):
        async with TorCrawler(ADDRESS, PASSWORD) as crawler:
            result = await crawler.rotate_ip()
            assert result.is_ok()
