from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from myrmex import Crawler


@pytest.mark.asyncio
async def test_fetch_success():
    mock_response = AsyncMock()
    mock_response.__aenter__.return_value.text = AsyncMock(return_value="Hello")
    mock_response.__aenter__.return_value.raise_for_status = MagicMock()

    with patch("aiohttp.ClientSession.get", return_value=mock_response):
        async with Crawler() as crawler:
            result = await crawler.fetch("http://example.com")
            assert result.is_ok()
            assert result.ok() == "Hello"


@pytest.mark.asyncio
async def test_fetch_error():
    with patch("aiohttp.ClientSession.get", side_effect=Exception("Failed")):
        async with Crawler() as crawler:
            result = await crawler.fetch("http://example.com")
            assert result.is_err()
            assert isinstance(result.err(), Exception)
