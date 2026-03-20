import aiohttp
import asyncio
import logging

logger = logging.getLogger(__name__)

class SessionManager:
    """Manages a global aiohttp.ClientSession to reuse connections."""
    _session: aiohttp.ClientSession = None

    @classmethod
    async def get_session(cls) -> aiohttp.ClientSession:
        if cls._session is None or cls._session.closed:
            timeout = aiohttp.ClientTimeout(total=30)
            cls._session = aiohttp.ClientSession(timeout=timeout)
            logger.info("Global aiohttp.ClientSession created.")
        return cls._session

    @classmethod
    async def close_session(cls):
        if cls._session and not cls._session.closed:
            await cls._session.close()
            logger.info("Global aiohttp.ClientSession closed.")

async def fetch_json(url: str, params=None, headers=None) -> dict:
    session = await SessionManager.get_session()
    async with session.get(url, params=params, headers=headers) as response:
        response.raise_for_status()
        return await response.json()
