from typing import Self

from aiohttp import ClientSession
from aiolimiter import AsyncLimiter

from src.settings import settings


class AmoClient:
    _session: ClientSession
    _MAX_REQUESTS_PER_SECOND = 2

    def __init__(self):
        self._headers = {'Authorization': f'Bearer {settings.amo_long_term_token}', 'Content-Type': 'application/json'}
        self._base_url = f"https://{settings.amo_subdomain}.amocrm.ru/api"
        self._rate_limit = AsyncLimiter(self._MAX_REQUESTS_PER_SECOND, 1)

    async def __aenter__(self) -> Self:
        self._session = ClientSession(headers=self._headers, trust_env=True)
        return self

    async def __aexit__(self, *args) -> None:
        await self._session.close()
        return
