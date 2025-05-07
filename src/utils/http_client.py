import logging
from typing import Optional, Dict, Any, Callable, Self
from functools import partial
from aiohttp import ClientSession, ClientResponse, ClientError
from aiolimiter import AsyncLimiter

logger = logging.getLogger(__name__)


class HTTPClient:
    def __init__(self, base_url: str, headers: Optional[Dict[str, str]] = None, rate_limiter: Optional[AsyncLimiter] = None):
        self._base_url = base_url.rstrip('/')
        self._headers = headers if headers is not None else {}
        self._rate_limiter = rate_limiter
        self._session: Optional[ClientSession] = None

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
            logger.debug("HTTPClient session closed")
        return False

    async def __aenter__(self) -> Self:
        """
        Инициализация асинхронной сессии при входе в контекстный менеджер.
        """
        # Создаем новую сессию, если она еще не создана или закрыта
        if self._session is None or self._session.closed:
            self._session = ClientSession(headers=self._headers, trust_env=True) # trust_env=True может быть полезен для прокси
            logger.debug("HTTPClient session created")
        return self

    async def _request(self, method: str, url: str, **kwargs) -> Optional[Dict[str, Any]]:
        """
        Внутренний метод для выполнения HTTP запросов.
        Обрабатывает лимитирование, выполнение запроса, базовые статусы ответа и логирование.

        Args:
            method: HTTP метод (GET, POST, PATCH, DELETE).
            url: Относительный URL для запроса (например, /users).
            **kwargs: Дополнительные аргументы для session.request (params, json, data и т.д.).

        Returns:
            Словарь с JSON-ответом при успешном статусе (2xx), None в случае ошибки
            или статуса 204 (No Content).
        """
        if self._session is None or self._session.closed:
            logger.error("Attempted to make HTTP request with a closed session. Ensure client is used within an 'async with'.")
            return None
        
        full_url = f"{self._base_url}{url}"

        if self._rate_limiter:
            async with self._rate_limiter:
                pass

        try:
            async with self._session.request(method, full_url, **kwargs) as response:
                logger.debug(f"Request: {method} {full_url}, Status: {response.status}")

                if 200 <= response.status < 300:
                    if response.status == 204:
                        logger.debug(f"Request successful (204 no content): {method} {full_url}") 
                        return None
                    
                    try: 
                        json_response = await response.json()
                        return json_response
                    except Exception as json_e:
                        logger.error(f"Failed to parse JSON response for successful request {method} {full_url}: {json_e}")
                        return None
                else:
                    response_text = await response.text()
                    logger.error(f"API request failed: {method} {full_url}, Status: {response.status}, Response: {response_text}")
                    return None
        except ClientError as e:
            logger.error(f"HTTP Client Error during request {method} {full_url}: {e}")
            return None
        except Exception as e:
            logger.error(f"An unexpected error occurred during request {method} {full_url}: {e}")
            return None

    async def get(self, url: str, **kwargs) -> Optional[Dict[str, Any]]:
        """Выполняет GET запрос."""
        return await self._request('GET', url, **kwargs)

    async def post(self, url: str, json: Any, **kwargs) -> Optional[Dict[str, Any]]:
        """Выполняет POST запрос."""
        return await self._request('POST', url, json=json, **kwargs)

    async def patch(self, url: str, json: Any, **kwargs) -> Optional[Dict[str, Any]]:
        """Выполняет PATCH запрос."""
        return await self._request('PATCH', url, json=json, **kwargs)

    async def delete(self, url: str, **kwargs) -> Optional[Dict[str, Any]]:
        """Выполняет DELETE запрос."""
        return await self._request('DELETE', url, **kwargs)