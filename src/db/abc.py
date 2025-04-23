from abc import ABC, abstractmethod
from collections.abc import Iterable
from typing import Self

from src.amo.schemas import DBStatePurchase


class DB(ABC):
    """Абстрактный класс для реализации работы с БД"""
    async def __aenter__(self) -> Self:
        print('Connecting to DB...')
        self._conn = await self._get_connection()
        print('Connected...')
        return self

    async def __aexit__(self, *args, **kwargs) -> None:
        await self._close_connection()

    @abstractmethod
    async def write_purchases(self, purchases: Iterable[DBStatePurchase]) -> None:
        """
        Записывает коллекцию закупок в БД
        :param purchases: Закупки
        """
        ...

    @abstractmethod
    async def _get_connection(self):
        """
        :return: Подключение к БД
        """
        ...

    @abstractmethod
    async def _close_connection(self):
        """
        Закрывает соединение к БД
        """
        ...
