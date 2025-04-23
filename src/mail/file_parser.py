from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Type, Iterable

# np and pd are here just for dev speed, sorry
import numpy as np
import pandas as pd
from typing_extensions import TypeVar

from src.amo.schemas import StatePurchase, DBStatePurchase

T = TypeVar("T", bound=StatePurchase)


class ExcelParser:
    def __init__(self, file: bytes, file_name: str) -> None:
        self._df = pd.read_excel(BytesIO(file), dtype={'Номер закупки': np.str_})
        self.extraction_dt = self._get_datetime_from_file_name(file_name)

    async def parse(self) -> list[StatePurchase]:
        """
        :return: Список объектов `StatePurchase` для записи в Amo
        """
        data = await self._get_records()
        return await self._bulk_validate(StatePurchase, data)

    async def parse_for_db(self) -> list[DBStatePurchase]:
        """
        :return: Список объектов `DBStatePurchase` для записи в БД
        """
        data = await self._get_records()
        data = [{**item, "extraction_dt": self.extraction_dt} for item in data]
        return await self._bulk_validate(DBStatePurchase, data)

    async def _get_records(self) -> list[dict]:
        """
        :return: Список словарей, представляющих строки таблицы
        """
        dt_cols = ('Дата подведения итогов', 'Окончание контракта')
        for col in dt_cols:
            self._df[col] = pd.to_datetime(self._df[col], format="%d.%m.%Y",
                                                                errors='coerce').dt.date.replace({pd.NaT: None})
        self._df.replace({np.nan: None}, inplace=True)
        return self._df.to_dict(orient='records')

    @classmethod
    async def _bulk_validate(cls, model: Type[T], items: Iterable[dict]) -> list[T]:
        """
        :param model: Модель, валидирующая словари
        :param items: Список словарей к валидации
        :return: Список объектов модели
        """
        return [model.model_validate(item) for item in items]

    @classmethod
    def _get_datetime_from_file_name(cls, file_name: str) -> datetime:
        pure_name = Path(file_name).stem.replace('Победители закупок', '').strip()
        return datetime.strptime(pure_name, '%d.%m.%Y %H%M')
