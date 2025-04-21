from io import BytesIO

import numpy as np
import pandas as pd

from src.amo.schemas import StatePurchase


class ExcelParser:
    def __init__(self, file: bytes) -> None:
        self._df = pd.read_excel(BytesIO(file))

    def parse(self) -> list[StatePurchase]:
        self._df['Дата подведения итогов'] = pd.to_datetime(self._df['Дата подведения итогов'], format="%d.%m.%Y",
                                                      errors='ignore').dt.date.replace({pd.NaT: None})
        self._df['Окончание контракта'] = pd.to_datetime(self._df['Окончание контракта'], format="%d.%m.%Y",
                                                   errors='ignore').dt.date.replace({pd.NaT: None})
        self._df.replace({np.nan: None}, inplace=True)
        data = self._df.to_dict(orient='records')
        return [StatePurchase.model_validate(item) for item in data]
