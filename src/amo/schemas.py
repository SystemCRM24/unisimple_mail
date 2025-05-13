from datetime import date, datetime
from typing import Optional, Iterable

from pydantic import BaseModel, Field, AliasChoices, field_validator


class StatePurchase(BaseModel):
    eis_url: Optional[str] = Field(None, alias='Закупка в ЕИС')
    winner_name: Optional[str] = Field(None, alias='Победитель')
    inn: Optional[str] = Field(None, alias="ИНН победителя")
    result_date: Optional[date] = Field(None, alias='Дата подведения итогов')
    customer_name: Optional[str] = Field(None, alias='Заказчик')
    nmck: Optional[float] = Field(None, alias='НМЦК')
    contract_securing: Optional[float] = Field(None, alias='Обеспечение контракта')
    warranty_obligations_securing: Optional[float] = Field(None, alias='Обеспечение гарантийных обязательств')
    contract_end_date: Optional[date] = Field(None, alias='Окончание контракта')
    winner_price: Optional[float] = Field(None, alias='Цена победителя')
    phone_1: Optional[str] = Field(None, alias='Телефон 1')
    fio_1: Optional[str] = Field(None, alias='ФИО 1')
    email_1: Optional[str] = Field(None, alias='Email 1')
    phone_2: Optional[str] = Field(None, alias='Телефон 2')
    fio_2: Optional[str] = Field(None, alias='ФИО 2')
    email_2: Optional[str] = Field(None, alias='Email 2')
    phone_3: Optional[str] = Field(None, alias='Телефон 3')
    fio_3: Optional[str] = Field(None, validation_alias=AliasChoices(
        'ФИО 3', 'ФИО 1.1', 'ФИО 2.1', 'ФИО 3.1'
    ))
    email_3: Optional[str] = Field(None, alias='Email 3')
    smp_advantages: Optional[str] = Field(None, alias='Преимущества СМП')
    smp_status: Optional[str] = Field(None, alias='Статус СМП у победителя')

    @field_validator('phone_1', 'phone_2', 'phone_3', 'inn', mode='before')
    def validate_numbers_to_str(cls, v: Optional[int | float]) -> Optional[str]:
        if v is not None:
            val = int(v)
            return str(val)
        return None

    @property
    def phones(self) -> Iterable[str]:
        return (phone for phone in (self.phone_1, self.phone_2, self.phone_3) if phone)

    @property
    def emails(self) -> Iterable[str]:
        return (email for email in (self.email_1, self.email_2, self.email_3) if email)

    @property
    def fios(self) -> Iterable[str]:
        return (fio for fio in (self.fio_1, self.fio_2, self.fio_3) if fio)


class DBStatePurchase(StatePurchase):
    extraction_dt: datetime
    purchase_number: str = Field(..., alias='Номер закупки')
