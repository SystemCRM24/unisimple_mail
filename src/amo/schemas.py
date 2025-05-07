# src/amo/schemas.py
from datetime import date, datetime
from typing import Optional

from pydantic import BaseModel, Field, EmailStr, AliasChoices, field_validator


class StatePurchase(BaseModel):
    """
    Схема для данных о закупке, спарсенных из Excel.
    """
    purchase_number: str = Field(..., alias='Номер закупки')

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
    email_1: Optional[EmailStr] = Field(None, alias='Email 1')
    phone_2: Optional[str] = Field(None, alias='Телефон 2')
    fio_2: Optional[str] = Field(None, alias='ФИО 2')
    email_2: Optional[EmailStr] = Field(None, alias='Email 2')
    phone_3: Optional[str] = Field(None, alias='Телефон 3')
    fio_3: Optional[str] = Field(None, validation_alias=AliasChoices(
        'ФИО 3', 'ФИО 1.1', 'ФИО 2.1', 'ФИО 3.1'
    ))
    email_3: Optional[EmailStr] = Field(None, alias='Email 3')

    smp_advantages: Optional[str] = Field(None, alias='Преимущества СМП')
    smp_status: Optional[str] = Field(None, alias='Статус СМП у победителя')

    @field_validator('phone_1', 'phone_2', 'phone_3', 'inn', mode='before')
    def validate_numbers_to_str(cls, v: Optional[int | float | str]) -> Optional[str]:
        if v is not None and not isinstance(v, str):
            try:
                val = int(v)
                return str(val)
            except (ValueError, TypeError):
                return str(v) if v is not None else None
        return v
    
    @property
    def full_purchase_link(self) -> str:
        """Формирует полную ссылку на закупку из номера и ссылки ЕИС."""
        parts = []
        if self.purchase_number:
            parts.append(f"Номер закупки: {self.purchase_number}")
        if self.eis_url:
            parts.append(f"Ссылка в ЕИС: {self.eis_url}")

        return ", ".join(parts) if parts else "Ссылка на закупку: Нет данных"

    @property
    def contact_details(self) -> str:
        """Собирает все доступные контактные данные (ФИО, Телефон, Email) в строку."""
        details = []
        for i in range(1, 4):
            phone = getattr(self, f'phone_{i}', None)
            fio = getattr(self, f'fio_{i}', None)
            email = getattr(self, f'email_{i}', None)

            contact_parts = []
            if fio:
                contact_parts.append(fio)
            if phone:
                contact_parts.append(f"Тел: {phone}")
            if email:
                contact_parts.append(f"Email: {email}")

            if contact_parts:
                details.append(f"Контакт {i}: ({', '.join(contact_parts)})")

        return "; ".join(details) if details else "Контактные данные: Нет данных"


class DBStatePurchase(StatePurchase):
    """
    Схема для данных о закупке перед записью в БД.
    Наследует от StatePurchase и добавляет специфичные для БД поля/логику.
    """
    extraction_dt: datetime

    pass
