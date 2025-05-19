from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from pathlib import Path
from typing import Optional, List

from .enums import AppMode

BASE_DIR = Path(__file__).resolve().parent.parent.parent


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=BASE_DIR / '.env',
        env_file_encoding='utf-8',
        extra='ignore'
    )

    imap_email: str
    imap_password: str
    amo_long_term_token: str
    amo_subdomain: str
    db_user: str
    db_password: str
    db_name: str
    db_host: str
    db_port: int
    mode: AppMode

    MIN_LEAD_BUDGET: int = 100_000
    PIPELINE_NAME_GOSZAKAZ: str = "Гос.заказ - прогрев клиента"
    STATUS_NAME_POBEDITELI: str = "Победители"
    
    CUSTOM_FIELD_NAME_INN_LEAD: str = "ИНН клиента"
    CUSTOM_FIELD_NAME_PURCHASE_LINK_LEAD: str = "Ссылка на закупку"
    CUSTOM_FIELD_NAME_INN_COMPANY: str = "ИНН"
    
    EXCLUDE_RESPONSIBLE_USERS: List[str] = ["Алена", "Новикова Евгения"]
    
    USER_NAME_UNSORTED_LEADS: str = "НЕРАЗОБРАННЫЕ ЗАЯВКИ"
    USER_NAME_DEFAULT_TASK_ASSIGN_POPOVA: str = "Анастасия Попова"
    
    TASK_TEXT_NEW_TENDER_WIN: str = "Пришло обновление из базы победителей"
    TASK_COMPLETE_OFFSET_MINUTES: int = 10
    TASK_TYPE_NAME_DEFAULT: str = "Связаться с клиентом"

    CHECK_INTERVAL_SECONDS: int = 3600

    test_amo_subdomain: Optional[str] = Field(default=None)
    test_amo_long_term_token: Optional[str] = Field(default=None)

    request_delay: float = Field(default=0.5)

    @property
    def current_amo_subdomain(self) -> str:
        if self.mode == AppMode.TEST and self.test_amo_subdomain:
            return self.test_amo_subdomain
        return self.amo_subdomain

    @property
    def current_amo_long_term_token(self) -> str:
        if self.mode == AppMode.TEST and self.test_amo_long_term_token:
            return self.test_amo_long_term_token
        return self.amo_long_term_token


settings = Settings()