from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from pathlib import Path
from typing import Optional, List 

from .enums import AppMode

BASE_DIR = Path(__file__).resolve().parent.parent.parent


class Settings(BaseSettings):
    """
    Класс настроек приложения, загружаемых из переменных окружения и файла .env.
    """
    model_config = SettingsConfigDict(env_file=BASE_DIR / '.env', env_file_encoding='utf-8')
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
    STATUS_NAME_POBEDITELI: str = "Победители" # Целевой этап для создания новых сделок из Excel
    # Добавим другие нужные имена этапов, если они используются для логики
    STATUS_NAME_KHOLODNYE_ZAYAVKI: str = "Холодные заявки" 
    
    CUSTOM_FIELD_NAME_INN_LEAD: str = "ИНН" # Имя поля ИНН в сделке
    CUSTOM_FIELD_NAME_PURCHASE_LINK_LEAD: str = "Ссылка на закупку" # Имя поля Ссылки в сделке
    CUSTOM_FIELD_NAME_INN_COMPANY: str = "ИНН" # Имя поля ИНН в компании
    
    EXCLUDE_RESPONSIBLE_USERS: List[str] = ["Алена", "Новикова Евгения"]
    USER_NAME_UNSORTED_LEADS: str = "НЕРАЗОБРАННЫЕ ЗАЯВКИ" # Имя пользователя/системы для неразобр.
    USER_NAME_DEFAULT_TASK_ASSIGN_POPOVA: str = "Анастасия Попова" # Для задач
    
    # Настройки для задач
    TASK_TEXT_NEW_LEAD_UNSORTED: str = "Новая неразобранная сделка требует внимания"
    TASK_TEXT_NEW_WIN_EXISTING_LEAD: str = "Новая победа по существующей сделке"
    TASK_COMPLETE_OFFSET_MINUTES: int = 10
    TASK_TYPE_NAME_DEFAULT: str = "Связаться с клиентом" # Имя типа задачи по умолчанию
    CHECK_INTERVAL_SECONDS: int = 60

    test_amo_subdomain: Optional[str] = Field(None)
    test_amo_long_term_token: Optional[str] = Field(None)

    request_delay: float = Field(0.5)

    @property
    def current_amo_subdomain(self) -> str:
        """
        Возвращает поддомен amoCRM для текущего режима работы (тест или боевой).
        В тестовом режиме использует test_amo_subdomain, если он указан.
        В противном случае (не в тестовом режиме или тестовый поддомен не указан), использует amo_subdomain.
        """
        if self.mode == AppMode.TEST and self.test_amo_subdomain:
            return self.test_amo_subdomain
        return self.amo_subdomain

    @property
    def current_amo_long_term_token(self) -> str:
        """
        Возвращает долгосрочный токен amoCRM для текущего режима работы (тест или боевой).
        В тестовом режиме использует test_amo_long_term_token, если он указан.
        В противном случае использует amo_long_term_token.
        """
        if self.mode == AppMode.TEST and self.test_amo_long_term_token:
            return self.test_amo_long_term_token
        return self.amo_long_term_token


settings = Settings()