from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from pathlib import Path
from typing import Optional

from .enums import AppMode

BASE_DIR = Path(__file__).resolve().parent.parent.parent


class Settings(BaseSettings):
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

    test_amo_subdomain: Optional[str] = Field(None)
    test_amo_long_term_token: Optional[str] = Field(None)

    request_delay: float = Field(0.5)

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

    @property
    def is_test_mode(self) -> bool:
        return self.mode == AppMode.TEST


settings = Settings()