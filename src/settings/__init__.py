from pydantic_settings import BaseSettings, SettingsConfigDict

from pathlib import Path

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
    db_host: str
    db_port: int
    mode: AppMode


settings = Settings()
