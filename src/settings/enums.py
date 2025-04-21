from enum import StrEnum


class AppMode(StrEnum):
    DEBUG = 'debug'
    PRODUCTION = 'production'
    TEST = 'test'
