import imaplib
import asyncio
import logging
from datetime import datetime
from typing import Optional

from src.amo.client import AmoClient
from src.amo.schemas import DBStatePurchase
from src.db import PostgresDB
from src.mail.file_parser import ExcelParser
from src.mail.mail_connector import Gmail
from src.settings import settings

from src.processing.amocrm_processor import process_parsed_data_for_amocrm

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


async def main():
    logger.info(f"Приложение запущено в режиме: {settings.mode.name}")

    try:
        gmail_client = Gmail()
        logger.info("Gmail клиент успешно инициализирован.")
    except Exception as e:
        logger.critical(f"Критическая ошибка при инициализации Gmail клиента: {e}", exc_info=True)
        return

    last_processed_extraction_dt: Optional[datetime] = None

    check_interval = settings.CHECK_INTERVAL_SECONDS
    if not check_interval or check_interval <= 0:
        logger.warning(f"CHECK_INTERVAL_SECONDS некорректен ({check_interval}). Установлено значение по умолчанию: 60 секунд.")
        check_interval = 60

    try:
        while True:
            logger.info("Проверка новых писем...")
            try:
                file_content, file_name = gmail_client.get_most_recent_file()
                
                if file_content and file_name:
                    logger.info(f"Обнаружен файл в почте: {file_name}")
                    
                    parser = ExcelParser(file_content, file_name)
                    current_file_extraction_dt = parser.extraction_dt

                    process_this_file = False
                    if last_processed_extraction_dt is None:
                        process_this_file = True
                        logger.info(f"Первый файл для обработки: '{file_name}' (дата выгрузки: {current_file_extraction_dt})")
                    elif current_file_extraction_dt > last_processed_extraction_dt:
                        process_this_file = True
                        logger.info(f"Новый файл: '{file_name}' (дата выгрузки: {current_file_extraction_dt}, предыдущая обработка: {last_processed_extraction_dt}).")
                    else:
                        logger.info(f"Файл '{file_name}' (дата выгрузки: {current_file_extraction_dt}) не новее последнего обработанного ({last_processed_extraction_dt}). Пропуск.")

                    if process_this_file:
                        logger.info(f"Начало обработки файла: {file_name}")
              
                        parsed_data_for_db = await parser.parse_for_db()

                        # if parsed_data_for_db:
                        #     async with PostgresDB() as db:
                        #         await db.write_purchases(parsed_data_for_db)
                        #     logger.info(f"Записано {len(parsed_data_for_db)} записей в БД из файла '{file_name}'.")
                        # else:
                        #     logger.warning(f"Нет данных для записи в БД из файла '{file_name}'.")

                        if parsed_data_for_db:
                            async with AmoClient() as amo_client_instance:
                                await process_parsed_data_for_amocrm(amo_client_instance, parsed_data_for_db)
                        else:
                            logger.warning(f"Нет данных для обработки в AmoCRM из файла '{file_name}'.")
                  
                        last_processed_extraction_dt = current_file_extraction_dt
                        logger.info(f"Файл '{file_name}' успешно обработан. Дата последней обработки обновлена на: {last_processed_extraction_dt}")
                else:
                    logger.info("Новых файлов Excel в почте не найдено.")

            except imaplib.IMAP4.error as e:
                logger.error(f"Ошибка IMAP при работе с почтой: {e}", exc_info=True)
                logger.info("Попытка переподключения к Gmail через некоторое время...")
                await asyncio.sleep(check_interval)
                try:
                    gmail_client = Gmail()
                    logger.info("Переподключение к Gmail успешно.")
                except Exception as recon_e:
                    logger.error(f"Ошибка при переподключении к Gmail: {recon_e}", exc_info=True)
            except Exception as e:
                logger.error(f"Ошибка в цикле обработки файла: {e}", exc_info=True)

            logger.info(f"Следующая проверка почты через {check_interval} секунд...")
            await asyncio.sleep(check_interval)

    except KeyboardInterrupt:
        logger.info("Приложение остановлено пользователем (KeyboardInterrupt).")
    except asyncio.CancelledError:
        logger.info("Главная задача была отменена.")
    finally:
        logger.info("Приложение завершает работу.")
        if 'gmail_client' in locals() and hasattr(gmail_client.imap, 'state') and gmail_client.imap.state == 'SELECTED':
            try:
                gmail_client.imap.close()
                logger.info("IMAP папка закрыта.")
            except Exception as e_close:
                logger.warning(f"Ошибка при закрытии IMAP папки: {e_close}")
        if 'gmail_client' in locals() and hasattr(gmail_client.imap, 'logout'):
            try:
                status, msg = gmail_client.imap.logout()
                logger.info(f"IMAP logout: {status} - {msg}")
            except Exception as e_logout:
                logger.warning(f"Ошибка при IMAP logout: {e_logout}")


if __name__ == '__main__':
    critical_settings_keys = [
        'imap_email', 'imap_password', 'amo_long_term_token', 'amo_subdomain',
        'db_user', 'db_password', 'db_name', 'db_host', 'db_port',
        'MIN_LEAD_BUDGET', 'PIPELINE_NAME_GOSZAKAZ', 'STATUS_NAME_POBEDITELI',
        'CUSTOM_FIELD_NAME_INN_LEAD', 'CUSTOM_FIELD_NAME_PURCHASE_LINK_LEAD'
    ]
    missing_keys = [key for key in critical_settings_keys if not hasattr(settings, key) or getattr(settings, key) is None]

    if missing_keys:
        logger.critical(f"Отсутствуют или не заданы обязательные настройки: {', '.join(missing_keys)}. "
                        "Проверьте переменные окружения и файл src/settings/__init__.py")
    else:
        asyncio.run(main())
