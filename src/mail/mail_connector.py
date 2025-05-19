import logging
import imaplib
import email
from email.header import decode_header
from typing import Optional
import time
import socket


from src.settings import settings

logger = logging.getLogger(__name__)


class Gmail:
    def __init__(self):
        self.imap: Optional[imaplib.IMAP4_SSL] = None
        self.is_connected = False

    def _connect(self):
        """Устанавливает соединение и логинится на IMAP сервере."""
        imap_server = "imap.gmail.com"
        imap_port = 993

        if self.imap:
            try:
                self.imap.logout()
            except Exception:
                pass
            finally:
                self.imap = None
            logger.debug("Предыдущее IMAP соединение закрыто перед попыткой нового.")

        try:
            logger.info(f"Попытка подключения к IMAP серверу {imap_server}:{imap_port}...")
            self.imap = imaplib.IMAP4_SSL(imap_server, imap_port, timeout=15)
            auth_response = self.imap.login(settings.imap_email, settings.imap_password)
            logger.info(f"IMAP login response: {auth_response}")

            if auth_response and auth_response[0] == 'OK':
                self.is_connected = True
                logger.info("Подключение и аутентификация Gmail успешны.")
            else:
                self.is_connected = False
                logger.error(f"Не удалось выполнить IMAP логин. Ответ: {auth_response}")

        except (imaplib.IMAP4.error, socket.error, ConnectionRefusedError, socket.timeout) as e:
            self.is_connected = False
            self.imap = None
            logger.error(f"Ошибка IMAP при подключении или логине: {e}")

        except Exception as e:
            self.is_connected = False
            self.imap = None
            logger.error(f"Неожиданная ошибка при подключении к IMAP: {e}", exc_info=True)

    def _ensure_connected(self):
        """Проверяет активность соединения и пытается переподключиться при необходимости."""
        try:
            if not self.is_connected or self.imap is None or self.imap.state in ('LOGOUT', 'NONAUTH'):
                logger.info("IMAP соединение неактивно или в некорректном состоянии. Попытка переподключения...")
                self._connect()

                if not self.is_connected:
                    logger.warning("Первая попытка переподключения к Gmail не удалась.")
                    retry_count = 3
                    for i in range(retry_count):
                        time.sleep(5 * (i + 1))
                        logger.info(f"Повторная попытка переподключения к Gmail ({i+1}/{retry_count})...")
                        self._connect()
                        if self.is_connected:
                            logger.info("Переподключение к Gmail успешно после повторных попыток.")
                            break
                    
                    if not self.is_connected:
                        logger.critical("Не удалось переподключиться к Gmail после нескольких попыток.")
                        raise ConnectionError("Не удалось восстановить IMAP соединение.")

        except Exception as e:
            logger.error(f"Ошибка в _ensure_connected: {e}", exc_info=True)
            self.is_connected = False
            self.imap = None
            raise

    def get_most_recent_file(self) -> tuple[bytes, str] | None:
        """
        Получает содержимое и имя самого последнего вложения Excel из папки "Входящие".
        Автоматически переподключается при ошибках соединения и повторно выполняет команду.
        :return: Кортеж содержимого файла (bytes) и его имени (str), или None.
        """
        try:
            self._ensure_connected()

            if not self.is_connected or self.imap is None:
                logger.error("Не удалось выполнить команду GET_MOST_RECENT_FILE: IMAP клиент не подключен после попыток восстановления.")
                return None

            max_command_retries = 2
            for attempt in range(max_command_retries):
                try:
                    if not self.is_connected or self.imap is None or self.imap.state in ('LOGOUT', 'NONAUTH'):
                        self._ensure_connected()
                        if not self.is_connected or self.imap is None:
                            logger.error(f"Переподключение не удалось перед выполнением команды (попытка {attempt + 1}). Пропуск команды.")
                            if attempt < max_command_retries - 1:
                                continue
                            else:
                                logger.critical(f"Не удалось выполнить команду IMAP после {max_command_retries} попыток.")
                                raise ConnectionError("Не удалось выполнить IMAP команду: соединение не активно.")

                    logger.info(f"Выполнение команды IMAP: SELECT INBOX (попытка {attempt + 1})")
                    status, messages = self.imap.select("INBOX")
                    if status != "OK":
                        logger.error(f"IMAP SELECT INBOX failed: Status {status}, Messages {messages}")
                        self.is_connected = False
                        if attempt < max_command_retries - 1:
                            continue
                        else:
                            raise imaplib.IMAP4.abort(f"command: SELECT => Status {status}")

                    logger.info(f"Выполнение команды IMAP: SEARCH ALL (попытка {attempt + 1})")
                    status, messages = self.imap.search(None, "ALL")
                    if status != "OK":
                        logger.error(f"IMAP SEARCH ALL failed: Status {status}, Messages {messages}")
                        self.is_connected = False
                        if attempt < max_command_retries - 1:
                            continue
                        else:
                            raise imaplib.IMAP4.abort(f"command: SEARCH ALL => Status {status}")

                    break

                except (imaplib.IMAP4.error, socket.error, socket.timeout) as e:
                    logger.error(f"Ошибка IMAP при выполнении команды (попытка {attempt + 1}): {e}")
                    self.is_connected = False
                    if attempt < max_command_retries - 1:
                        logger.info("Попытка переподключения для повторного выполнения команды...")
                        continue
                    else:
                        logger.critical(f"IMAP команда не выполнена после {max_command_retries} попыток.")
                        raise

                except Exception as e:
                    logger.error(f"Неожиданная ошибка при выполнении IMAP команды (попытка {attempt + 1}): {e}", exc_info=True)
                    raise

            if messages and messages[0]:
                message_ids = messages[0].decode('utf-8').split()
                if not message_ids:
                    logger.info("Нет писем в папке Входящие.")
                    return None

                latest_message_id = message_ids[-1]
                logger.info(f"Найдены письма. Получение последнего сообщения (ID: {latest_message_id})...")

                for attempt in range(max_command_retries):
                    try:
                        if not self.is_connected or self.imap is None or self.imap.state in ('LOGOUT', 'NONAUTH'):
                           self._ensure_connected()
                           if not self.is_connected or self.imap is None:
                                logger.error(f"Переподключение не удалось перед выполнением FETCH (попытка {attempt + 1}).")
                                if attempt < max_command_retries - 1:
                                    continue
                                else:
                                    raise ConnectionError("Не удалось выполнить FETCH: соединение не активно.")

                        logger.info(f"Выполнение команды IMAP: FETCH {latest_message_id} (RFC822) (попытка {attempt + 1})")
                        status, msg_data = self.imap.fetch(latest_message_id, "(RFC822)")
                        if status != "OK":
                            logger.error(f"IMAP FETCH failed for message ID {latest_message_id}: Status {status}, Data {msg_data}")
                            self.is_connected = False
                            if attempt < max_command_retries - 1:
                                continue
                            else:
                                raise imaplib.IMAP4.abort(f"command: FETCH => Status {status}")

                        break

                    except (imaplib.IMAP4.error, socket.error, socket.timeout) as e:
                        logger.error(f"Ошибка IMAP при выполнении FETCH (попытка {attempt + 1}): {e}")
                        self.is_connected = False
                        if attempt < max_command_retries - 1:
                            logger.info("Попытка переподключения для повторного выполнения FETCH...")
                            continue
                        else:
                            logger.critical(f"IMAP FETCH не выполнен после {max_command_retries} попыток.")
                            raise

                    except Exception as e:
                        logger.error(f"Неожиданная ошибка при выполнении FETCH (попытка {attempt + 1}): {e}", exc_info=True)
                        raise

                if not msg_data or not msg_data[0]:
                    logger.error(f"Не получены данные сообщения для ID {latest_message_id} после FETCH.")
                    return None

                try:
                    msg = email.message_from_bytes(msg_data[0][1])
                    logger.debug(f"Парсинг сообщения от {msg.get('From')}, Тема: {decode_header(msg.get('Subject', 'Н/Д'))[0][0]}")

                    for part in msg.walk():
                        if part.get_content_disposition() == "attachment":
                            filename = part.get_filename()
                            if filename:
                                try:
                                    decoded_filename_tuple = decode_header(filename)[0]
                                    decoded_filename = decoded_filename_tuple[0]
                                    if isinstance(decoded_filename, bytes):
                                        encoding = decoded_filename_tuple[1] or 'utf-8'
                                        decoded_filename = decoded_filename.decode(encoding)
                                    filename = decoded_filename
                                except Exception as e:
                                    logger.warning(f"Не удалось декодировать имя файла '{filename}': {e}. Используется исходное имя.")

                                data = part.get_payload(decode=True)

                                if filename.lower().endswith(('.xlsx', '.xls', '.xlsm')):
                                    logger.info(f"Найдено и извлечено вложение Excel: {filename}")
                                    return data, filename

                    logger.info(f"Последнее сообщение (ID: {latest_message_id}) не содержит вложений Excel с нужным расширением.")
                    return None

                except Exception as e:
                    logger.error(f"Ошибка при парсинге сообщения или извлечении вложений для ID {latest_message_id}: {e}", exc_info=True)
                    return None

            else:
                logger.info("Нет сообщений в папке Входящие для обработки.")
                return None

        except ConnectionError as e:
            logger.critical(f"Не удалось получить файл из Gmail: Ошибка соединения после нескольких попыток: {e}")
            return None

        except Exception as e:
            logger.error(f"Непредвиденная ошибка в get_most_recent_file: {e}", exc_info=True)
            return None

    def logout(self):
        """Выполняет выход из IMAP сервера, если соединение активно."""
        if self.imap and self.is_connected:
            logger.info("Выполнение IMAP logout...")
            try:
                if self.imap.state == 'SELECTED':
                    try:
                        self.imap.close()
                        logger.debug("IMAP папка закрыта перед logout.")
                    except Exception as e_close:
                        logger.warning(f"Ошибка при закрытии IMAP папки перед logout: {e_close}")

                status, msg = self.imap.logout()
                status_str = status.decode() if isinstance(status, bytes) else str(status)
                msg_str = msg.decode() if isinstance(msg, bytes) else str(msg)
                logger.info(f"IMAP logout выполнен: Статус '{status_str}', Сообщение: '{msg_str}'")

            except Exception as e_logout:
                logger.warning(f"Ошибка при выполнении IMAP logout: {e_logout}")

            finally:
                self.is_connected = False
                self.imap = None
        else:
            logger.debug("IMAP клиент не подключен или уже вышел, logout пропущен.")