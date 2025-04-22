import logging
import imaplib
import email
from email.header import decode_header
import os

from src.settings import settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class Gmail:
    def __init__(self):
        imap_server = "imap.gmail.com"
        self.imap = imaplib.IMAP4_SSL(imap_server)
        auth_response = self.imap.login(settings.imap_email, settings.imap_password)
        logger.info(auth_response)
        self.data_folder = os.path.join(os.path.dirname(__file__), 'data')
        os.makedirs(self.data_folder, exist_ok=True)

    def get_most_recent_file(self) -> tuple[bytes, str]:
        """
        :return: Кортеж содержимого файла и его имени
        """
        self.imap.select("INBOX")  # папка входящие
        res, messages = self.imap.search(None, "ALL")
        messages = messages[0].decode('utf-8').split(" ")
        if res == "OK":
            res, msg_data = self.imap.fetch(messages[-1], "(RFC822)")
            if res == "OK":
                msg = email.message_from_bytes(msg_data[0][1])
                for part in msg.walk():
                    # Проверяем, является ли часть вложением
                    if part.get_content_disposition() == "attachment":
                        filename = part.get_filename()
                        if filename:
                            # Декодируем имя файла, если оно закодировано
                            filename = decode_header(filename)[0][0]
                            if isinstance(filename, bytes):
                                filename = filename.decode()
                            # Полный путь к файлу
                            filepath = os.path.join(self.data_folder, filename)
                            # Получаем содержимое вложения
                            data = part.get_payload(decode=True)
                            # Сохраняем файл
                            with open(filepath, "wb") as f:
                                f.write(data)
                            print(f"Сохранено вложение: {filepath}")
                            return data, filename
