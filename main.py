import asyncio

from src.mail.file_parser import ExcelParser
from src.mail.mail_connector import Gmail


async def main():
    gmail = Gmail()
    file, file_name = gmail.get_most_recent_file()
    parser = ExcelParser(file, file_name)
    data = await parser.parse()
    db_data = await parser.parse_for_db()
    print(db_data)


if __name__ == '__main__':
    asyncio.run(main())
