import asyncio

from src.mail.file_parser import ExcelParser
from src.mail.mail_connector import Gmail
from src.db import PostgresDB


async def main():
    last_extraction_dt = None
    gmail = Gmail()
    while True:
        file, file_name = gmail.get_most_recent_file()
        print("parsing ...")
        parser = ExcelParser(file, file_name)
        print(last_extraction_dt != parser.extraction_dt)
        if last_extraction_dt != parser.extraction_dt:
            data = await parser.parse()
            db_data = await parser.parse_for_db()
            async with PostgresDB() as db:
                await db.write_purchases(db_data)
            last_extraction_dt = parser.extraction_dt
        await asyncio.sleep(60)


if __name__ == '__main__':
    asyncio.run(main())
