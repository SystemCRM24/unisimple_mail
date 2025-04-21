from src.mail.file_parser import ExcelParser
from src.mail.mail_connector import Gmail


def main():
    gmail = Gmail()
    file = gmail.get_most_recent_file()
    parser = ExcelParser(file)
    data = parser.parse()


if __name__ == '__main__':
    main()
