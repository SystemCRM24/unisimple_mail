def format_number_with_spaces(number_str: str) -> str:
    # Проверка на число (целое или с плавающей точкой)
    try:
        # Пытаемся привести строку к float
        number = float(number_str.replace(',', '.'))
    except ValueError:
        logger.info(f"НЕКОРРЕКТНОЕ ЧИСЛО В СТРОКЕ - {number_str}")
        raise ValueError("Входная строка должна представлять корректное число")

    # Разделяем целую и дробную части
    if number.is_integer():
        integer_part = str(int(number))
        decimal_part = ""
    else:
        integer_part = str(int(number))
        decimal_part = f",{str(round(number % 1, 2))[2:]}"  # Убираем "0."

    # Форматируем целую часть по разрядам
    reversed_str = integer_part[::-1]
    chunks = [reversed_str[i:i+3] for i in range(0, len(reversed_str), 3)]
    formatted_integer = ' '.join(chunks)[::-1]

    # Собираем результат
    result = formatted_integer
    if decimal_part:
        result += f"{decimal_part}"

    return result + " р."

print(format_number_with_spaces("1200456.6"))