def format_number_with_spaces(number_str: str) -> str:
    if not number_str.isdigit():
        raise ValueError("Входная строка должна содержать только цифры")

    reversed_str = number_str[::-1]
    chunks = [reversed_str[i:i+3] for i in range(0, len(reversed_str), 3)]

    formatted = ' '.join(chunks)[::-1]

    return formatted + " р."

print(format_number_with_spaces("1200456"))