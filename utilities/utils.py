import re


def convert_camel_to_snake(s: str) -> str:
    return re.sub(r'(?<!^)(?=[A-Z])', '_', s).lower()
